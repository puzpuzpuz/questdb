/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.BitmapIndexReader;
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.Misc;

/**
 * Optimized record cursor factory for count_distinct queries on indexed symbol columns.
 * This factory scans the symbol indices in each partition to count distinct values,
 * bypassing the need to iterate through all table rows.
 * <p>
 * Used for simple queries of the form:
 * SELECT count_distinct(sym1), count_distinct(sym2), ... FROM table
 * <p>
 * Where:
 * - All columns are indexed symbols
 * - No WHERE clause
 * - No GROUP BY clause
 * - Full table scan
 */
public class IndexedCountDistinctRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory base;
    private final IndexedCountDistinctRecordCursor cursor;
    private final IntList indexedSymbolColumns;

    public IndexedCountDistinctRecordCursorFactory(RecordMetadata metadata, RecordCursorFactory base, IntList indexedSymbolColumns) {
        super(metadata);
        this.base = base;
        this.indexedSymbolColumns = indexedSymbolColumns;
        this.cursor = new IndexedCountDistinctRecordCursor();
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        cursor.of(base, indexedSymbolColumns, executionContext);
        return cursor;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("IndexedCountDistinct");
        sink.child(base);
    }

    @Override
    public boolean usesIndex() {
        return true;
    }

    @Override
    protected void _close() {
        Misc.free(base);
    }

    private static class IndexedCountDistinctRecord implements Record {
        private long[] countDistinctValues;

        @Override
        public long getDate(int col) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getLong(int col) {
            return countDistinctValues[col];
        }

        @Override
        public long getRowId() {
            return 0;
        }

        @Override
        public long getTimestamp(int col) {
            throw new UnsupportedOperationException();
        }

        public void prepare(int columnCount) {
            if (countDistinctValues == null || countDistinctValues.length != columnCount) {
                countDistinctValues = new long[columnCount];
            }
        }

        public void setCountDistinct(int index, long value) {
            countDistinctValues[index] = value;
        }
    }

    private static class IndexedCountDistinctRecordCursor implements NoRandomAccessRecordCursor {
        private final IntHashSet distinctKeys = new IntHashSet();
        private final IndexedCountDistinctRecord record = new IndexedCountDistinctRecord();
        private boolean hasNext = true;

        @Override
        public void close() {
            // no-op
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            boolean oldHasNext = hasNext;
            hasNext = false;
            return oldHasNext;
        }

        public void of(RecordCursorFactory base, IntList indexedSymbolColumns, SqlExecutionContext executionContext) throws SqlException {
            this.hasNext = true;

            // Calculate count distinct values for each indexed symbol column
            TableToken tableToken = base.getTableToken();
            if (tableToken != null) {
                // TODO: this won't do as the schema may have changed already - we need to use exactly the table reader/index readers from the base cursor
                try (TableReader reader = executionContext.getCairoEngine().getReader(tableToken)) {
                    record.prepare(indexedSymbolColumns.size());

                    for (int i = 0; i < indexedSymbolColumns.size(); i++) {
                        int columnIndex = indexedSymbolColumns.getQuick(i);
                        record.setCountDistinct(i, scanPartitionsForDistinctSymbols(reader, columnIndex));
                    }
                } catch (Exception e) {
                    throw SqlException.$(0, "Failed to scan symbol indices: ").put(e.getMessage());
                }
            } else {
                // Fallback: if we can't get table token, return 0s
                record.prepare(indexedSymbolColumns.size());
                for (int i = 0; i < indexedSymbolColumns.size(); i++) {
                    record.setCountDistinct(i, 0);
                }
            }

            distinctKeys.clear();
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public long size() {
            return 1;
        }

        @Override
        public void toTop() {
            hasNext = true;
        }

        private long scanPartitionsForDistinctSymbols(TableReader reader, int columnIndex) {
            distinctKeys.clear();
            final int partitionCount = reader.getPartitionCount();

            // Get symbol map reader to get the maximum possible key count
            final SymbolMapReader symbolMapReader = reader.getSymbolMapReader(columnIndex);
            if (symbolMapReader == null) {
                return 0;
            }

            final int maxSymbolCount = symbolMapReader.getSymbolCount();

            // Scan each partition to find which symbol keys are actually present
            for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
                final long partitionSize = reader.openPartition(partitionIndex);
                if (partitionSize <= 0) {
                    continue; // Skip empty partitions
                }

                try {
                    // Get bitmap index reader for this partition
                    final BitmapIndexReader indexReader = reader.getBitmapIndexReader(
                            partitionIndex,
                            columnIndex,
                            BitmapIndexReader.DIR_FORWARD
                    );

                    if (indexReader != null && indexReader.isOpen()) {
                        // Scan through all possible symbol keys (0 to maxSymbolCount - 1)
                        // Key 0 is reserved for NULL values, so we start from 0 if nulls should be counted
                        for (int key = 0; key < maxSymbolCount; key++) {
                            // Check if this key has any rows in this partition
                            final RowCursor cursor = indexReader.getCursor(true, key + 1, 0, partitionSize - 1);
                            if (cursor.hasNext()) {
                                distinctKeys.add(key);
                                // If we found all possible symbols, we can stop early
                                if (distinctKeys.size() >= maxSymbolCount) {
                                    break;
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    // If we can't read this partition's index, continue with other partitions
                    // This handles cases where partition files might be corrupted or missing
                    continue;
                }

                // If we've found all possible distinct symbols, no need to check more partitions
                if (distinctKeys.size() >= maxSymbolCount) {
                    break;
                }
            }

            return distinctKeys.size();
        }
    }
}