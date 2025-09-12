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

package io.questdb.test.griffin.engine.groupby;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class IndexedCountDistinctRecordCursorFactoryTest extends AbstractCairoTest {

    @Test
    public void testEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (sym1 symbol index, value int)");

            // Empty table should return 0
            final String expected = "count_distinct\n0\n";

            assertQuery(
                    expected,
                    "select count_distinct(sym1) from x",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testMultipleIndexedSymbolColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (sym1 symbol index, sym2 symbol index, value int)");

            execute(
                    "insert into x values " +
                            "('A', 'X', 1), " +
                            "('B', 'Y', 2), " +
                            "('A', 'X', 3), " +  // duplicate sym1 and sym2 values
                            "('C', 'Z', 4), " +
                            "('B', 'Y', 5), " +  // duplicate sym1 and sym2 values
                            "('A', 'Z', 6)"      // different combo
            );

            // Test that the optimization works with multiple columns
            final String expected = "count_distinct\tcount_distinct1\n3\t3\n";

            assertQuery(
                    expected,
                    "select count_distinct(sym1), count_distinct(sym2) from x",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testOptimizationSkippedWithMixedFunctions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (sym1 symbol index, value int)");

            execute(
                    "insert into x values " +
                            "('A', 1), " +
                            "('B', 2), " +
                            "('C', 3)"
            );

            // This should NOT use the optimization because of mixed aggregate functions
            final String expected = "count_distinct\tcount\n3\t3\n";

            assertQuery(
                    expected,
                    "select count_distinct(sym1), count(*) from x",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testOptimizationSkippedWithNonIndexedColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (sym1 symbol index, sym2 symbol, value int)");

            execute(
                    "insert into x values " +
                            "('A', 'X', 1), " +
                            "('B', 'Y', 2), " +
                            "('C', 'Z', 3)"
            );

            // This should NOT use the optimization because sym2 is not indexed
            final String expected = "count_distinct\tcount_distinct1\n3\t3\n";

            assertQuery(
                    expected,
                    "select count_distinct(sym1), count_distinct(sym2) from x",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testOptimizationSkippedWithWhereClause() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (sym1 symbol index, value int)");

            execute(
                    "insert into x values " +
                            "('A', 1), " +
                            "('B', 2), " +
                            "('C', 3)"
            );

            // This should NOT use the optimization because of WHERE clause
            final String expected = "count_distinct\n2\n";

            assertQuery(
                    expected,
                    "select count_distinct(sym1) from x where value > 1",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testSingleIndexedSymbolColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (sym1 symbol index, value int)");

            execute(
                    "insert into x values " +
                            "('A', 1), " +
                            "('B', 2), " +
                            "('A', 3), " +
                            "('C', 4), " +
                            "('B', 5)"
            );

            // Test that the optimization works correctly
            final String expected = "count_distinct\n3\n";

            assertQuery(
                    expected,
                    "select count_distinct(sym1) from x",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testWithNullValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (sym1 symbol index, value int)");

            execute(
                    "insert into x values " +
                            "('A', 1), " +
                            "(null, 2), " +  // null symbol value
                            "('B', 3), " +
                            "('A', 4), " +   // duplicate
                            "(null, 5)"      // another null
            );

            // count_distinct should exclude NULLs, so result should be 2 (A, B)
            final String expected = "count_distinct\n2\n";

            assertQuery(
                    expected,
                    "select count_distinct(sym1) from x",
                    null,
                    false,
                    true
            );
        });
    }
}
