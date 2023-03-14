/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package org.questdb;

import io.questdb.Metrics;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import org.openjdk.jmh.runner.RunnerException;

import java.io.File;
import java.io.IOException;

import static org.questdb.TsbsCompressionOffBenchmark.*;

public class TsbsCompressionOffSubPartitioningBenchmark {

    private static final String DIR = "/tmp/compr-off-sub-part";

    /**
     * This benchmark needs to flush the page cache, so make sure to run it with sudo.
     */
    public static void main(String[] args) throws RunnerException, IOException, InterruptedException {
        File dir = new File(DIR);
        if (!dir.exists()) {
            dir.mkdirs();
        }

        final FilesFacade ff = FilesFacadeImpl.INSTANCE;
        final CairoConfiguration configuration = new DefaultCairoConfiguration(DIR);
        final Rnd rnd = new Rnd();
        final Path path = new Path();

        executeDdl("create table if not exists test (l1 long, l2 long)", configuration);

        TableToken tableToken = new TableToken("test", "test", 0, false);

        long rows = 0;
        try (TableWriter writer = new TableWriter(configuration, tableToken, Metrics.disabled())) {
            int sym = 0;
            long lng = 0;
            for (int i = 0; i < ROWS; i++) {
                if (++sym == DISTINCT_SYMBOLS) {
                    sym = rnd.nextInt(FILTERED_SYMBOL);
                }
                if (++lng == LONG_UPPER_BOUND) {
                    lng = rnd.nextLong(100);
                }
                if (sym != FILTERED_SYMBOL) {
                    // we write only the filtered sub-partition
                    continue;
                }
                TableWriter.Row r = writer.newRow();
                r.putLong(0, lng);
                r.putLong(1, lng);
                r.append();
                rows++;
            }
            writer.commit();
        }

        // Flush page cache
        Process process = Runtime.getRuntime().exec(new String[]{"sh", "-c", "sync; echo 3 > /proc/sys/vm/drop_caches"});
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new IllegalStateException("Non-zero exit code for page cache flush: " + exitCode);
        }

        long start = System.nanoTime();

        MemoryCMR l1mem = Vm.getCMRInstance();
        l1mem.of(ff, path.of(DIR).concat("test/default/l1.d").$(), 0, -1, MemoryTag.MMAP_DEFAULT);
        MemoryCMR l2mem = Vm.getCMRInstance();
        l2mem.of(ff, path.of(DIR).concat("test/default/l2.d").$(), 0, -1, MemoryTag.MMAP_DEFAULT);

        long agg = aggregate(rows, l1mem, l2mem);

        System.out.println("Aggregate value: " + agg + ", took: " + (System.nanoTime() - start) / 1_000_000 + "ms");
    }

    private static long aggregate(long rowCount, MemoryCR l1mem, MemoryCR l2mem) {
        long agg = 0;
        for (long i = 0; i < rowCount; i++) {
            agg += l1mem.getLong(i * Long.BYTES) + l2mem.getLong(i * Long.BYTES);
        }
        return agg;
    }
}
