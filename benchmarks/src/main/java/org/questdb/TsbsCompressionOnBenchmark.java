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
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.openjdk.jmh.runner.RunnerException;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import static org.questdb.TsbsCompressionOffBenchmark.*;

public class TsbsCompressionOnBenchmark {

    private static final String DIR = "/tmp/compr-on";

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

        executeDdl("create table if not exists test (i int, l1 long, l2 long)", configuration);

        TableToken tableToken = new TableToken("test", "test", 0, false);

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
                TableWriter.Row r = writer.newRow();
                r.putInt(0, sym);
                r.putLong(1, lng);
                r.putLong(2, lng);
                r.append();
            }
            writer.commit();
        }

        // First, compress the data files.

        MemoryCMR imemr = Vm.getCMRInstance();
        imemr.of(ff, path.of(DIR).concat("test/default/i.d").$(), 0, -1, MemoryTag.MMAP_DEFAULT);
        MemoryMARW imemw = Vm.getMARWInstance();
        imemw.of(ff, path.of(DIR).concat("test/default/i.d.c").$(), ff.getPageSize(), MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE);
        MemoryCMR l1memr = Vm.getCMRInstance();
        l1memr.of(ff, path.of(DIR).concat("test/default/l1.d").$(), 0, -1, MemoryTag.MMAP_DEFAULT);
        MemoryMARW l1memw = Vm.getMARWInstance();
        l1memw.of(ff, path.of(DIR).concat("test/default/l1.d.c").$(), ff.getPageSize(), MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE);
        MemoryCMR l2memr = Vm.getCMRInstance();
        l2memr.of(ff, path.of(DIR).concat("test/default/l2.d").$(), 0, -1, MemoryTag.MMAP_DEFAULT);
        MemoryMARW l2memw = Vm.getMARWInstance();
        l2memw.of(ff, path.of(DIR).concat("test/default/l2.d.c").$(), ff.getPageSize(), MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE);

        final LZ4Factory factory = LZ4Factory.fastestInstance();
        final LZ4Compressor compressor = factory.fastCompressor();

        byte[] idata = new byte[(int) imemr.size()];
        int maxLen = compressor.maxCompressedLength(idata.length);
        byte[] icomp = new byte[maxLen];
        int icomplen = compress(compressor, imemr, imemw, idata, icomp);

        System.out.printf("i.d original size: %d, compressed size: %s, ratio: %5.2f\n", idata.length, icomplen, ((double) idata.length / icomplen));

        byte[] l1data = new byte[(int) l1memr.size()];
        maxLen = compressor.maxCompressedLength(l1data.length);
        byte[] l1comp = new byte[maxLen];
        int l1complen = compress(compressor, l1memr, l1memw, l1data, l1comp);

        System.out.printf("l1.d original size: %d, compressed size: %s, ratio: %5.2f\n", l1data.length, l1complen, ((double) l1data.length / l1complen));

        byte[] l2data = new byte[(int) l2memr.size()];
        maxLen = compressor.maxCompressedLength(l2data.length);
        byte[] l2comp = new byte[maxLen];
        int l2complen = compress(compressor, l2memr, l2memw, l2data, l2comp);

        System.out.printf("l2.d original size: %d, compressed size: %s, ratio: %5.2f\n", l2data.length, l2complen, ((double) l2data.length / l2complen));

        Arrays.fill(idata, (byte) 0);
        Arrays.fill(icomp, (byte) 0);
        Arrays.fill(l1data, (byte) 0);
        Arrays.fill(l1comp, (byte) 0);
        Arrays.fill(l2data, (byte) 0);
        Arrays.fill(l2comp, (byte) 0);

        // Flush page cache
        Process process = Runtime.getRuntime().exec(new String[]{"sh", "-c", "sync; echo 3 > /proc/sys/vm/drop_caches"});
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new IllegalStateException("Non-zero exit code for page cache flush: " + exitCode);
        }

        // Decompress and filter

        long start = System.nanoTime();

        imemr.of(ff, path.of(DIR).concat("test/default/i.d.c").$(), 0, -1, MemoryTag.MMAP_DEFAULT);
        l1memr.of(ff, path.of(DIR).concat("test/default/l1.d.c").$(), 0, -1, MemoryTag.MMAP_DEFAULT);
        l2memr.of(ff, path.of(DIR).concat("test/default/l2.d.c").$(), 0, -1, MemoryTag.MMAP_DEFAULT);

        for (int i = 0; i < icomplen; i++) {
            icomp[i] = imemr.getByte(i);
        }
        for (int i = 0; i < l1complen; i++) {
            l1comp[i] = l1memr.getByte(i);
        }
        for (int i = 0; i < l2complen; i++) {
            l2comp[i] = l2memr.getByte(i);
        }

        System.out.println("Disk reads took: " + (System.nanoTime() - start) / 1_000_000 + "ms");

        final LZ4FastDecompressor decompressor = factory.fastDecompressor();
        decompressor.decompress(icomp, 0, idata, 0, idata.length);
        decompressor.decompress(l1comp, 0, l1data, 0, l1data.length);
        decompressor.decompress(l2comp, 0, l2data, 0, l2data.length);

        System.out.println("Decompression took: " + (System.nanoTime() - start) / 1_000_000 + "ms");

        long agg = aggregate(FILTERED_SYMBOL, ROWS, idata, l1data, l2data);

        System.out.println("Aggregate value: " + agg + ", took: " + (System.nanoTime() - start) / 1_000_000 + "ms");
    }

    private static long aggregate(int filteredValue, long rowCount, byte[] idata, byte[] l1data, byte[] l2data) {
        long agg = 0;
        for (int i = 0; i < rowCount; i++) {
            if (Unsafe.byteArrayGetInt(idata, i * Integer.BYTES) == filteredValue) {
                agg += Unsafe.byteArrayGetLong(l1data, i * Long.BYTES) + Unsafe.byteArrayGetLong(l2data, i * Long.BYTES);
            }
        }
        return agg;
    }

    private static int compress(LZ4Compressor compressor, MemoryCMR memr, MemoryMARW memw, byte[] in, byte[] out) {
        for (int i = 0; i < in.length; i++) {
            in[i] = memr.getByte(i);
        }

        int maxLen = compressor.maxCompressedLength(in.length);
        int compLen = compressor.compress(in, 0, in.length, out, 0, maxLen);

        for (int i = 0; i < compLen; i++) {
            memw.putByte(out[i]);
        }
        memw.close(false);

        return compLen;
    }
}
