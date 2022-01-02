/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.std.*;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Threads(Threads.MAX)
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ReadWriteLockBenchmark {

    private static final int NUM_SPINS = 50;

    private final Rnd rnd = new Rnd(NanosecondClockImpl.INSTANCE.getTicks(), MicrosecondClockImpl.INSTANCE.getTicks());

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ReadWriteLockBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(10)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public void testBaseline(Blackhole bh) {
        int sum = 0;
        for (int i = 0; i < NUM_SPINS; i++) {
            sum += rnd.nextInt();
        }
        bh.consume(sum);
    }

    @Benchmark
    public void testReadLock(BenchmarkState state, Blackhole bh) {
        final Lock rLock = state.lock.readLock();
        rLock.lock();
        // emulate some work
        int sum = 0;
        for (int i = 0; i < NUM_SPINS; i++) {
            sum += rnd.nextInt();
        }
        bh.consume(sum);
        rLock.unlock();
    }

    @Benchmark
    public void testReadWriteLock(BenchmarkState state, Blackhole bh) {
        final int n = rnd.nextInt(state.readWriteRatio);
        final Lock lock = n == 0 ? state.lock.writeLock() : state.lock.readLock();
        lock.lock();
        // emulate some work
        int sum = 0;
        for (int i = 0; i < NUM_SPINS; i++) {
            sum += rnd.nextInt();
        }
        bh.consume(sum);
        lock.unlock();
    }

    @State(Scope.Benchmark)
    public static class BenchmarkState {

        @Param({"JUC", "SIMPLE", "BIASED", "XBIASED", "TLBIASED"})
        public LockType type;
        @Param({"1000", "10000", "100000"})
        public int readWriteRatio;
        public ReadWriteLock lock;

        @Setup(Level.Trial)
        public void setUp() {
            switch (type) {
                case JUC:
                    lock = new ReentrantReadWriteLock();
                    break;
                case SIMPLE:
                    lock = new SimpleReadWriteLock();
                    break;
                case BIASED:
                    lock = new BiasedReadWriteLock();
                    break;
                case XBIASED:
                    lock = new XBiasedReadWriteLock();
                    break;
                case TLBIASED:
                    lock = new TLBiasedReadWriteLock();
                    break;
                default:
                    throw new IllegalStateException("unknown lock type: " + type);
            }
        }
    }

    public enum LockType {
        JUC, SIMPLE, BIASED, XBIASED, TLBIASED
    }
}
