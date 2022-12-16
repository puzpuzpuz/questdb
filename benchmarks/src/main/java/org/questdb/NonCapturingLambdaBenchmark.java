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

import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.ThreadLocal;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class NonCapturingLambdaBenchmark {
    private static final ThreadLocal<String> THE_KEY = new ThreadLocal<>(() -> "abc");
    private static final AtomicReference<CharSequence> THE_OBJECT = new AtomicReference<>();
    private final ConcurrentHashMap<Object> map = new ConcurrentHashMap<>();

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(NonCapturingLambdaBenchmark.class.getSimpleName())
                .warmupIterations(1)
                .measurementIterations(3)
                .addProfiler("gc")
                .forks(1)
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public Object testComputeIfAbsent() {
        String key = THE_KEY.get();
        return map.computeIfAbsent(key, k -> {
            THE_OBJECT.set(k);
            return THE_OBJECT;
        });
    }
}
