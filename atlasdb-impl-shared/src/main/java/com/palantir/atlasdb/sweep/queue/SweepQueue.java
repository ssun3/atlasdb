/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.sweep.queue;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.sweep.Sweeper;

public final class SweepQueue implements SweepQueueWriter {
    private static final int FIVE_MINUTES = 5000 * 60;

    private final SweepableCells sweepableCells;
    private final SweepableTimestamps sweepableTimestamps;
    private final ShardProgress progress;
    private final SweepQueueDeleter deleter;
    private final SweepQueueCleaner cleaner;
    private final Supplier<Integer> numShards;

    private SweepQueue(SweepableCells cells, SweepableTimestamps timestamps, ShardProgress progress,
            SweepQueueDeleter deleter, SweepQueueCleaner cleaner, Supplier<Integer> numShards) {
        this.sweepableCells = cells;
        this.sweepableTimestamps = timestamps;
        this.progress = progress;
        this.deleter = deleter;
        this.cleaner = cleaner;
        this.numShards = numShards;
    }

    public static SweepQueue create(KeyValueService kvs, Supplier<Integer> shardsConfig) {
        ShardProgress progress = new ShardProgress(kvs);
        Supplier<Integer> shards = createProgressUpdatingSupplier(shardsConfig, progress, FIVE_MINUTES);
        WriteInfoPartitioner partitioner = new WriteInfoPartitioner(kvs, shards);
        SweepableCells cells = new SweepableCells(kvs, partitioner);
        SweepableTimestamps timestamps = new SweepableTimestamps(kvs, partitioner);
        SweepQueueDeleter deleter = new SweepQueueDeleter(kvs);
        SweepQueueCleaner cleaner = new SweepQueueCleaner(cells, timestamps, progress);
        return new SweepQueue(cells, timestamps, progress, deleter, cleaner, shards);
    }

    /**
     * Creates a supplier such that the first call to {@link Supplier#get()} on it will take the maximum of the runtime
     * configuration and the persisted number of shards, and persist and memoize the result. Subsequent calls will
     * return the cached value until refreshTimeMillis has passed, at which point the next call will again perform the
     * check nad set.
     *
     * @param runtimeConfig live reloadable runtime configuration for the number of shards
     * @param progress progress table persisting the number of shards
     * @param refreshTimeMillis timeout for caching the number of shards
     * @return supplier calculating and persisting the number of shards to use
     */
    @VisibleForTesting
    static Supplier<Integer> createProgressUpdatingSupplier(Supplier<Integer> runtimeConfig,
            ShardProgress progress, long refreshTimeMillis) {
        return Suppliers.memoizeWithExpiration(
                () -> progress.updateNumberOfShards(runtimeConfig.get()), refreshTimeMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void enqueue(List<WriteInfo> writes) {
        sweepableTimestamps.enqueue(writes);
        sweepableCells.enqueue(writes);
    }

    /**
     * Sweep the next batch for the shard and strategy specified by shardStrategy, with the sweep timestamp sweepTs.
     * After successful deletes, the persisted information about the writes is removed, and progress is updated
     * accordingly.
     *
     * @param shardStrategy shard and strategy to use
     * @param sweepTs sweep timestamp, the upper limit to the start timestamp of writes to sweep
     */
    public void sweepNextBatch(ShardAndStrategy shardStrategy, long sweepTs) {
        long lastSweptTs = progress.getLastSweptTimestamp(shardStrategy);

        SweepBatch sweepBatch = getNextBatchToSweep(shardStrategy, lastSweptTs, sweepTs);

        deleter.sweep(sweepBatch.writes(), Sweeper.of(shardStrategy));
        cleaner.clean(shardStrategy, lastSweptTs, sweepBatch.lastSweptTimestamp());
    }

    private SweepBatch getNextBatchToSweep(ShardAndStrategy shardStrategy, long lastSweptTs, long sweepTs) {
        return sweepableTimestamps.nextSweepableTimestampPartition(shardStrategy, lastSweptTs, sweepTs)
                .map(fine -> sweepableCells.getBatchForPartition(shardStrategy, fine, lastSweptTs, sweepTs))
                .orElse(SweepBatch.of(ImmutableList.of(), sweepTs - 1L));
    }

    /**
     * Returns number modulo the most recently known number of shards.
     */
    public int modShards(long number) {
        return (int) (number % numShards.get());
    }
}