/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.groot.store;

import com.alibaba.graphscope.groot.common.config.Configs;
import com.alibaba.graphscope.groot.common.config.StoreConfig;
import com.alibaba.graphscope.groot.common.exception.InvalidArgumentException;
import com.alibaba.graphscope.groot.meta.MetaService;
import com.alibaba.graphscope.groot.operation.StoreDataBatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/** sort by (snapshotId, queueId) */
public class SnapshotSortQueue {

    private static final Logger logger = LoggerFactory.getLogger(SnapshotSortQueue.class);

    private final long queueWaitMs;
    private final int queueCount;

    private final List<BlockingQueue<StoreDataBatch>> innerQueues;
    private final List<StoreDataBatch> queueHeads;

    private int currentPollQueueIdx;
    private long currentPollSnapshotId;
    private final AtomicInteger size;

    public SnapshotSortQueue(Configs configs, MetaService metaService) {
        this.currentPollSnapshotId = -1L;

        this.queueCount = metaService.getQueueCount();
        int queueSize = StoreConfig.STORE_QUEUE_BUFFER_SIZE.get(configs);

        this.innerQueues = new ArrayList<>(this.queueCount);
        this.queueHeads = new ArrayList<>(this.queueCount);
        for (int i = 0; i < this.queueCount; i++) {
            this.innerQueues.add(new ArrayBlockingQueue<>(queueSize));
            this.queueHeads.add(null);
        }
        this.currentPollQueueIdx = this.queueCount - 1;

        this.queueWaitMs = StoreConfig.STORE_QUEUE_WAIT_MS.get(configs);
        this.size = new AtomicInteger(0);
    }

    public boolean offerQueue(int queueId, StoreDataBatch entry) throws InterruptedException {
        BlockingQueue<StoreDataBatch> innerQueue = this.innerQueues.get(queueId);
        if (innerQueue == null) {
            throw new InvalidArgumentException("invalid queueId [" + queueId + "]");
        }
        //        boolean res = innerQueue.offer(entry, this.queueWaitMs, TimeUnit.MILLISECONDS);
        //        if (res) {x
        //            this.size.incrementAndGet();
        //        }
        //        return res;
        innerQueue.put(entry);
        this.size.incrementAndGet();
        return true;
    }

    public StoreDataBatch poll() throws InterruptedException {
        if (this.currentPollSnapshotId == -1L) {
            // We need to wait for all queues each has at least 1 entry to decide initial
            // pollSnapshotId
            long minSnapshotId = Long.MAX_VALUE;
            for (int i = 0; i < this.innerQueues.size(); i++) {
                StoreDataBatch entry = this.queueHeads.get(i);
                if (entry == null) {
                    entry = this.innerQueues.get(i).poll(this.queueWaitMs, TimeUnit.MILLISECONDS);
                    if (entry == null) {
                        return null;
                    }
                    this.queueHeads.set(i, entry);
                }
                minSnapshotId = Math.min(minSnapshotId, entry.getSnapshotId());
            }
            this.currentPollSnapshotId = minSnapshotId;
            logger.info("currentPollSnapshotId initialize to [{}]", currentPollSnapshotId);
        }
        while (true) {
            StoreDataBatch entry = this.queueHeads.get(this.currentPollQueueIdx);
            this.queueHeads.set(this.currentPollQueueIdx, null);
            if (entry == null) {
                entry =
                        innerQueues
                                .get(currentPollQueueIdx)
                                .poll(queueWaitMs, TimeUnit.MILLISECONDS);
            }
            if (entry == null) {
                return null;
            }

            long snapshotId = entry.getSnapshotId();
            // allow for a short duration inconsistent, due to different frontend may have minor
            // difference in timing
            if (snapshotId == this.currentPollSnapshotId
                    || currentPollSnapshotId - snapshotId < 10) {
                this.size.decrementAndGet();
                return entry;
            }
            if (snapshotId > this.currentPollSnapshotId) {
                this.queueHeads.set(this.currentPollQueueIdx, entry);
                this.currentPollQueueIdx--;
                if (this.currentPollQueueIdx == -1) {
                    this.currentPollQueueIdx = this.queueCount - 1;
                    this.currentPollSnapshotId++;
                }
            } else {
                logger.warn(
                        "Illegal entry polled from queue, entrySnapshotId [{}] <"
                                + " currentSnapshotId [{}]. Ignored entry {}",
                        snapshotId,
                        currentPollSnapshotId,
                        entry.toProto());
            }
        }
    }

    public int size() {
        return this.size.get();
    }

    public List<Integer> innerQueueSizes() {
        return this.innerQueues.stream().map(q -> q.size()).collect(Collectors.toList());
    }
}
