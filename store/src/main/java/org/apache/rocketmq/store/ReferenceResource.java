/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store;

import java.util.concurrent.atomic.AtomicLong;

public abstract class ReferenceResource {
    protected final AtomicLong refCount = new AtomicLong(1);
    protected volatile boolean available = true;
    protected volatile boolean cleanupOver = false;
    private volatile long firstShutdownTimestamp = 0;

    public synchronized boolean hold() {
        if (this.isAvailable()) {
            if (this.refCount.getAndIncrement() > 0) {
                return true;
            } else {
                this.refCount.getAndDecrement();
            }
        }

        return false;
    }

    public boolean isAvailable() {
        return this.available;
    }

    /**
     * 关闭
     * @param intervalForcibly
     */
    public void shutdown(final long intervalForcibly) {
        /**
         * 如果available为true，表示第一次执行shutdown方法，首先设置available为false，并记录
         * firstShutdownTimestamp时间戳，释放资源
         * 如果当前文件被其他线程引用，则本次不强制删除，如果没有被其他线程使用，释放相关资源，
         *
         */
        if (this.available) {
            this.available = false;
            this.firstShutdownTimestamp = System.currentTimeMillis();
            this.release();
        }
        // 不是第一次调用shutdown，文件还被引用中
        else if (this.getRefCount() > 0) {
            // 已经到了强制删除时间
            if ((System.currentTimeMillis() - this.firstShutdownTimestamp) >= intervalForcibly) {
                // 在拒绝被删除保护期内destroyMappedFileIntervalForcibly，每执行一次清理任务，将引用次数减去1000，引用数小于1，该文件最终将被删除
                this.refCount.set(-1000 - this.getRefCount());
                this.release();
            }
        }
    }

    /**
     * 释放资源
     */
    public void release() {
        // 当前引用数减一
        long value = this.refCount.decrementAndGet();
        // 引用数仍大于0，还有其他在使用，先不做清理
        if (value > 0)
            return;

        // 没有其他在引用该文件
        synchronized (this) {

            // 清除掉文件
            this.cleanupOver = this.cleanup(value);
        }
    }

    public long getRefCount() {
        return this.refCount.get();
    }

    public abstract boolean cleanup(final long currentRef);

    /**
     * 文件是否已经被清除
     * @return
     */
    public boolean isCleanupOver() {
        return this.refCount.get() <= 0 && this.cleanupOver;
    }
}
