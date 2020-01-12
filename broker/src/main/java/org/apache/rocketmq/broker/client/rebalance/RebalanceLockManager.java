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
package org.apache.rocketmq.broker.client.rebalance;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 消息消费的各个环节基本都是围绕消息消费队列(messageQueue)和消息处理队列(processQueue)展开的，
 * 消息消费进度拉取，消息消费进度都要判断ProcessQueue的locked是否为true，locked为true的前提条件是消息消费者cid
 * 向broker端发送锁定消息队列的请求并返回加锁成功。
 *
 * RebalacnceLockManager便是服务端加锁的处理类
 *
 */
public class RebalanceLockManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.REBALANCE_LOCK_LOGGER_NAME);
    // 锁最大存活时间，，默认60s
    private final static long REBALANCE_LOCK_MAX_LIVE_TIME = Long.parseLong(System.getProperty(
        "rocketmq.broker.rebalance.lockMaxLiveTime", "60000"));
    private final Lock lock = new ReentrantLock();
    // 消息消费队列锁容器，按消费组分组。不同的消费组同一个消费队列可以同时加锁，同一个消费组内同一个消费队列只允许一个消费者加锁成功
    private final ConcurrentMap<String/* group */, ConcurrentHashMap<MessageQueue, LockEntry>> mqLockTable =
        new ConcurrentHashMap<String, ConcurrentHashMap<MessageQueue, LockEntry>>(1024);

    /**
     * 尝试锁住某个队列
     * @param group
     * @param mq
     * @param clientId
     * @return
     */
    public boolean tryLock(final String group, final MessageQueue mq, final String clientId) {

        // mq是没有被clientId锁定
        if (!this.isLocked(group, mq, clientId)) {
            try {
                this.lock.lockInterruptibly();
                try {
                    // 获取group下队列锁定状态
                    ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
                    if (null == groupValue) {
                        groupValue = new ConcurrentHashMap<>(32);
                        this.mqLockTable.put(group, groupValue);
                    }

                    // 获取队列mq对应的锁
                    LockEntry lockEntry = groupValue.get(mq);
                    // 队列mq的锁被释放了
                    if (null == lockEntry) {
                        // 构建新的锁
                        lockEntry = new LockEntry();
                        // 设置锁有clientId持有
                        lockEntry.setClientId(clientId);
                        groupValue.put(mq, lockEntry);
                        log.info("tryLock, message queue not locked, I got it. Group: {} NewClientId: {} {}",
                            group,
                            clientId,
                            mq);
                    }

                    // 锁由clientId持有
                    if (lockEntry.isLocked(clientId)) {
                        // 更新锁时间
                        lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                        return true;
                    }

                    String oldClientId = lockEntry.getClientId();

                    // 如果锁过期了，更新锁持有者为clientId
                    if (lockEntry.isExpired()) {
                        lockEntry.setClientId(clientId);
                        lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                        log.warn(
                            "tryLock, message queue lock expired, I got it. Group: {} OldClientId: {} NewClientId: {} {}",
                            group,
                            oldClientId,
                            clientId,
                            mq);
                        return true;
                    }

                    log.warn(
                        "tryLock, message queue locked by other client. Group: {} OtherClientId: {} NewClientId: {} {}",
                        group,
                        oldClientId,
                        clientId,
                        mq);
                    return false;
                } finally {
                    this.lock.unlock();
                }
            } catch (InterruptedException e) {
                log.error("putMessage exception", e);
            }
        } else {

        }

        return true;
    }

    /**
     * 消费组group中，队列mq是否被clientId锁住
     * @param group
     * @param mq
     * @param clientId
     * @return
     */
    private boolean isLocked(final String group, final MessageQueue mq, final String clientId) {
        // 获取group下队列锁定关系
        ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
        if (groupValue != null) {
            // 获取队列mq对应的锁
            LockEntry lockEntry = groupValue.get(mq);
            if (lockEntry != null) {
                // 锁lockEntry是否被clientId持有
                boolean locked = lockEntry.isLocked(clientId);
                if (locked) {
                    // 更新锁时间
                    lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                }

                return locked;
            }
        }

        return false;
    }

    /**
     * 申请对mqs消息消费队列集合加锁
     * @param group 消费组
     * @param mqs 待加锁的消息消费队列集合
     * @param clientId 消息消费者
     * @return 返回成功加锁的消息队列集合
     */
    public Set<MessageQueue> tryLockBatch(final String group, final Set<MessageQueue> mqs,
        final String clientId) {
        Set<MessageQueue> lockedMqs = new HashSet<MessageQueue>(mqs.size());
        Set<MessageQueue> notLockedMqs = new HashSet<MessageQueue>(mqs.size());

        // 遍历mq
        for (MessageQueue mq : mqs) {
            // 如果mq是由clientId锁定的，加入lockedMqs set
            if (this.isLocked(group, mq, clientId)) {
                lockedMqs.add(mq);
            } else {
                // mq不是由clientId锁定，加入notLockMqs set
                notLockedMqs.add(mq);
            }
        }

        // 未锁定队列不空
        if (!notLockedMqs.isEmpty()) {
            try {
                this.lock.lockInterruptibly();
                try {
                    ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
                    if (null == groupValue) {
                        groupValue = new ConcurrentHashMap<>(32);
                        this.mqLockTable.put(group, groupValue);
                    }

                    // 遍历未锁定队列
                    for (MessageQueue mq : notLockedMqs) {
                        // 队列锁
                        LockEntry lockEntry = groupValue.get(mq);
                        // 队列没有被锁定，
                        if (null == lockEntry) {
                            lockEntry = new LockEntry();
                            // 将mq队列的持有者设置为clientId
                            lockEntry.setClientId(clientId);
                            groupValue.put(mq, lockEntry);
                            log.info(
                                "tryLockBatch, message queue not locked, I got it. Group: {} NewClientId: {} {}",
                                group,
                                clientId,
                                mq);
                        }

                        // 如果mq已经被clientId锁住，更新锁
                        if (lockEntry.isLocked(clientId)) {
                            lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                            lockedMqs.add(mq);
                            continue;
                        }

                        String oldClientId = lockEntry.getClientId();

                        // 如果mq对应的锁，失效了，设置锁由clientId持有，并更新锁
                        if (lockEntry.isExpired()) {
                            lockEntry.setClientId(clientId);
                            lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                            log.warn(
                                "tryLockBatch, message queue lock expired, I got it. Group: {} OldClientId: {} NewClientId: {} {}",
                                group,
                                oldClientId,
                                clientId,
                                mq);
                            lockedMqs.add(mq);
                            continue;
                        }

                        log.warn(
                            "tryLockBatch, message queue locked by other client. Group: {} OtherClientId: {} NewClientId: {} {}",
                            group,
                            oldClientId,
                            clientId,
                            mq);
                    }
                } finally {
                    this.lock.unlock();
                }
            } catch (InterruptedException e) {
                log.error("putMessage exception", e);
            }
        }

        return lockedMqs;
    }

    /**
     * 对消息队列集合解锁
     * @param group 消息消费组名
     * @param mqs 待解锁的消息消费队列集合
     * @param clientId 当前消息队列锁拥有者
     */
    public void unlockBatch(final String group, final Set<MessageQueue> mqs, final String clientId) {
        try {
            this.lock.lockInterruptibly();
            try {
                ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
                if (null != groupValue) {
                    // 遍历mq
                    for (MessageQueue mq : mqs) {
                        // 获取mq上的锁
                        LockEntry lockEntry = groupValue.get(mq);
                        // 锁存在
                        if (null != lockEntry) {
                            // 锁有clientId持有
                            if (lockEntry.getClientId().equals(clientId)) {
                                // 移除锁定关系
                                groupValue.remove(mq);
                                log.info("unlockBatch, Group: {} {} {}",
                                    group,
                                    mq,
                                    clientId);
                            } else {
                                log.warn("unlockBatch, but mq locked by other client: {}, Group: {} {} {}",
                                    lockEntry.getClientId(),
                                    group,
                                    mq,
                                    clientId);
                            }
                        } else {
                            log.warn("unlockBatch, but mq not locked, Group: {} {} {}",
                                group,
                                mq,
                                clientId);
                        }
                    }
                } else {
                    log.warn("unlockBatch, group not exist, Group: {} {}",
                        group,
                        clientId);
                }
            } finally {
                this.lock.unlock();
            }
        } catch (InterruptedException e) {
            log.error("putMessage exception", e);
        }
    }

    static class LockEntry {
        private String clientId;
        private volatile long lastUpdateTimestamp = System.currentTimeMillis();

        public String getClientId() {
            return clientId;
        }

        public void setClientId(String clientId) {
            this.clientId = clientId;
        }

        public long getLastUpdateTimestamp() {
            return lastUpdateTimestamp;
        }

        public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
            this.lastUpdateTimestamp = lastUpdateTimestamp;
        }

        public boolean isLocked(final String clientId) {
            boolean eq = this.clientId.equals(clientId);
            return eq && !this.isExpired();
        }

        public boolean isExpired() {
            boolean expired =
                (System.currentTimeMillis() - this.lastUpdateTimestamp) > REBALANCE_LOCK_MAX_LIVE_TIME;

            return expired;
        }
    }
}
