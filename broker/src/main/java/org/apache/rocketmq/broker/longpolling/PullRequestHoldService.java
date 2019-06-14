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
package org.apache.rocketmq.broker.longpolling;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.ConsumeQueueExt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 消息拉取时为了提交网络性能，在消息服务端根据拉去偏移量去物理文件查找消息时，如果没有找到，并不会立即返回消费未找到，
 * 而是会将该线程挂起一段时间，然后重试。
 * 挂起分为长轮询和短轮询，在broker端可以通过longPullingEnable=true来开启长轮询，设置longPullingEnable=true开启短轮询。
 * 短轮询：第一次未拉取到消息后等待shortPollingTimeMillis时间后再试。默认1s
 * 长轮询：根据消费者设置的挂起超时时间，DefaultMQPullConsumer#brokerSuspendMaxTimeMillis,默认20s
 *
 * 长轮询通过两个线程实现：
 *   1.PullRequestHoldService，每隔5s重试一次
 *   2.DefaultMessageStore#ReputMessageService
 *   每当有消息达到后，转发消息，然后调用PullRequestHoldService线程中拉去任务，尝试拉取，每处理一次，Thread.sleep(1)，继续下次检查
 *
 */
public class PullRequestHoldService extends ServiceThread {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final String TOPIC_QUEUEID_SEPARATOR = "@";
    private final BrokerController brokerController;
    private final SystemClock systemClock = new SystemClock();
    private ConcurrentMap<String/* topic@queueId */, ManyPullRequest> pullRequestTable =
        new ConcurrentHashMap<String, ManyPullRequest>(1024);

    public PullRequestHoldService(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**
     * hold住提交的pullRequest
     *   pullRequest被放入一个以topic@queueId为key的Map队列中异步处理，任务被放入等待队列后，有两种情况会被执行：
     *      1.定时
     *      2.broker有新的消息达到
     * @param topic
     * @param queueId
     * @param pullRequest
     */
    public void suspendPullRequest(final String topic, final int queueId, final PullRequest pullRequest) {
        // key = topic@queueId
        String key = this.buildKey(topic, queueId);
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (null == mpr) {
            mpr = new ManyPullRequest();
            ManyPullRequest prev = this.pullRequestTable.putIfAbsent(key, mpr);
            if (prev != null) {
                mpr = prev;
            }
        }
        // 放入map队列中异步执行
        mpr.addPullRequest(pullRequest);
    }

    private String buildKey(final String topic, final int queueId) {
        StringBuilder sb = new StringBuilder();
        sb.append(topic);
        sb.append(TOPIC_QUEUEID_SEPARATOR);
        sb.append(queueId);
        return sb.toString();
    }

    /**
     * 遍历hold住pullRequest
     */
    @Override
    public void run() {
        log.info("{} service started", this.getServiceName());
        while (!this.isStopped()) {
            try {
                // 如果开启了长轮询模式，每次挂起5s后尝试拉取
                if (this.brokerController.getBrokerConfig().isLongPollingEnable()) {
                    this.waitForRunning(5 * 1000);
                } else {
                    // 短轮询，只挂起一次，挂起时间为shortPollingTimeMillis，然后便尝试获取消息
                    this.waitForRunning(this.brokerController.getBrokerConfig().getShortPollingTimeMills());
                }

                long beginLockTimestamp = this.systemClock.now();
                // 定期检查队列中request重新执行
                this.checkHoldRequest();
                long costTime = this.systemClock.now() - beginLockTimestamp;
                if (costTime > 5 * 1000) {
                    log.info("[NOTIFYME] check hold request cost {} ms.", costTime);
                }
            } catch (Throwable e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        log.info("{} service end", this.getServiceName());
    }

    @Override
    public String getServiceName() {
        return PullRequestHoldService.class.getSimpleName();
    }

    /**
     * 定时执行的检查操作
     */
    private void checkHoldRequest() {
        for (String key : this.pullRequestTable.keySet()) {
            String[] kArray = key.split(TOPIC_QUEUEID_SEPARATOR);
            if (2 == kArray.length) {
                String topic = kArray[0];
                int queueId = Integer.parseInt(kArray[1]);
                // 获取topic queueId的最新偏移量
                final long offset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
                try {
                    // 唤醒等待线程，判断是否有新的消息到达
                    this.notifyMessageArriving(topic, queueId, offset);
                } catch (Throwable e) {
                    log.error("check hold request failed. topic={}, queueId={}", topic, queueId, e);
                }
            }
        }
    }

    public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset) {
        notifyMessageArriving(topic, queueId, maxOffset, null, 0, null, null);
    }

    /**
     * 定时任务会定时调用该方法
     * 当ReputMessageService发现有新消息时，也会调用该方法
     * @param topic 主题
     * @param queueId 队列
     * @param maxOffset 消息队列当前最大偏移量
     * @param tagsCode 消息tag hashcode，基于tag过滤
     * @param msgStoreTime 消息存储时间
     * @param filterBitMap 过滤位图
     * @param properties
     */
    public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset, final Long tagsCode,
        long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
        // topic@queueId
        String key = this.buildKey(topic, queueId);
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (mpr != null) {
            // clone等待的List，同时会清空等待列表，保证线程安全
            List<PullRequest> requestList = mpr.cloneListAndClear();
            if (requestList != null) {
                List<PullRequest> replayList = new ArrayList<PullRequest>();

                // 循环执行Request
                for (PullRequest request : requestList) {
                    long newestOffset = maxOffset;
                    if (newestOffset <= request.getPullFromThisOffset()) {
                        newestOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
                    }

                    // 判断等待的时间内有没有新的消息进来
                    if (newestOffset > request.getPullFromThisOffset()) {
                        // 判断消息是否符合过滤条件，对于定时唤醒任务，match=true
                        boolean match = request.getMessageFilter().isMatchedByConsumeQueue(tagsCode,
                            new ConsumeQueueExt.CqExtUnit(tagsCode, msgStoreTime, filterBitMap));
                        // match by bit map, need eval again when properties is not null.
                        if (match && properties != null) {
                            match = request.getMessageFilter().isMatchedByCommitLog(null, properties);
                        }

                        if (match) {
                            try {
                                // 消息符合request的过滤条件，重新通过PullRequestProcessor执行消息读取
                                this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(request.getClientChannel(),
                                    request.getRequestCommand());
                            } catch (Throwable e) {
                                log.error("execute request when wakeup failed.", e);
                            }
                            continue;
                        }
                    }

                    // 如果request等待超时，无论前一步是否符合条件，肯定会发送给processor
                    if (System.currentTimeMillis() >= (request.getSuspendTimestamp() + request.getTimeoutMillis())) {
                        try {
                            this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(request.getClientChannel(),
                                request.getRequestCommand());
                        } catch (Throwable e) {
                            log.error("execute request when wakeup failed.", e);
                        }
                        continue;
                    }

                    replayList.add(request);
                }

                // 未超时和不符合过滤条件的request，重新加入队列等待
                if (!replayList.isEmpty()) {
                    mpr.addPullRequest(replayList);
                }
            }
        }
    }
}
