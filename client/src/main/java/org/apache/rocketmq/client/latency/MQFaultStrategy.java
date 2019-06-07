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

package org.apache.rocketmq.client.latency;

import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

public class MQFaultStrategy {
    private final static InternalLogger log = ClientLogger.getLog();
    // 延迟故障容错，维护每个Broker的发送消息的延迟
    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();
    // 发送消息延迟容错开关：producer消息发送容错策略，默认情况下容错策略关闭
    private boolean sendLatencyFaultEnable = false;
    // 消息发送时长的延迟级别数组：当消息发送延迟的区间选择，与下面broker的notAvailableDuration数组对应
    private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
    // broker预估不可用时长数组
    private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};

    public long[] getNotAvailableDuration() {
        return notAvailableDuration;
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.notAvailableDuration = notAvailableDuration;
    }

    public long[] getLatencyMax() {
        return latencyMax;
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.latencyMax = latencyMax;
    }

    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    /**
     * 根据topic发布信息选择一个消息队列
     * queue的选择除了轮询之外，还可以通过broker的可用性
     *    producer为每个topic缓存了一个全局的index，每次发送之后+1，然后从所有Queue列表中选择index位置上的Queue，这样就实现了负载均衡的效果
     *    如果开启了延时容错，则会考虑broker的可用性：
     *      1.根据index全局找到Queue
     *      2.如果根据延时容错判断Queue所在的broker当前可用，并且时第一次发送，或者时重试并且和上次用的broker是同一个，则使用这个Queue。
     *        这里有两个逻辑，一个是broker的可用性是如何判断的，第二个是为什么重试的时候还要选上次的broker。
     *        在发送的逻辑中，出现重试的情况可能为:第一种：broker返回处理成功，但是store失败；第二种：broker返回失败
     *        对于返回失败其实会直接更新broker为短时不可用状态，这个在第一个if条件就已经通不过；对于store失败的情况，说明broker当前是正常的，
     *        重发还是发给同一个broker有利于防止消息重复。
     * @param tpInfo topic发布信息
     * @param lastBrokerName
     * @return
     */
    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        // 开启了延时容错
        if (this.sendLatencyFaultEnable) {
            try {
                // 1.获取上次使用的Queue index + 1
                int index = tpInfo.getSendWhichQueue().getAndIncrement();
                for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
                    int pos = Math.abs(index++) % tpInfo.getMessageQueueList().size();
                    if (pos < 0)
                        pos = 0;
                    // 2.找到一个对应的Queue
                    MessageQueue mq = tpInfo.getMessageQueueList().get(pos);
                    // 3.如果queue对应的broker可用，则使用该broker
                    if (latencyFaultTolerance.isAvailable(mq.getBrokerName())) {
                        if (null == lastBrokerName || mq.getBrokerName().equals(lastBrokerName))
                            return mq;
                    }
                }
                // 4.如果没有找到一个合适的broker，选择一个相对好的broker，并获得其相对应的一个消息队列，不考虑队列的可用性
                final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();
                int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);
                if (writeQueueNums > 0) {
                    final MessageQueue mq = tpInfo.selectOneMessageQueue();
                    if (notBestBroker != null) {
                        mq.setBrokerName(notBestBroker);
                        mq.setQueueId(tpInfo.getSendWhichQueue().getAndIncrement() % writeQueueNums);
                    }
                    return mq;
                } else {
                    latencyFaultTolerance.remove(notBestBroker);
                }
            } catch (Exception e) {
                log.error("Error occurred when selecting message queue", e);
            }
            // 5.如果以上都没有找到，则直接按顺序选择下一个
            return tpInfo.selectOneMessageQueue();
        }
        // 6.未开启延时容错，直接按顺序选下一个
        return tpInfo.selectOneMessageQueue(lastBrokerName);
    }

    /**
     * broker的延时控制策略
     *   queue的选择除了轮询之外，还可以通过broker的可用性，在消息发送后会更新时间和发送状态到MQFaultStrategy
     * @param brokerName
     * @param currentLatency
     * @param isolation
     */
    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        if (this.sendLatencyFaultEnable) {
            // 根据发送结果，计算broker不可用时长
            long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
            // 更新broker不可用时长
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
        }
    }

    private long computeNotAvailableDuration(final long currentLatency) {
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            if (currentLatency >= latencyMax[i])
                return this.notAvailableDuration[i];
        }

        return 0;
    }
}
