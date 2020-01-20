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
package org.apache.rocketmq.client.consumer.store;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.Map;
import java.util.Set;

/**
 * 首先消费者订阅消息消费队列(MessageQueue),当生产者将消息负载发送到MessageQueue中时，消费订阅者开始消费消息，
 * 消息消费过程中，为了避免重复消费，需要有一个地方保存消费进度(消费偏移量)。
 *
 * 消息消费模式分为集群模式、广播模式：
 *    集群模式：一条消息被集群中任何一个消息费者消费
 *    广播模式：每条消息都被每一个消费者消费
 * 广播模式，既然每条消息要被每一个消费者消费，则消费进度可以与消费者保存在一起，也就是消费者本地保存，
 * 但是由于集群模式下，一条消息只能被集群中的一个消费者消费，进度不能保存在消费端，只能集中保存在一个地方，即保存在Broker端
 * Offset store interface
 */
public interface OffsetStore {
    /**
     * Load
     * 从消息进度存储文件中加载消息进度到内存
     */
    void load() throws MQClientException;

    /**
     * Update the offset,store it in memory
     * 更新内存这种的消息消费进度
     * @param mq  消息消费队列
     * @param offset  消息消费偏移量
     * @param increaseOnly true表示offset必须大于内存中当前的消费偏移量才更新
     */
    void updateOffset(final MessageQueue mq, final long offset, final boolean increaseOnly);

    /**
     * Get offset from local storage
     * 读取消息消费进度
     * @param mq 消息消费队列
     * @param type 读取方式 READ_FROM_MEMORY从内存中读取 READ_FROM_STORE 从磁盘读取 MEMORY_FIRST_THEN_STORE 先从内存中读取，在从磁盘
     * @return
     */
    long readOffset(final MessageQueue mq, final ReadOffsetType type);

    /**
     * Persist all offsets,may be in local storage or remote name server
     * 持久化指定消息队列进度到磁盘
     */
    void persistAll(final Set<MessageQueue> mqs);

    /**
     * Persist the offset,may be in local storage or remote name server
     */
    void persist(final MessageQueue mq);

    /**
     * Remove offset
     * 将消息队列的消息消费进度从主内存中移除
     */
    void removeOffset(MessageQueue mq);

    /**
     * 克隆该主题下所有消息队列的消息消费进度
     * @return The cloned offset table of given topic
     */
    Map<MessageQueue, Long> cloneOffsetTable(String topic);

    /**
     * 更新存储在broker端的消息消费进度，使用集群模式
     * @param mq
     * @param offset
     * @param isOneway
     */
    void updateConsumeOffsetToBroker(MessageQueue mq, long offset, boolean isOneway) throws RemotingException,
        MQBrokerException, InterruptedException, MQClientException;
}
