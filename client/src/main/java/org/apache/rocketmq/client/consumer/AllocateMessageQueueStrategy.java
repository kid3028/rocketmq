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
package org.apache.rocketmq.client.consumer;

import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;

/**
 * Queue分配策略
 *   AVG：平均分配
 *   AVG_BY_CIRCLE:跟AVG类似，只是分到的queue不是连续的。比如一共12个queue，3个consumer，则第一个consumer接收1,4,7,9的消息
 *   CONSISTENT_HASH：使用一致性hash算法来分配queue，用户需要自定义虚拟节点的数量
 *   MACHINE_ROOM：将queue先按照broker划分为几个computer room，不同的consumer只消费某几个broker上的消息
 *   CONFIG：用户启动时指定消费哪些queue消息
 * Strategy Algorithm for message allocating between consumers
 */
public interface AllocateMessageQueueStrategy {

    /**
     * Allocating by consumer id
     *
     * @param consumerGroup current consumer group
     * @param currentCID current consumer id
     * @param mqAll message queue set in current topic
     * @param cidAll consumer set in current consumer group
     * @return The allocate result of given strategy
     */
    List<MessageQueue> allocate(
        final String consumerGroup,
        final String currentCID,
        final List<MessageQueue> mqAll,
        final List<String> cidAll
    );

    /**
     * Algorithm name
     *
     * @return The strategy name
     */
    String getName();
}
