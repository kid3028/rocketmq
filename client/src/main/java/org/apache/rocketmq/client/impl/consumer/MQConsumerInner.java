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
package org.apache.rocketmq.client.impl.consumer;

import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

import java.util.Set;

/**
 * Consumer分为两种：PullConsumer、PushConsumer。
 *     PullConsumer，由用户主动调用pull方法来获取消息，没有则返回
 *     PushConsumer，在启动后，Consumer客户端会主动循环发送Pull请求到broker，如果没有消息，broker会把请求放入等待队里，新消息到达后返回response
 *     本质上，两种方式都是通过客户端pull实现的
 *
 * 消费模式
 *     Consumer有两种消费模式，broadcast和cluster，由初始化consumer时设置。对于消费同一个topic的多个consumer，可以通过设置同一个consumerGroup来标识属于同一个消费集群。
 *     在Broadcast模式下，消息会发送给group内的所有consumer
 *     在Cluster模式下，每条消息只会发送给group内的一个consumer，但是集群模式支持消费失败重发，从而保证消息一定会被消费
 * Consumer inner interface
 */
public interface MQConsumerInner {
    String groupName();

    MessageModel messageModel();

    ConsumeType consumeType();

    ConsumeFromWhere consumeFromWhere();

    Set<SubscriptionData> subscriptions();

    void doRebalance();

    void persistConsumerOffset();

    void updateTopicSubscribeInfo(final String topic, final Set<MessageQueue> info);

    boolean isSubscribeTopicNeedUpdate(final String topic);

    boolean isUnitMode();

    ConsumerRunningInfo consumerRunningInfo();
}
