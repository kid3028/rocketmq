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

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * 消息过滤接口
 * 消息发送者在消息发送时如果设置了消息的tags属性，存储在消息属性中，先存储在commitlog文件中，然后转发到消息消费队列中，消息消费队列会用8个字节存储消息tag的hashcode，
 * 之所以不直接存储tag字符串，是因为consumequeue设计为定长结构，加快消息消费的加载性能。在broker端拉取消息时，遍历consumequeue，只对比消息tag的hashcode，如果匹配则返回，否则忽略该消息。
 * consumer在收到消息后，同样需要先对消息进行过滤，只是此时比较的是消息的tag值而不是hashcode
 */
public interface MessageFilter {
    /**判断消息是否匹配
     * 根据ConsumeQueue
     * match by tags code or filter bit map which is calculated when message received
     * and stored in consume queue ext.
     *
     * @param tagsCode tagsCode 消息tag的hashcode
     * @param cqExtUnit extend unit of consume queue consumequeue条目扩张属性
     */
    boolean isMatchedByConsumeQueue(final Long tagsCode,
        final ConsumeQueueExt.CqExtUnit cqExtUnit);

    /**
     * 根据存储在commitlog文件中的内容判断消息是否匹配
     * match by message content which are stored in commit log.
     * <br>{@code msgBuffer} and {@code properties} are not all null.If invoked in store,
     * {@code properties} is null;If invoked in {@code PullRequestHoldService}, {@code msgBuffer} is null.
     *
     * @param msgBuffer message buffer in commit log, may be null if not invoked in store.  消息内容，如果为空，该方法返回true
     * @param properties message properties, should decode from buffer if null by yourself. 消息属性，主要用于sql92
     */
    boolean isMatchedByCommitLog(final ByteBuffer msgBuffer,
        final Map<String, String> properties);
}
