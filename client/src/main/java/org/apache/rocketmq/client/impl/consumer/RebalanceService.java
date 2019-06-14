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

import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.logging.InternalLogger;

/**
 * 消息负载均衡机制服务
 *   首先RebalanceService线程启动，为消费者分配消息队列，其实每一个MessageQueue会构建一个PullRequest对象，
 *   然后通过RebalanceImpl将PullRequest放入到PullMessageService线程的LinkedBlockingQueue，进而唤醒
 *   queue.take()方法，然后执行DefaultMQPushConsumerImpl的pullMessage，通过网络从broker端拉消息，一次
 *   最多拉取的消息条数可配置，默认1条，然后将拉取的消息，执行过滤等，然后封装成一个任务ConsumeRequest,提交到
 *   消费者的线程池去执行，每次消费消息后，又将该pullRequest放入到PullMessageService中
 */
public class RebalanceService extends ServiceThread {
    private static long waitInterval =
        Long.parseLong(System.getProperty(
            "rocketmq.client.rebalance.waitInterval", "20000"));
    private final InternalLogger log = ClientLogger.getLog();
    private final MQClientInstance mqClientFactory;

    public RebalanceService(MQClientInstance mqClientFactory) {
        this.mqClientFactory = mqClientFactory;
    }

    /**
     * 默认每20s进行一次消息队列重新负载，判断消息队列是否需要进行重新负载，
     * 如果消费者个数和主题的队列数没有发送变化，则继续保持原样。
     * 对于Pull模型，如果消费者监听某些主题队列发生事件，注册消息队列变更事件方法，则RebalanceService会将消息队列负载变化
     * 事件通知消费者。pull模式根据消息队列拉消息的方法，与Push模式走的逻辑相同，唯一的区别就是pull模式需要应用程序触发消息
     * 拉取动作。
     */
    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            this.waitForRunning(waitInterval);
            this.mqClientFactory.doRebalance();
        }

        log.info(this.getServiceName() + " service end");
    }

    @Override
    public String getServiceName() {
        return RebalanceService.class.getSimpleName();
    }
}
