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
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.InternalLogger;

import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

/**
 * 从LinkedBlockingQueue中循环取出PullRequest对象，然后执行pullMessage
 */
public class PullMessageService extends ServiceThread {
    private final InternalLogger log = ClientLogger.getLog();
    // 向pullRequestQueue中放入PullRequestHoldService的是RebalanceService#run方法
    private final LinkedBlockingQueue<PullRequest> pullRequestQueue = new LinkedBlockingQueue<PullRequest>();
    private final MQClientInstance mQClientFactory;
    private final ScheduledExecutorService scheduledExecutorService = Executors
        .newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "PullMessageServiceScheduledThread");
            }
        });

    public PullMessageService(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }

    /**
     * 延迟添加PullRequest
     * @param pullRequest
     * @param timeDelay
     */
    public void executePullRequestLater(final PullRequest pullRequest, final long timeDelay) {
        if (!isStopped()) {
            this.scheduledExecutorService.schedule(new Runnable() {
                @Override
                public void run() {
                    PullMessageService.this.executePullRequestImmediately(pullRequest);
                }
            }, timeDelay, TimeUnit.MILLISECONDS);
        } else {
            log.warn("PullMessageServiceScheduledThread has shutdown");
        }
    }

    /**
     * 立即添加PullRequest
     * @param pullRequest
     */
    public void executePullRequestImmediately(final PullRequest pullRequest) {
        try {
            this.pullRequestQueue.put(pullRequest);
        } catch (InterruptedException e) {
            log.error("executePullRequestImmediately pullRequestQueue.put", e);
        }
    }

    public void executeTaskLater(final Runnable r, final long timeDelay) {
        if (!isStopped()) {
            this.scheduledExecutorService.schedule(r, timeDelay, TimeUnit.MILLISECONDS);
        } else {
            log.warn("PullMessageServiceScheduledThread has shutdown");
        }
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    /**
     * 消息拉取服务
     *   这个服务是一个单独运行的线程，在收到pull请求之后异步执行
     * @param pullRequest
     */
    private void pullMessage(final PullRequest pullRequest) {
        // 根据消费者组名从MQXClientInstance中获取消费者内部实现类MQConsumerInner
        final MQConsumerInner consumer = this.mQClientFactory.selectConsumer(pullRequest.getConsumerGroup());
        if (consumer != null) {
            DefaultMQPushConsumerImpl impl = (DefaultMQPushConsumerImpl) consumer;
            // 真正执行消息拉取
            impl.pullMessage(pullRequest);
        } else {
            log.warn("No matched consumer for the PullRequest {}, drop it", pullRequest);
        }
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            try {
                /**
                 * PullRequest的产生
                 * PullRequest是由RebalanceService产生的，它根据主题消息队列个数和当前消费组内消费者个数进行负载
                 * {@link RebalanceImpl#updateProcessQueueTableInRebalance(String, Set, boolean)} ，更新负载信息，
                 * 然后产生对应的PullRequest对象，调用{@link RebalanceImpl#dispatchPullRequest(List)}，再将这些对象放入到PullMessageService的pullRequestQueue队列中，
                 *
                 * 在pull模式下{@link RebalancePullImpl#dispatchPullRequest(List)} 实现为空，所以这里的拉消息将不会真正执行拉取，
                 * 因为此时pullRequestQueue为空，调用take将会阻塞
                 *
                 * 因此PullMess只为push模式服务，RebalanceService进行路由重新分配时，如果是RebalancePullImpl，并不会产生PullRequest，
                 *
                 * {@link LinkedBlockingQueue#take()} 如果队列为空，发生阻塞，等待有元素
                 * {@link LinkedBlockingQueue#poll()} 如果队列为空，返回null
                 * {@link LinkedBlockingQueue#remove()} 如果队列为空，抛出NoSuchElementException
                 */
                // 从pullRequestQueue中获取一个PullRequest消息拉取任务，如果pullRequestQueue为空，则线程将阻塞，直到有拉取任务放入
                PullRequest pullRequest = this.pullRequestQueue.take();
                // 拉取消息
                this.pullMessage(pullRequest);
            } catch (InterruptedException ignored) {
            } catch (Exception e) {
                log.error("Pull Message Service Run Method exception", e);
            }
        }

        log.info(this.getServiceName() + " service end");
    }

    @Override
    public void shutdown(boolean interrupt) {
        super.shutdown(interrupt);
        ThreadUtils.shutdownGracefully(this.scheduledExecutorService, 1000, TimeUnit.MILLISECONDS);
    }

    @Override
    public String getServiceName() {
        return PullMessageService.class.getSimpleName();
    }

}
