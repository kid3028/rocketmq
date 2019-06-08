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
package org.apache.rocketmq.remoting;

import io.netty.channel.Channel;
import org.apache.rocketmq.remoting.common.Pair;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.concurrent.ExecutorService;

public interface RemotingServer extends RemotingService {

    /**
     * 注册一个处理请求的处理器，根据requestCode，获取处理器，处理请求
     * 注册一个请求处理器，存放到HashMap中，requestCode为Map的key
     * @param requestCode
     * @param processor
     * @param executor
     */
    // HashMap<Integer /*request code*/, Pair<NettyProcessor, ExecutorService>> processorTable
    void registerProcessor(final int requestCode, final NettyRequestProcessor processor,
        final ExecutorService executor);

    /**
     * 注册一个默认的处理器，当根据requestCode匹配不到处理器，则使用这个默认的处理器
     * @param processor
     * @param executor
     */
    void registerDefaultProcessor(final NettyRequestProcessor processor, final ExecutorService executor);

    /**
     * 获取端口
     * @return
     */
    int localListenPort();

    /**
     * 根据requestCode获取请求处理器
     * @param requestCode
     * @return
     */
    Pair<NettyRequestProcessor, ExecutorService> getProcessorPair(final int requestCode);

    /**
     * 同步调用(同步发送消息)
     * @param channel
     * @param request
     * @param timeoutMillis
     * @return
     * @throws InterruptedException
     * @throws RemotingSendRequestException
     * @throws RemotingTimeoutException
     */
    RemotingCommand invokeSync(final Channel channel, final RemotingCommand request,
        final long timeoutMillis) throws InterruptedException, RemotingSendRequestException,
        RemotingTimeoutException;

    /**
     * 异步调用(异步发送消息)
     * @param channel
     * @param request
     * @param timeoutMillis
     * @param invokeCallback
     * @throws InterruptedException
     * @throws RemotingTooMuchRequestException
     * @throws RemotingTimeoutException
     * @throws RemotingSendRequestException
     */
    void invokeAsync(final Channel channel, final RemotingCommand request, final long timeoutMillis,
        final InvokeCallback invokeCallback) throws InterruptedException,
        RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException;

    /**
     * 单向发送消息，只发送消息，不处理发送结果
     * @param channel
     * @param request
     * @param timeoutMillis
     * @throws InterruptedException
     * @throws RemotingTooMuchRequestException
     * @throws RemotingTimeoutException
     * @throws RemotingSendRequestException
     */
    void invokeOneway(final Channel channel, final RemotingCommand request, final long timeoutMillis)
        throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException,
        RemotingSendRequestException;

}
