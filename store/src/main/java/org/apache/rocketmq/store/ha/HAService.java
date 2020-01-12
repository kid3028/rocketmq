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
package org.apache.rocketmq.store.ha;

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 主从同步核心类
 * HA机制大概步骤
 *   1.master启动并监听slave的连接请求你
 *   2.slave启动，与master建立连接
 *   3.slave发送待拉取偏移量，待master返回数据，持续该过程
 */
public class HAService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final AtomicInteger connectionCount = new AtomicInteger(0);

    private final List<HAConnection> connectionList = new LinkedList<>();
    // 接收slave的连接并构建HAConnection实例
    private final AcceptSocketService acceptSocketService;
    // BrokerRole为SYNC_MASTER的情况下，GroupTransferService会作为一个中间服务，设置一个标识位，用来判断slave是否已经同步完成
    private final GroupTransferService groupTransferService;
    // broker存储实现
    private final DefaultMessageStore defaultMessageStore;
    // 同步等待实现
    private final WaitNotifyObject waitNotifyObject = new WaitNotifyObject();
    // 该master所有slave中同步最大的偏移量
    private final AtomicLong push2SlaveMaxOffset = new AtomicLong(0);

    // HA客户端，Slave端网络的实现类
    private final HAClient haClient;

    public HAService(final DefaultMessageStore defaultMessageStore) throws IOException {
        this.defaultMessageStore = defaultMessageStore;
        // 监听端口 broker+1  10912
        this.acceptSocketService =
            new AcceptSocketService(defaultMessageStore.getMessageStoreConfig().getHaListenPort());
        this.groupTransferService = new GroupTransferService();
        this.haClient = new HAClient();
    }

    /**
     * 更新HAMaster地址
     * @param newAddr
     */
    public void updateMasterAddress(final String newAddr) {
        if (this.haClient != null) {
            this.haClient.updateMasterAddress(newAddr);
        }
    }

    public void putRequest(final CommitLog.GroupCommitRequest request) {
        this.groupTransferService.putRequest(request);
    }

    /**
     * slave如果落后太多，或者没有slave连接，返回false
     * @param masterPutWhere
     * @return
     */
    public boolean isSlaveOK(final long masterPutWhere) {
        boolean result = this.connectionCount.get() > 0;
        result =
            result
                && ((masterPutWhere - this.push2SlaveMaxOffset.get()) < this.defaultMessageStore
                .getMessageStoreConfig().getHaSlaveFallbehindMax());
        return result;
    }

    /**
     * 该方法是在master收到从服务器的拉取请求(slave下一次待拉取的消息偏移量，或者说是slave的拉取偏移量确认信息)
     * 先获取当前master的最大偏移量，再跟传入的slave的maxOffset比较，如果大于则忽略，如果当前master的最大偏移量
     * 小于传入的slave的maxOffset，那么通过CAS将push2SlaveMaxOffset改为slave的maxOffset，
     * 并且调用groupTransferService的notifyTransferSome()方法唤起前端线程
     * @param offset
     */
    public void notifyTransferSome(final long offset) {
        for (long value = this.push2SlaveMaxOffset.get(); offset > value; ) {
            // CAS更新push2SlaveMaxOffset的值为当前slave同步到的offset
            boolean ok = this.push2SlaveMaxOffset.compareAndSet(value, offset);
            if (ok) {
                // 进行通知
                this.groupTransferService.notifyTransferSome();
                break;
            } else {
                value = this.push2SlaveMaxOffset.get();
            }
        }
    }

    public AtomicInteger getConnectionCount() {
        return connectionCount;
    }

    // public void notifyTransferSome() {
    // this.groupTransferService.notifyTransferSome();
    // }

    /**
     * master与slave之间commitLog的HA传输
     * HAService.start()会启动相关的服务
     *    AcceptSocketService ：启动serverSocket并监听来自HAClient的连接
     *    GroupTransferService：broker写消息的时候如果需要同步等待消息同步到slave，会调用这个服务
     *    HAClient：如果是slave，才会启动HAClient
     *
     * 不管master还是slave都会按照这个流程启动，在内部的实现会根据broker配置来决定真正开启的流程
     */
    public void start() throws Exception {
        // 建立HA服务端监听服务，处理客户端slave客户端监听请求
        this.acceptSocketService.beginAccept();
        // 启动AcceptSocketService，处理监听逻辑
        this.acceptSocketService.start();
        // 启动GroupTransferService线程
        this.groupTransferService.start();
        // 启动HAClient线程
        this.haClient.start();
    }

    public void addConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.add(conn);
        }
    }

    public void removeConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.remove(conn);
        }
    }

    public void shutdown() {
        this.haClient.shutdown();
        this.acceptSocketService.shutdown(true);
        this.destroyConnections();
        this.groupTransferService.shutdown();
    }

    public void destroyConnections() {
        synchronized (this.connectionList) {
            for (HAConnection c : this.connectionList) {
                c.shutdown();
            }

            this.connectionList.clear();
        }
    }

    public DefaultMessageStore getDefaultMessageStore() {
        return defaultMessageStore;
    }

    public WaitNotifyObject getWaitNotifyObject() {
        return waitNotifyObject;
    }

    public AtomicLong getPush2SlaveMaxOffset() {
        return push2SlaveMaxOffset;
    }

    /**
     * AcceptSocketService中使用原生的Nio实现，run方法是核心，主要用来接收slave的连接然后构建HAConnection对象
     * Listens to slave connections to create {@link HAConnection}.
     */
    class AcceptSocketService extends ServiceThread {
        // broker服务监听套接字 ip+port
        private final SocketAddress socketAddressListen;
        // 服务端socket通道，基于NIO
        private ServerSocketChannel serverSocketChannel;
        // 事件选择器，基于Nio
        private Selector selector;

        public AcceptSocketService(final int port) {
            this.socketAddressListen = new InetSocketAddress(port);
        }

        /**
         * Starts listening to slave connections.
         *
         * @throws Exception If fails.
         */
        public void beginAccept() throws Exception {
            this.serverSocketChannel = ServerSocketChannel.open();
            this.selector = RemotingUtil.openSelector();
            this.serverSocketChannel.socket().setReuseAddress(true);
            this.serverSocketChannel.socket().bind(this.socketAddressListen);
            this.serverSocketChannel.configureBlocking(false);
            this.serverSocketChannel.register(this.selector, SelectionKey.OP_ACCEPT);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void shutdown(final boolean interrupt) {
            super.shutdown(interrupt);
            try {
                this.serverSocketChannel.close();
                this.selector.close();
            } catch (IOException e) {
                log.error("AcceptSocketService shutdown exception", e);
            }
        }

        /**
         * 在该端口上只接受新的连接，当新连接建立之后，将把新连接channel传递到HAConnection，
         * HAConnection再将channel分别传递给ReadSocketService和WriteSocketService分别绑定在selector并注册读写事件
         * {@inheritDoc}
         */
        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 阻塞等待事件触发
                    this.selector.select(1000);
                    // 获取到所有有事件的key
                    Set<SelectionKey> selected = this.selector.selectedKeys();

                    if (selected != null) {
                        // 遍历处理key
                        for (SelectionKey k : selected) {
                            // 如果是连接请求
                            if ((k.readyOps() & SelectionKey.OP_ACCEPT) != 0) {
                                // 接收连接
                                SocketChannel sc = ((ServerSocketChannel) k.channel()).accept();

                                if (sc != null) {
                                    HAService.log.info("HAService receive new connection, "
                                        + sc.socket().getRemoteSocketAddress());

                                    try {
                                        // 每有一个Slave连接进来，都会构建一个HAConnection对象，并持有对应的channel
                                        HAConnection conn = new HAConnection(HAService.this, sc);
                                        conn.start();
                                        HAService.this.addConnection(conn);
                                    } catch (Exception e) {
                                        log.error("new HAConnection exception", e);
                                        sc.close();
                                    }
                                }
                            } else {
                                log.warn("Unexpected ops in select " + k.readyOps());
                            }
                        }

                        selected.clear();
                    }
                } catch (Exception e) {
                    log.error(this.getServiceName() + " service has exception.", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String getServiceName() {
            return AcceptSocketService.class.getSimpleName();
        }
    }

    /**
     * 主要职责：判断主从同步是否完成
     * 判断主从同步是否完成的依据：
     *   所有slave中已经成功复制的最大偏移量是否大于等于消息生产者发送消息后服务端返回下一条消息的起始偏移量，
     *   如果是则表示主从同步复制已经完成，唤醒消息发送线程，否则等待1s，再次判断，每一个任务在一批任务中循环判断5次。
     *   消费者返回有两种情况，如果等待超过5s，或者GroupTransferService通知主从复制完成则返回，可以通过syncFlushTimeout、来设置等待时间。
     *
     * 主从同步阻塞实现，如果是同步主从模式，消息发送者将消息刷写到磁盘后，需要等待新数据被传输到从服务器。
     *
     * HAService在启动的时候会启动GroupTransferService，GroupTransferService在启动后会循环执行
     * doWaitTransfer() 方法，循环检测GroupCommitRequest的执行状态。每存储一条消息生成一个GroupCommitRequest，
     * 加入GroupTransferService。
     * GroupTransferService每10ms对其持有的GroupCommitRequest集合做响应处理。slave每次上报进度唤醒GroupTransferService等待，
     * 当发现上报的进度大于等于当前消息的存储进度时，唤醒GroupCommitRequest的waitForFlush();
     * 当Slave的5次上报进度都小于当前消息的存储时，也唤醒GroupCommitRequest的waitForFlush().
     * 唤醒的同时设置slave是否flush成功。
     * 唤醒成功之后，commitLog就能继续消息的处理流程，将Slave的flush结果返回给消费者
     * GroupTransferService Service
     */
    class GroupTransferService extends ServiceThread {

        private final WaitNotifyObject notifyTransferObject = new WaitNotifyObject();
        private volatile List<CommitLog.GroupCommitRequest> requestsWrite = new ArrayList<>();
        private volatile List<CommitLog.GroupCommitRequest> requestsRead = new ArrayList<>();

        public synchronized void putRequest(final CommitLog.GroupCommitRequest request) {
            synchronized (this.requestsWrite) {
                this.requestsWrite.add(request);
            }
            if (hasNotified.compareAndSet(false, true)) {
                waitPoint.countDown(); // notify
            }
        }

        public void notifyTransferSome() {
            this.notifyTransferObject.wakeup();
        }

        private void swapRequests() {
            List<CommitLog.GroupCommitRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        }

        /**
         * 判断slave中已成功复制的最大偏移量是否大于等于消息生产者发送消息后消息服务端返回吓一跳消息的起始偏移量
         * 如果大于等于那么说明主从同步复制已经完成，唤醒消息发送线程，否则等待1s，再次判断，每个任务在一批任务中循环判断5次。
         */
        private void doWaitTransfer() {
            synchronized (this.requestsRead) {
                if (!this.requestsRead.isEmpty()) {
                    for (CommitLog.GroupCommitRequest req : this.requestsRead) {
                        // 推送到slave的offset是否大于等于当前消息的offset
                        boolean transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                        // 重试5次，每次条件不符合我都等待slave上传同步数据
                        for (int i = 0; !transferOK && i < 5; i++) {
                            this.notifyTransferObject.waitForRunning(1000);
                            transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                        }

                        if (!transferOK) {
                            log.warn("transfer messsage to slave timeout, " + req.getNextOffset());
                        }

                        // 唤醒GroupCommitRequest并标记同步完成
                        req.wakeupCustomer(transferOK);
                    }

                    this.requestsRead.clear();
                }
            }
        }

        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 定时等待之后调用doWaitTransfer()方法
                    this.waitForRunning(10);
                    this.doWaitTransfer();
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }

        @Override
        public String getServiceName() {
            return GroupTransferService.class.getSimpleName();
        }
    }

    /**
     * master与slave之间的数据通信过程：
     *   master启动之后会监听来自slave的连接，slave启动之后会主动连接到master
     *   在连接建立之后，slave会想master上报自己的本地的commitLog的offset
     *   master根据slave的offset来决定从哪里向slave发送数据
     */
    class HAClient extends ServiceThread {
        // socket读缓冲区大小
        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024 * 4;
        // master ip:port 保存master的信息，master也会初始化HAClient，但是masterAddress是空的
        private final AtomicReference<String> masterAddress = new AtomicReference<>();
        // 向master汇报slave的最大offset
        private final ByteBuffer reportOffset = ByteBuffer.allocate(8);
        // slave向master汇报
        private long currentReportedOffset = 0;
        // 拆包粘包使用
        private int dispatchPostion = 0;
        // 读缓冲区，从master接收数据buffer 4M
        private ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
        // 网络传输通道
        private SocketChannel socketChannel;
        // NIO事件选择器
        private Selector selector;
        // 上一次写入时间戳
        private long lastWriteTimestamp = System.currentTimeMillis();

        // 读缓冲区备份，与bufferRead进行交换 4M
        private ByteBuffer byteBufferBackup = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);

        public HAClient() throws IOException {
            this.selector = RemotingUtil.openSelector();
        }

        /**
         * 更新HAMaster地址
         * @param newAddr
         */
        public void updateMasterAddress(final String newAddr) {
            // 获取本地缓存的HAMaster地址
            String currentAddr = this.masterAddress.get();
            // 如果本地缓存的HAMaster地址为空，或者与newAdrr不相等
            if (currentAddr == null || !currentAddr.equals(newAddr)) {
                // 更新本地HAMaster信息
                this.masterAddress.set(newAddr);
                log.info("update master address, OLD: " + currentAddr + " NEW: " + newAddr);
            }
        }

        /**
         * 判断是否已经到可以向master报告当前offset的事件  5s
         * @return
         */
        private boolean isTimeToReportOffset() {
            long interval =
                HAService.this.defaultMessageStore.getSystemClock().now() - this.lastWriteTimestamp;
            boolean needHeart = interval > HAService.this.defaultMessageStore.getMessageStoreConfig()
                .getHaSendHeartbeatInterval();

            return needHeart;
        }

        /**
         * 向master上报当前最大offset并作为心跳数据
         * @param maxOffset
         * @return
         */
        private boolean reportSlaveMaxOffset(final long maxOffset) {
            this.reportOffset.position(0);
            this.reportOffset.limit(8);
            this.reportOffset.putLong(maxOffset);
            // 将reportOffset从写状态变为读状态 也可以直接使用reportOffset.flip()
            this.reportOffset.position(0);
            this.reportOffset.limit(8);

            // 循环写，将reportOffset数据写入channel，hasRemaining返回false，说明已经全部写完
            for (int i = 0; i < 3 && this.reportOffset.hasRemaining(); i++) {
                try {
                    this.socketChannel.write(this.reportOffset);
                } catch (IOException e) {
                    log.error(this.getServiceName()
                        + "reportSlaveMaxOffset this.socketChannel.write exception", e);
                    return false;
                }
            }

            return !this.reportOffset.hasRemaining();
        }

        /**
         * 扩容算法。实际并没有扩大byteBufferRead的大小
         * readBuffer不够的时候重新申请buffer(实际并没有重新申请，只是将尚未读取的部分放在准备好backup的buffer中，然后将backup赋值给readBuffer)
         */
        private void reallocateByteBuffer() {
            // 计算byteBufferRead尚未处理部分的size
            int remain = READ_MAX_BUFFER_SIZE - this.dispatchPostion;
            // 如果byteBufferReadRead中还有未处理的字节
            if (remain > 0) {
                this.byteBufferRead.position(this.dispatchPostion);

                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);
                this.byteBufferBackup.put(this.byteBufferRead);
            }

            this.swapByteBuffer();
            // 设置下次写入的位置为之前byteBufferRead尚未处理部分的后面
            this.byteBufferRead.position(remain);
            this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
            // 从0开始处理byteBufferRead中的数据
            this.dispatchPostion = 0;
        }

        private void swapByteBuffer() {
            ByteBuffer tmp = this.byteBufferRead;
            this.byteBufferRead = this.byteBufferBackup;
            this.byteBufferBackup = tmp;
        }

        /**
         * slave处理master发来的数据
         * master发送过来的数据格式为 header（offset(8个byte) + bodySize(4个byte) + body）
         *   offset：由于master发送给slave的commitLog的单位是MappedFile的个数，这个offset是mappedFile的起始位置
         *   bodySize：MappedFile的大小
         *   body：MappedFile的内容
         *
         *  Slave的处理逻辑主要是：
         *    1.slave将接收到的数据都存在byteBufferRead
         *    2.判断收到的数据是否完整，如果byteBufferRead待处理的数据大于headSize则认为可以则认为可以开始处理
         *    3.判断收到的数据的offset是否和slave当前offset一致(也就是判断是否是slave需要的下一个MappedFile)，如果不一致说明系统错误
         *    4.按照数据协议从byteBufferRead依次读取出offset、size、body
         *    5.将body(MappedFile)写入slave的commitLog
         *    6.更新byteBufferRead里面的处理进度(当前已处理的字节数)
         *    7.如果上面判断出收到的数据尚不足以处理，需要继续接收数据之前先对byteBufferRead进行扩容
         * @return
         */
        private boolean processReadEvent() {
            int readSizeZeroTimes = 0;
            // hasRemaining返回true，输入buffer中还有数据
            while (this.byteBufferRead.hasRemaining()) {
                try {
                    int readSize = this.socketChannel.read(this.byteBufferRead);
                    if (readSize > 0) {
                        // 更新最后一次写入时间戳
                        lastWriteTimestamp = HAService.this.defaultMessageStore.getSystemClock().now();
                        // 重置连续读取到0字节的次数
                        readSizeZeroTimes = 0;
                        // 转发请求
                        boolean result = this.dispatchReadRequest();
                        if (!result) {
                            log.error("HAClient, dispatchReadRequest error");
                            return false;
                        }
                    } else if (readSize == 0) {
                        // 如果连续三次读到0字节，返回false
                        if (++readSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        log.info("HAClient, processReadEvent read socket < 0");
                        return false;
                    }
                } catch (IOException e) {
                    log.info("HAClient, processReadEvent read socket exception", e);
                    return false;
                }
            }

            return true;
        }

        /**
         * master发送过来的数据格式为 header（offset(8个byte) + bodySize(4个byte) + body）
         * 读取Master传输的commitLog数据，并返回是否OK
         * 如果读取到数据，写入CommitLog
         * 异常原因：
         *   1.Master传输的数据开始位置offset(masterPhyOffset)不等于Slave的commitLog数据最大offset(slavePhyOffset)
         *   2.上报到Master进度失败
         * @return
         */
        private boolean dispatchReadRequest() {
            final int msgHeaderSize = 8 + 4; // phyoffset + size
            int readSocketPos = this.byteBufferRead.position();

            while (true) {
                // begin ->读取到请求数据 当前从master读取到的数据的总大小 - 上一次处理(写入commitLog)到的位置
                int diff = this.byteBufferRead.position() - this.dispatchPostion;
                // 确定收到的数据是完整的
                if (diff >= msgHeaderSize) {
                    // 读取masterPhyOffset、bodySize
                    long masterPhyOffset = this.byteBufferRead.getLong(this.dispatchPostion);  // getLong() --> offset为Long类型占用8个byte
                    int bodySize = this.byteBufferRead.getInt(this.dispatchPostion + 8); // master发送过来的数据格式为 header（offset(8个byte) + bodySize(4个byte)） + body 所以 bodySize 为8位后的4个byte
                    // 获取slave节点上commitLog文件最大的offset位置
                    long slavePhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();

                    if (slavePhyOffset != 0) {
                        // 校验master传输来的数据开始位置offset和slave的commitLog数据最大的offset是否相同
                        // 保证本次消息同步slave的commitLog起始位置和master这个mappedFile的起始位置相同
                        if (slavePhyOffset != masterPhyOffset) {
                            // 数据中断了。slave的最大offset匹配不上推送过来的offset
                            log.error("master pushed offset not equal the max phy offset in slave, SLAVE: "
                                + slavePhyOffset + " MASTER: " + masterPhyOffset);
                            return false;
                        }
                    }

                    /**
                     * 读取到消息
                     * 拆包粘包处理
                     * 如果diff小于msgHeaderSize + bodySize，那么代表发生了拆包。等待下一次读取数据
                     */
                    if (diff >= (msgHeaderSize + bodySize)) {
                        // 写入到commitLog
                        byte[] bodyData = new byte[bodySize];
                        this.byteBufferRead.position(this.dispatchPostion + msgHeaderSize);
                        this.byteBufferRead.get(bodyData);
                        // 将收到的MappedFile写入slave的commitLog
                        HAService.this.defaultMessageStore.appendToCommitLog(masterPhyOffset, bodyData);
                        // 设置处理到的位置
                        this.byteBufferRead.position(readSocketPos);
                        // 记录本次(上次)读取消息的位置
                        this.dispatchPostion += msgHeaderSize + bodySize;
                        // 上报master进度
                        if (!reportSlaveMaxOffsetPlus()) {
                            return false;
                        }
                        // 继续读取数据
                        continue;
                    }
                }
                // 到这里说明当前收到的消息不完整，需要使用byteBufferRead继续接收，所以保证byteBufferRead的空间是足够接收的
                if (!this.byteBufferRead.hasRemaining()) {
                    this.reallocateByteBuffer();
                }

                break;
            }

            return true;
        }

        private boolean reportSlaveMaxOffsetPlus() {
            boolean result = true;
            // 只要本地有更新，就汇报最大物理offset
            long currentPhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();
            if (currentPhyOffset > this.currentReportedOffset) {
                this.currentReportedOffset = currentPhyOffset;
                result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                if (!result) {
                    this.closeMaster();
                    log.error("HAClient, reportSlaveMaxOffset error, " + this.currentReportedOffset);
                }
            }

            return result;
        }

        /**
         * 尝试连接master
         * @return 是否成功连接master
         * @throws ClosedChannelException
         */
        private boolean connectMaster() throws ClosedChannelException {
            // socketChannel为空，尝试连接master
            if (null == socketChannel) {
                String addr = this.masterAddress.get();
                /**
                 * master 地址为空，返回false
                 * 如果master地址不为空，则建立到master的tcp连接，然后注册OP_READ读事件，并初始化currentReportedOffset为
                 * commitLog文件的最大偏移量，lastWriteTimestamp上次写入时间戳为当前时间戳，并返回true。
                 * 在broker启动时，如果其角色为slave，将读取broker配置文件中haMasterAddress属性更新HAClient的masterAddress，
                 * 如果角色是slave，但是haMasterAddress为空，启动不会报错，但不会执行主从复制
                 */
                if (addr != null) {

                    SocketAddress socketAddress = RemotingUtil.string2SocketAddress(addr);
                    if (socketAddress != null) {
                        this.socketChannel = RemotingUtil.connect(socketAddress);
                        if (this.socketChannel != null) {
                            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
                        }
                    }
                }

                // 每次连接时，会重新拿到最大的offset，以便后续对master报告自己的存储位置
                this.currentReportedOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();

                this.lastWriteTimestamp = System.currentTimeMillis();
            }

            return this.socketChannel != null;
        }

        private void closeMaster() {
            if (null != this.socketChannel) {
                try {

                    SelectionKey sk = this.socketChannel.keyFor(this.selector);
                    if (sk != null) {
                        sk.cancel();
                    }

                    this.socketChannel.close();

                    this.socketChannel = null;
                } catch (IOException e) {
                    log.warn("closeMaster exception. ", e);
                }

                this.lastWriteTimestamp = 0;
                this.dispatchPostion = 0;

                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);

                this.byteBufferRead.position(0);
                this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
            }
        }

        /**
         * Client(Slave)主循环，实现了不断从Master传输commitLog数据，上传Master自己本地的commitLog已经同步物理位置
         * ASYNC_MASTER同步数据到slave
         *   1.slave连接到master，向master上报slave当前的offset
         *   2.master收到后确认给slave发送数据的开始位置
         *   3.master查询开始位置对应的MappedFile
         *   4.master将查找到的数据发送给slave
         *   5.slave收到数据后保存到自己的commitLog
         */
        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    /**
                     * 只有配置了HAMaster地址的broker才会连接到master
                     * 如果masterAddress不为空，会进行连接并初始化SocketChannel，Master没有masterAddress，所以这里直接跳过
                     */
                    if (this.connectMaster()) {

                        // 汇报最大物理offset，定时心跳方式汇报
                        if (this.isTimeToReportOffset()) {
                            // 向master上报slave本地最大的commitLog的offset
                            boolean result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                            // 如果汇报失败，关闭连接
                            if (!result) {
                                this.closeMaster();
                            }
                        }

                        // 阻塞等待事件触发
                        this.selector.select(1000);
                        // 处理socket上的read事件，也就是处理master发来的数据
                        boolean ok = this.processReadEvent();
                        if (!ok) {
                            this.closeMaster();
                        }

                        // 查看本地是否有更新，如果有，汇报新的最大物理offset
                        if (!reportSlaveMaxOffsetPlus()) {
                            continue;
                        }

                        // 检查master的反向心跳
                        long interval =
                            HAService.this.getDefaultMessageStore().getSystemClock().now()
                                - this.lastWriteTimestamp;
                        if (interval > HAService.this.getDefaultMessageStore().getMessageStoreConfig()
                            .getHaHousekeepingInterval()) {
                            log.warn("HAClient, housekeeping, found this connection[" + this.masterAddress
                                + "] expired, " + interval);
                            this.closeMaster();
                            log.warn("HAClient, master not response some time, so close connection");
                        }
                    } else {
                        this.waitForRunning(1000 * 5);
                    }
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                    this.waitForRunning(1000 * 5);
                }
            }

            log.info(this.getServiceName() + " service end");
        }
        // private void disableWriteFlag() {
        // if (this.socketChannel != null) {
        // SelectionKey sk = this.socketChannel.keyFor(this.selector);
        // if (sk != null) {
        // int ops = sk.interestOps();
        // ops &= ~SelectionKey.OP_WRITE;
        // sk.interestOps(ops);
        // }
        // }
        // }
        // private void enableWriteFlag() {
        // if (this.socketChannel != null) {
        // SelectionKey sk = this.socketChannel.keyFor(this.selector);
        // if (sk != null) {
        // int ops = sk.interestOps();
        // ops |= SelectionKey.OP_WRITE;
        // sk.interestOps(ops);
        // }
        // }
        // }

        @Override
        public String getServiceName() {
            return HAClient.class.getSimpleName();
        }
    }
}
