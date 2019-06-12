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
import org.apache.rocketmq.store.SelectMappedBufferResult;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

/**
 * Master-Slave网络连接对象
 * 每个slave都会对应一个HAConnection实例，用来与slave交互
 */
public class HAConnection {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    // 关联的HAService实现类
    private final HAService haService;
    // 网络通道
    private final SocketChannel socketChannel;
    // 客户端地址
    private final String clientAddr;
    // 向slave推送消息
    private WriteSocketService writeSocketService;
    // 读取slave发回的同步消息
    private ReadSocketService readSocketService;

    // 从服务器请求拉取数据的偏移量
    private volatile long slaveRequestOffset = -1;
    // 从服务器反馈已拉取完成的数据偏移量
    private volatile long slaveAckOffset = -1;

    public HAConnection(final HAService haService, final SocketChannel socketChannel) throws IOException {
        this.haService = haService;
        this.socketChannel = socketChannel;
        this.clientAddr = this.socketChannel.socket().getRemoteSocketAddress().toString();
        this.socketChannel.configureBlocking(false);
        this.socketChannel.socket().setSoLinger(false, -1);
        this.socketChannel.socket().setTcpNoDelay(true);
        this.socketChannel.socket().setReceiveBufferSize(1024 * 64);
        this.socketChannel.socket().setSendBufferSize(1024 * 64);
        this.writeSocketService = new WriteSocketService(this.socketChannel);
        this.readSocketService = new ReadSocketService(this.socketChannel);
        this.haService.getConnectionCount().incrementAndGet();
    }

    public void start() {
        this.readSocketService.start();
        this.writeSocketService.start();
    }

    public void shutdown() {
        this.writeSocketService.shutdown(true);
        this.readSocketService.shutdown(true);
        this.close();
    }

    public void close() {
        if (this.socketChannel != null) {
            try {
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }
        }
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    /**
     * 主要负责处理slave上传的进度及其他相关操作，核心run方法
     * 通过HAConnection启动ReadSocketService一直监听来自slave的汇报最大偏移量的消息。每一个HAConnection对应
     * 一个来自Slave的请求，也就是说么一个HAConnection保存一个slave的最大偏移量
     */
    class ReadSocketService extends ServiceThread {
        // 默认缓冲区大小 1M
        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024;
        // 事件选择器
        private final Selector selector;
        // 网络通道
        private final SocketChannel socketChannel;
        private final ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
        // 当前已处理数据的指针
        private int processPostion = 0;
        // 上次读取数据的时间戳
        private volatile long lastReadTimestamp = System.currentTimeMillis();

        /**
         * socketChannel即之前serverSocketChannel accept到的，这里开启一个selector并将socketChannel注册其中，并且监听OP_READ事件
         * @param socketChannel
         * @throws IOException
         */
        public ReadSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = RemotingUtil.openSelector();
            this.socketChannel = socketChannel;
            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
            this.thread.setDaemon(true);
        }

        /**
         * master处理slave发送来的数据
         */
        @Override
        public void run() {
            HAConnection.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.selector.select(1000);
                    // 读取slave发送的数据
                    boolean ok = this.processReadEvent();
                    if (!ok) {
                        HAConnection.log.error("processReadEvent error");
                        break;
                    }

                    // 检测心跳间隔时间，超过规则强制断开
                    long interval = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now() - this.lastReadTimestamp;
                    if (interval > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaHousekeepingInterval()) {
                        log.warn("ha housekeeping, found this connection[" + HAConnection.this.clientAddr + "] expired, " + interval);
                        break;
                    }
                } catch (Exception e) {
                    HAConnection.log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }

            this.makeStop();

            writeSocketService.makeStop();

            // 避免内存泄漏
            haService.removeConnection(HAConnection.this);

            HAConnection.this.haService.getConnectionCount().decrementAndGet();

            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                this.selector.close();
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }

            HAConnection.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return ReadSocketService.class.getSimpleName();
        }

        /**
         * slave发送给master的数据格式
         *   offset(8个字节)：slave本地到的commitLog的maxOffset
         * master收到slave上报的offset后进行处理
         * @return
         */
        private boolean processReadEvent() {
            int readSizeZeroTimes = 0;

            if (!this.byteBufferRead.hasRemaining()) {
                this.byteBufferRead.flip();
                this.processPostion = 0;
            }

            while (this.byteBufferRead.hasRemaining()) {
                try {
                    int readSize = this.socketChannel.read(this.byteBufferRead);
                    if (readSize > 0) {
                        readSizeZeroTimes = 0;
                        this.lastReadTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                        /**
                         * 接收slave上传的offset
                         * 检查拆包粘包
                         * 8是offset的字节数，processPosition为上次处理的位置，如果出现拆包，那么传输的小于8字节，不处理，等待下一次读取
                         */
                        if ((this.byteBufferRead.position() - this.processPostion) >= 8) {
                            /**
                             * eg：
                             * 第一次获取到数据，this.byteBufferRead.position - 0 = 3,忽略
                             * 第二次获取到数据，this,byteBufferRead.position为10，那么pos = 10 - 2 = 8, readOffset = getLong(0),刚好读取到第一个long数据，processPosition设置为8
                             * 第三次获取到数据，this.byteBufferRead.position - 8 > 8，this.byteBufferRead.position为21，那么pos = 21 - 5 = 16,readOffset = getLong(8),刚好读到第二个long数据
                             */
                            int pos = this.byteBufferRead.position() - (this.byteBufferRead.position() % 8);
                            long readOffset = this.byteBufferRead.getLong(pos - 8);
                            this.processPostion = pos;
                            /**
                             * 更新slaveAckOffset和slaveRequestOffset
                             * slaveAckOffset是每次slave上传的offset
                             * slaveRequestOffset是第一次slave上传的offset
                             */
                            // 设置slave commitLog的最大位置
                            HAConnection.this.slaveAckOffset = readOffset;
                            if (HAConnection.this.slaveRequestOffset < 0) {
                                // 如果是刚刚和slave建立连接，需要知道slave需要从哪里开始接收commitLog
                                HAConnection.this.slaveRequestOffset = readOffset;
                                log.info("slave[" + HAConnection.this.clientAddr + "] request offset " + readOffset);
                            }

                            // 如果收到来自slave的确认之后，唤醒等待同步到Slave的线程(如果是SYNC_MASTER) 通知目前slave进度，主要用于master节点为同步类型的
                            HAConnection.this.haService.notifyTransferSome(HAConnection.this.slaveAckOffset);
                        }
                    } else if (readSize == 0) {
                        // 如果连续3次读取到0字节，结束本次读请求处理
                        if (++readSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        log.error("read socket[" + HAConnection.this.clientAddr + "] < 0");
                        return false;
                    }
                } catch (IOException e) {
                    log.error("processReadEvent exception", e);
                    return false;
                }
            }

            return true;
        }
    }

    /**
     * master前面接收到slave报告的最大偏移量之后，将会在这里将数据传送回slave
     */
    class WriteSocketService extends ServiceThread {
        // 事件选择器
        private final Selector selector;
        // 网络通道
        private final SocketChannel socketChannel;

        // 消息头长度
        private final int headerSize = 8 + 4;
        private final ByteBuffer byteBufferHeader = ByteBuffer.allocate(headerSize);
        // 记录着从commitLog哪个offset拉取消息，即下一次传输的物理偏移量
        private long nextTransferFromWhere = -1;
        // 拉取消息后的结果，根据偏移量查找消息的结果
        private SelectMappedBufferResult selectMappedBufferResult;
        // 标记上一次数据传输是否传输完成
        private boolean lastWriteOver = true;

        // 上次写入时间戳
        private long lastWriteTimestamp = System.currentTimeMillis();

        /**
         * 在ReadSocketService中接收到slave汇报的最大offset，master会通过WriteSocketService将同步的数据传送回slave。
         * 将AcceptSocketService接收到SocketChannel绑定在自己的selector上，并绑定OP_WRITE事件
         * @param socketChannel
         * @throws IOException
         */
        public WriteSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = RemotingUtil.openSelector();
            this.socketChannel = socketChannel;
            this.socketChannel.register(this.selector, SelectionKey.OP_WRITE);
            this.thread.setDaemon(true);
        }

        /**
         * 先判断slaveRequestOffset是否等于-1，若是则说明Master还没有收到从服务器的拉取请求，放弃本次事件处理，等待并继续select轮询。
         * 如果nextTransferFromWhere为-1表示初次进行数据传输，需要计算需要传输的物理偏移量。
         * 接下来判断HAConnection是否存有数据，即slaveRequestOffset是否为0.如果为0则从最后一个物理文件的起始位置开始传输，
         * 否则根据从服务器的拉取请求偏移量开始传输。
         *
         * 接下来判断上次写事件是否把信息全部写入，如果是，那么判断当前的master是否长时间没有向slave写数据，如果超过一定的事件，
         * 那么需要发送一个心跳数据包(12字节大小，8偏移量+4size，默认size为0)，如果上次信息未写完那么继续将未传输的数据先写入。
         */
        @Override
        public void run() {
            HAConnection.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.selector.select(1000);
                    // 说明还没收到来自slave的offset，等待10ms重试
                    if (-1 == HAConnection.this.slaveRequestOffset) {
                        Thread.sleep(10);
                        continue;
                    }
                    // 如果是第一次发送数据需要计算从哪里开始给slave发送数据
                    if (-1 == this.nextTransferFromWhere) {
                        // slave如果本地没有数据，请求的offset为0，那么master则从物理文件最后一个文件开始传送数据
                        if (0 == HAConnection.this.slaveRequestOffset) {
                            long masterOffset = HAConnection.this.haService.getDefaultMessageStore().getCommitLog().getMaxOffset();
                            masterOffset =
                                masterOffset
                                    - (masterOffset % HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig()
                                    .getMapedFileSizeCommitLog());

                            if (masterOffset < 0) {
                                masterOffset = 0;
                            }

                            this.nextTransferFromWhere = masterOffset;
                        } else {
                            this.nextTransferFromWhere = HAConnection.this.slaveRequestOffset;
                        }

                        log.info("master transfer data from " + this.nextTransferFromWhere + " to slave[" + HAConnection.this.clientAddr
                            + "], and slave request " + HAConnection.this.slaveRequestOffset);
                    }
                    // 传输完成。如果上一次transfer完成了才进行下一次transfer
                    if (this.lastWriteOver) {
                        /**
                         * 如果已经全部写入，判断当前系统与上次最后写入的时间间隔是否大于HA心跳检测时间，
                         * 大于则发送一个心跳包，心跳包的长度为12个字节(从服务器已经拉取到的偏移量 + size)，
                         * 消息长度默认存0，表示本次数据包为心跳包，避免长连接由于空闲被关闭
                         */
                        long interval =
                            HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now() - this.lastWriteTimestamp;
                        if (interval > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig()
                            .getHaSendHeartbeatInterval()) {

                            // Build Header
                            this.byteBufferHeader.position(0);
                            this.byteBufferHeader.limit(headerSize);
                            this.byteBufferHeader.putLong(this.nextTransferFromWhere);
                            this.byteBufferHeader.putInt(0);
                            this.byteBufferHeader.flip();

                            this.lastWriteOver = this.transferData();
                            if (!this.lastWriteOver)
                                continue;
                        }
                    }
                    /**
                     * 继续传输
                     * 如果上次数据传输未完成，则继续传输上次的数据，然后再次判断是否传输完成，如果消息还未全部传输完成，
                     * 则结束此次事件处理，等待下次写事件到达后，继续将未传输完的数据先写入到从服务器
                     */
                    else {
                        // 说明上一次的数据还没有传输完成，这里继续上一次的传输
                        this.lastWriteOver = this.transferData();
                        if (!this.lastWriteOver)
                            continue;
                    }

                    // 传入一个offset，从CommitLog去拉取消息，和消费者拉取消息类似
                    SelectMappedBufferResult selectResult =
                        HAConnection.this.haService.getDefaultMessageStore().getCommitLogData(this.nextTransferFromWhere);
                    if (selectResult != null) {
                        // 发送找到的MappedFile数据，默认每次只同步32K数据
                        int size = selectResult.getSize();
                        if (size > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize()) {
                            size = HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize();
                        }

                        long thisOffset = this.nextTransferFromWhere;
                        // 计算下次需要给slave发送数据的起始位置
                        this.nextTransferFromWhere += size;

                        /**
                         * 如果找到的mappedFile大小超过了一次同步任务最大传输的字节数，则通过设置ByteBuffer的limit来
                         * 设置只传输指定长度的字节，这也就意味着HA客户端收的消息包含了不完整的信息
                         */
                        selectResult.getByteBuffer().limit(size);
                        this.selectMappedBufferResult = selectResult;

                        // Build Header
                        this.byteBufferHeader.position(0);
                        this.byteBufferHeader.limit(headerSize);
                        this.byteBufferHeader.putLong(thisOffset);
                        this.byteBufferHeader.putInt(size);
                        this.byteBufferHeader.flip();
                        // 向slave发送数据
                        this.lastWriteOver = this.transferData();
                    } else {
                        // 如果没有需要给slave发送的数据，传输数据的线程等待100ms 或者 等待broker接收到新发来的消息的时候唤醒这个线程
                        HAConnection.this.haService.getWaitNotifyObject().allWaitForRunning(100);
                    }
                } catch (Exception e) {
                    // 只要抛出异常，一般是网络发生错误，连接心跳必须断开，并清理资源
                    HAConnection.log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }

            // 清理资源
            if (this.selectMappedBufferResult != null) {
                this.selectMappedBufferResult.release();
            }

            this.makeStop();

            readSocketService.makeStop();

            // 避免内存泄漏
            haService.removeConnection(HAConnection.this);

            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                this.selector.close();
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }

            HAConnection.log.info(this.getServiceName() + " service end");
        }

        /**
         * 发送数据给slave 格式 : header(offset(8byte) + bodySize(4byte)) + body
         * @return
         * @throws Exception
         */
        private boolean transferData() throws Exception {
            int writeSizeZeroTimes = 0;
            // Write Header
            // 前面已经将需要发送的header数据放入ByteBufferHeader
            while (this.byteBufferHeader.hasRemaining()) {
                int writeSize = this.socketChannel.write(this.byteBufferHeader);
                if (writeSize > 0) {
                    writeSizeZeroTimes = 0;
                    this.lastWriteTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                } else if (writeSize == 0) {
                    if (++writeSizeZeroTimes >= 3) {
                        break;
                    }
                } else {
                    throw new Exception("ha master write header error < 0");
                }
            }

            if (null == this.selectMappedBufferResult) {
                return !this.byteBufferHeader.hasRemaining();
            }

            writeSizeZeroTimes = 0;

            // Write Body
            if (!this.byteBufferHeader.hasRemaining()) {
                // selectMappedBufferResult里面存放的是需要发送的MappedFile数据
                while (this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
                    // 写入socketChannel
                    int writeSize = this.socketChannel.write(this.selectMappedBufferResult.getByteBuffer());
                    if (writeSize > 0) {
                        writeSizeZeroTimes = 0;
                        this.lastWriteTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                    } else if (writeSize == 0) {
                        if (++writeSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        throw new Exception("ha master write body error < 0");
                    }
                }
            }

            boolean result = !this.byteBufferHeader.hasRemaining() && !this.selectMappedBufferResult.getByteBuffer().hasRemaining();

            if (!this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
                // 每次发送完数据之后清空selectMappedBufferResult，保证下一次发送前selectMappedBufferResult=null
                this.selectMappedBufferResult.release();
                this.selectMappedBufferResult = null;
            }

            return result;
            /**
             * 到这里，master已经通过网络将MappedFile数据发送给了slave，接下来就是slave收到master的数据，然后保存到自己的commitLog。
             * HAClient启动的时候向和master连接的socket上注册了read事件的selector，收到read事件之后，依次执行以下方法
             * org.apache.rocketmq.store.ha.HAService.HAClient#processReadEvent()
             * org.apache.rocketmq.store.ha.HAService.HAClient#dispatchReadRequest()
             */
        }

        @Override
        public String getServiceName() {
            return WriteSocketService.class.getSimpleName();
        }

        @Override
        public void shutdown() {
            super.shutdown();
        }
    }
}
