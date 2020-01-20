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

import org.apache.rocketmq.common.*;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.running.RunningStats;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.ha.HAService;
import org.apache.rocketmq.store.index.IndexService;
import org.apache.rocketmq.store.index.QueryOffsetResult;
import org.apache.rocketmq.store.schedule.ScheduleMessageService;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.rocketmq.store.config.BrokerRole.SLAVE;

/**
 * Broker将消息存储抽象成MessageStore，DefaultMessageStore是默认实现
 * 主要提供的功能：
 * 1.保存消息，包括单条和批量保存
 * 2.根据topic、queue、offset批量获取消息，consumer使用该方法拉取消息
 * 3.根据消息offset读取消息详情，根据messageId查询消息时使用该方法
 * 4.根据messageKey查询消息，可提供给终端用户使用
 * <p>
 * MessageQueue数据结构图
 * messageStore数据结构图：https://github.com/kid3028/imageRepository/blob/master/rocketmq/commitlog/messageStore%E6%95%B0%E6%8D%AE%E7%BB%93%E6%9E%84.png
 * CommitLog:存储消息的详细信息，按照消息的收到的顺序，所有消息都存储在一起。每个消息存储后都会有一个offset，代表在commitLog中偏移量。
 * 例如，当前commitLog文件的大小时12413435字节，那下一条消息到来后它的offset就是12413436。这个说法不是非常准确，但是offset大概就是这么计算来的。
 * commitLog并不是一个文件，而是一系列的文件(图中的MappedFile)。每个文件的大小都是固定的(默认时1G)，写满一个会生成一个新的文件，新文件的文件名就是他存储的第一条消息的offset。
 * <p>
 * ConsumeQueue:既然所有的消息都是存储在一个commitLog中，但是consumer时按照topic + queue的维度来消费消息的，没有办法直接直接从commitLog中读取，所以针对每个topic的每个queue都会
 * 生成一个consumequeue文件。ConsumeQueue文件中存储的是消息在commitLog中的offset，可以理解为一个按Queue建立的索引，每条消息(图中cq)占用20字节(offset 8bytes + msgsize 4bytes + tagcode 8bytes)。
 * 跟commitLog一样，每个Queue文件也是一系列连续的文件组成，每个文件默认存储30w个offset。
 * <p>
 * IndexFile: CommitLog的另外一种形式的索引文件，只是索引的是MessageKey，每个MsgKey经过hash后计算存储的slot，然后将offset存储到indexFile的相应的slot上。根据msgKey来查询消息时，可以先到indexFile中查询offset，然后根据offset去commitLog中查询消息详情。
 * <p>
 * 线程服务
 * 线程服务：对数据做操作的相应服务。
 * IndexService：负责读写IndexFile的服务
 * ReputMessageService：消息存储到commitLog后，MessageStore的接口调用就直接返回了，后续由ReputMessageService负责将消息分发到ConsumeQueue和IndexService
 * HAService：负责将master-slave之间的消息数据同步
 */
public class  DefaultMessageStore implements MessageStore {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    // 存储相关配置，例如存储路径，commitLog文件大小，刷盘频率
    private final MessageStoreConfig messageStoreConfig;
    // CommitLog 消息详情存储，同一个Broker上的所有消息都保存在一起，每条消息保存后都会有一个offset
    private final CommitLog commitLog;

    // topic消费队列 消息队列存储缓存表，按照消息主题分组
    private final ConcurrentMap<String/* topic */, ConcurrentMap<Integer/* queueId */, ConsumeQueue>> consumeQueueTable;

    // consumeQueue刷盘服务线程
    private final FlushConsumeQueueService flushConsumeQueueService;

    // 定时清除commitLog服务
    private final CleanCommitLogService cleanCommitLogService;

    // 定时清除consumeQueue服务
    private final CleanConsumeQueueService cleanConsumeQueueService;

    // 索引服务
    private final IndexService indexService;

    // MappedFile分配线程，RocketMQ使用内存映射处理commitLog，ConsumeQueue文件
    private final AllocateMappedFileService allocateMappedFileService;

    // commitLog消息分发，根据commitLog文件创建consumeQueue、IndexFile文件
    private final ReputMessageService reputMessageService;

    // 主从同步实现服务
    private final HAService haService;

    // 定时任务调度器，执行定时任务
    private final ScheduleMessageService scheduleMessageService;

    // 存储统计服务
    private final StoreStatsService storeStatsService;

    // DataBuffer池 消息堆内存缓存
    private final TransientStorePool transientStorePool;

    // 存储服务状态
    private final RunningFlags runningFlags = new RunningFlags();
    private final SystemClock systemClock = new SystemClock();

    private final ScheduledExecutorService scheduledExecutorService =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("StoreScheduledThread"));
    // broker统计服务
    private final BrokerStatsManager brokerStatsManager;
    // 消息到达监听器 消息拉取长轮询模式消息到达监听器
    private final MessageArrivingListener messageArrivingListener;
    // broker配置属性
    private final BrokerConfig brokerConfig;

    private volatile boolean shutdown = true;

    // 文件刷盘检查点
    private StoreCheckpoint storeCheckpoint;

    private AtomicLong printTimes = new AtomicLong(0);

    // 转发commitLog日志，主要从commitLog转发到consumeQueue，commitLog index
    private final LinkedList<CommitLogDispatcher> dispatcherList;

    private RandomAccessFile lockFile;

    private FileLock lock;

    boolean shutDownNormal = false;

    public DefaultMessageStore(final MessageStoreConfig messageStoreConfig, final BrokerStatsManager brokerStatsManager,
                               final MessageArrivingListener messageArrivingListener, final BrokerConfig brokerConfig) throws IOException {
        this.messageArrivingListener = messageArrivingListener;
        this.brokerConfig = brokerConfig;
        this.messageStoreConfig = messageStoreConfig;
        this.brokerStatsManager = brokerStatsManager;
        this.allocateMappedFileService = new AllocateMappedFileService(this);
        this.commitLog = new CommitLog(this);
        this.consumeQueueTable = new ConcurrentHashMap<>(32);

        this.flushConsumeQueueService = new FlushConsumeQueueService();
        this.cleanCommitLogService = new CleanCommitLogService();
        this.cleanConsumeQueueService = new CleanConsumeQueueService();
        this.storeStatsService = new StoreStatsService();
        this.indexService = new IndexService(this);
        this.haService = new HAService(this);

        this.reputMessageService = new ReputMessageService();

        this.scheduleMessageService = new ScheduleMessageService(this);

        this.transientStorePool = new TransientStorePool(messageStoreConfig);

        if (messageStoreConfig.isTransientStorePoolEnable()) {
            this.transientStorePool.init();
        }

        this.allocateMappedFileService.start();

        this.indexService.start();

        this.dispatcherList = new LinkedList<>();
        // 构建consumequeuue
        this.dispatcherList.addLast(new CommitLogDispatcherBuildConsumeQueue());
        // 构建index
        this.dispatcherList.addLast(new CommitLogDispatcherBuildIndex());

        File file = new File(StorePathConfigHelper.getLockFile(messageStoreConfig.getStorePathRootDir()));
        MappedFile.ensureDirOK(file.getParent());
        lockFile = new RandomAccessFile(file, "rw");
    }

    public void truncateDirtyLogicFiles(long phyOffset) {
        ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> tables = DefaultMessageStore.this.consumeQueueTable;

        for (ConcurrentMap<Integer, ConsumeQueue> maps : tables.values()) {
            for (ConsumeQueue logic : maps.values()) {
                logic.truncateDirtyLogicFiles(phyOffset);
            }
        }
    }

    /**
     * 加载相关文件，执行恢复
     * 1.首先加载相关文件到内存(内存映射文件)
     * 包括CommitLog文件、ConsumeQueue文件，存储检测点(checkpoint文件)文件、索引文件
     * 2.执行文件恢复
     * 引入临时文件abort来区分是否是异常启动。在存储管理启动时创建abort文件，结束(shutdown)时会删除abort文件，
     * 也就是如果在启动的时候，发送有该临时文件，则认为上次是异常关闭。
     * 3.恢复顺序
     * 3.1.先恢复consumequeue文件，把不符合的consumqueue文件删除，一个consume条目正确的标准是(commitLog offset > 0, size > 0),从倒数第三个文件开始恢复
     * 3.2.如果abort文件存在，此时找到第一个正常的commitLog文件，然后对该文件重新进行转发，依次更新consumequeue，index文件
     *
     * @throws IOException
     */
    public boolean load() {
        boolean result = true;

        try {
            // 判断${ROCKETMQ_HOME}/store/abort文件是否存在，用于判断进程是否正常退出
            // Broker在启动时创建${ROCKETMQ_HOME}/store/abort文件，在退出时通过注册JVM钩子函数删除abort文件。
            // 如果下一次启动时存在abort文件，说明broker是异常退出的。commitlog和consumequeue数据有可能不一致，需要进行修复
            boolean lastExitOK = !this.isTempFileExist();
            log.info("last shutdown {}", lastExitOK ? "normally" : "abnormally");

            if (null != scheduleMessageService) {
                /**
                 * 加载文件${ROCKETMQ_HOME}/store/config/delayOffset.json
                 * 将默认的延迟时间"1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h"转化为对应的毫秒值
                 */
                result = result && this.scheduleMessageService.load();
            }

            // load Commit Log
            // 加载${ROCKETMQ_HOME}/store/commitlog目录下的所有文件，并标记每一个文件的写位置、刷新位置、提交位置(也就是commit log文件的大小，默认1G)
            result = result && this.commitLog.load();

            // load Consume Queue
            /**
             * 加载${ROCKETMQ_HOME}/store/consumequeue目录下的所有文件
             * 获取文件之后构建ConsumeQueue，并设置topic、queueId、ConsumeQueue三者之间的关系
             * 同样标记每一个文件的写位置、刷新位置和提交位置
             */
            result = result && this.loadConsumeQueue();

            if (result) {
                // 加载${ROCKETMQ_HOME}/store/checkpoint文件，主要用于确定物理消息、逻辑消息、索引消息三个时间戳
                this.storeCheckpoint =
                        new StoreCheckpoint(StorePathConfigHelper.getStoreCheckpoint(this.messageStoreConfig.getStorePathRootDir()));
                // 加载${ROCKETMQ_HOME}/store/index目录下的索引文件
                this.indexService.load(lastExitOK);
                // 文件检测恢复
                this.recover(lastExitOK);

                log.info("load over, and the max phy offset = {}", this.getMaxPhyOffset());
            }
        } catch (Exception e) {
            log.error("load exception", e);
            result = false;
        }

        if (!result) {
            // 如果初始化失败则删除预先分配好的映射文件
            this.allocateMappedFileService.shutdown();
        }

        return result;
    }

    /**
     * broker启动的时候调用该方法
     *
     * @throws Exception
     */
    @Override
    public void start() throws Exception {
        // 写lock文件，尝试获取lock文件锁，保证磁盘上的文件只会被一个messageStore读写
        lock = lockFile.getChannel().tryLock(0, 1, false);
        if (lock == null || lock.isShared() || !lock.isValid()) {
            throw new RuntimeException("Lock failed,MQ already started");
        }

        lockFile.getChannel().write(ByteBuffer.wrap("lock".getBytes()));
        lockFile.getChannel().force(true);

        /**
         * 启动FlushConsumeQueueService，是一个单线程的服务，定时将ConsumeQueue文件的数据刷新到磁盘，周期由参数flushIntervalConsumeQueue设置，默认是1s
         *
         * 数据写入文件之后，因为多级缓存的原因不会马上写到磁盘上，所以会有一个单独的线程定时调用flush，这里是flush ConsumeQueue文件。
         * commitLog和IndexFile也有类似的逻辑
         */
        this.flushConsumeQueueService.start();
        // 启动commitLog CommitLogService只有在采用内存池缓存消息的时候才需要启动。在使用内存池的时候，这个服务会定时将内存池中的数据刷新到FileChannel中
        this.commitLog.start();
        // 消息存储指标统计服务，RT，TPS等
        this.storeStatsService.start();

        // 针对master，启动延时消息调度服务。如果消息消费失败，broker会延时重发。对于延时重发消息(topic=SCHEDULE_TOPIC_XXX)，这个服务负责检查是否有消息到了发送时间，到了时间则从延时队列中取出后重新发送
        if (this.scheduleMessageService != null && SLAVE != messageStoreConfig.getBrokerRole()) {
            this.scheduleMessageService.start();
        }

        // 启动ReputMessageService，该服务负责将CommitLog中的消息offset记录到consumeQueue文件中
        // 设置reputFromOffset偏移量，如果允许重复，初始化偏移量为confirmOffset，否则设置为commitLog当前最大偏移量
        if (this.getMessageStoreConfig().isDuplicationEnable()) {
            this.reputMessageService.setReputFromOffset(this.commitLog.getConfirmOffset());
        } else {
            this.reputMessageService.setReputFromOffset(this.commitLog.getMaxOffset());
        }
        this.reputMessageService.start();
        /**
         * 启动HAService，数据主从同步服务。如果是master，HAService默认监听10912端口，接收Slave的连接请求，然后将消息推送给slave，如果时slave则通过该服务连接到master接收数据
         * master与slave之间commitLog的HA传输
         * HAService.start()会启动相关的服务
         *    AcceptSocketService ：启动serverSocket并监听来自HAClient的连接
         *    GroupTransferService：broker写消息的时候如果需要同步等待消息同步到slave，会调用这个服务
         *    HAClient：如果是slave，才会启动HAClient
         */
        this.haService.start();
        // 启动broker的时候，创建一个abort文件，如果正常关闭，会在关闭的时候将abort文件删除
        this.createTempFile();
        /**
         * 启动定时任务
         *   1.定时清理过期的CommitLog、ConsumeQueue和IndexFile数据文件，默认文件写满后会保存72小时
         *   2.定时自检commitLog和ConsumeQueue文件，校验文件是否完整。主要用于监控，不会做修复文件的动作
         *   3.定时检查commitLog的lock时长(因为在write或者flush时候会lock)，如果lock的时间过长，则打印JVM堆栈，用于监控
         */
        this.addScheduleTask();
        this.shutdown = false;
    }

    public void shutdown() {
        if (!this.shutdown) {
            this.shutdown = true;

            this.scheduledExecutorService.shutdown();

            try {

                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.error("shutdown Exception, ", e);
            }

            if (this.scheduleMessageService != null) {
                this.scheduleMessageService.shutdown();
            }

            this.haService.shutdown();

            this.storeStatsService.shutdown();
            this.indexService.shutdown();
            this.commitLog.shutdown();
            this.reputMessageService.shutdown();
            this.flushConsumeQueueService.shutdown();
            this.allocateMappedFileService.shutdown();
            this.storeCheckpoint.flush();
            this.storeCheckpoint.shutdown();

            // 正常停机，需要删除abort文件
            if (this.runningFlags.isWriteable() && dispatchBehindBytes() == 0) {
                this.deleteFile(StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir()));
                shutDownNormal = true;
            } else {
                log.warn("the store may be wrong, so shutdown abnormally, and keep abort file.");
            }
        }

        this.transientStorePool.destroy();

        if (lockFile != null && lock != null) {
            try {
                lock.release();
                lockFile.close();
            } catch (IOException e) {
            }
        }
    }

    public void destroy() {
        this.destroyLogics();
        this.commitLog.destroy();
        this.indexService.destroy();
        this.deleteFile(StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir()));
        this.deleteFile(StorePathConfigHelper.getStoreCheckpoint(this.messageStoreConfig.getStorePathRootDir()));
    }

    public void destroyLogics() {
        for (ConcurrentMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueue logic : maps.values()) {
                logic.destroy();
            }
        }
    }

    /**
     * broker接收到消息之后，调用该方法存储消息
     * 首先判断broker是否是master，并且master当前是可写的。然后判断commitLog上次flush的时候是否超时，如果超时返回OS_PAGECACHE_BUSY的错误，最终调用commitlog.putMessage()方法保存消息。
     * <p>
     * 1、messageStore不可写
     * 2、当前broker是slave
     * 3、磁盘满或者写consumeQueue、IndexFile时出现错误
     * 4、topic长度超过了127
     * 5、消息属性长度超过了32676
     * 6、OSPage写忙
     *
     * @param msg Message instance to store
     * @return
     */
    public PutMessageResult putMessage(MessageExtBrokerInner msg) {
        // messageStore已经关闭，直接返回
        if (this.shutdown) {
            log.warn("message store has shutdown, so putMessage is forbidden");
            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }

        // salve不负责写消息，直接返回
        if (BrokerRole.SLAVE == this.messageStoreConfig.getBrokerRole()) {
            long value = this.printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                log.warn("message store is slave mode, so putMessage is forbidden ");
            }

            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }

        // broker当前不可写，直接返回
        if (!this.runningFlags.isWriteable()) {
            long value = this.printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                log.warn("message store is not writeable, so putMessage is forbidden " + this.runningFlags.getFlagBits());
            }

            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        } else {
            this.printTimes.set(0);
        }

        // topic长度过长，直接返回
        if (msg.getTopic().length() > Byte.MAX_VALUE) {
            log.warn("putMessage message topic length too long " + msg.getTopic().length());
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        // 消息属性长度过长，直接返回
        if (msg.getPropertiesString() != null && msg.getPropertiesString().length() > Short.MAX_VALUE) {
            log.warn("putMessage message properties length too long " + msg.getPropertiesString().length());
            return new PutMessageResult(PutMessageStatus.PROPERTIES_SIZE_EXCEEDED, null);
        }

        // 检测操作系统页写入是否忙
        if (this.isOSPageCacheBusy()) {
            return new PutMessageResult(PutMessageStatus.OS_PAGECACHE_BUSY, null);
        }

        // 记录开始时间
        long beginTime = this.getSystemClock().now();
        // commitLog保存消息
        PutMessageResult result = this.commitLog.putMessage(msg);

        // 计算耗时
        long eclipseTime = this.getSystemClock().now() - beginTime;
        // 耗时超过500ms，打印warning日志
        if (eclipseTime > 500) {
            log.warn("putMessage not in lock eclipse time(ms)={}, bodyLength={}", eclipseTime, msg.getBody().length);
        }
        // 收集消息store时间
        this.storeStatsService.setPutMessageEntireTimeMax(eclipseTime);

        if (null == result || !result.isOk()) {
            // 记录失败次数
            this.storeStatsService.getPutMessageFailedTimes().incrementAndGet();
        }

        return result;
    }

    /**
     * 保存多条消息
     *
     * @param messageExtBatch Message batch.
     * @return
     */
    public PutMessageResult putMessages(MessageExtBatch messageExtBatch) {
        // messageStore已经关闭
        if (this.shutdown) {
            log.warn("DefaultMessageStore has shutdown, so putMessages is forbidden");
            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }

        // slave不能写消息
        if (BrokerRole.SLAVE == this.messageStoreConfig.getBrokerRole()) {
            long value = this.printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                log.warn("DefaultMessageStore is in slave mode, so putMessages is forbidden ");
            }

            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }

        // messageStore不可写
        if (!this.runningFlags.isWriteable()) {
            long value = this.printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                log.warn("DefaultMessageStore is not writable, so putMessages is forbidden " + this.runningFlags.getFlagBits());
            }

            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        } else {
            this.printTimes.set(0);
        }

        // topic长度过长
        if (messageExtBatch.getTopic().length() > Byte.MAX_VALUE) {
            log.warn("PutMessages topic length too long " + messageExtBatch.getTopic().length());
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        // 消息体过长， 默认4M
        if (messageExtBatch.getBody().length > messageStoreConfig.getMaxMessageSize()) {
            log.warn("PutMessages body length too long " + messageExtBatch.getBody().length);
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        // os page cache 繁忙
        if (this.isOSPageCacheBusy()) {
            return new PutMessageResult(PutMessageStatus.OS_PAGECACHE_BUSY, null);
        }

        // 记录开始时间
        long beginTime = this.getSystemClock().now();
        PutMessageResult result = this.commitLog.putMessages(messageExtBatch);

        long eclipseTime = this.getSystemClock().now() - beginTime;
        if (eclipseTime > 500) {
            log.warn("not in lock eclipse time(ms)={}, bodyLength={}", eclipseTime, messageExtBatch.getBody().length);
        }
        this.storeStatsService.setPutMessageEntireTimeMax(eclipseTime);

        if (null == result || !result.isOk()) {
            this.storeStatsService.getPutMessageFailedTimes().incrementAndGet();
        }

        return result;
    }

    @Override
    public boolean isOSPageCacheBusy() {
        long begin = this.getCommitLog().getBeginTimeInLock();
        long diff = this.systemClock.now() - begin;

        return diff < 10000000
                && diff > this.messageStoreConfig.getOsPageCacheBusyTimeOutMills();
    }

    @Override
    public long lockTimeMills() {
        return this.commitLog.lockTimeMills();
    }

    public SystemClock getSystemClock() {
        return systemClock;
    }

    /**
     * 拉取消息
     *
     * @param group         Consumer group that launches this query. 消费组名称
     * @param topic         Topic to query. 主题名称
     * @param queueId       Queue ID to query. 队列id
     * @param offset        Logical offset to start from. 待拉取偏移量
     * @param maxMsgNums    Maximum count of messages to query. 一次行拉取的消息条数，默认32，可以通过消费者设置pullBatchSize，这个参数和consumeMessageBatchMaxSize=1是有区别的
     * @param messageFilter Message filter used to screen desired messages.
     * @return
     */
    public GetMessageResult getMessage(final String group, final String topic, final int queueId, final long offset,
                                       final int maxMsgNums,
                                       final MessageFilter messageFilter) {
        // messageStore已经宕机
        if (this.shutdown) {
            log.warn("message store has shutdown, so getMessage is forbidden");
            return null;
        }

        // messageStore不可读
        if (!this.runningFlags.isReadable()) {
            log.warn("message store is not readable, so getMessage is forbidden " + this.runningFlags.getFlagBits());
            return null;
        }

        // 记录开始时间
        long beginTime = this.getSystemClock().now();

        GetMessageStatus status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;
        // 设置拉取偏移量没从pullRequest中获取，初始从消费进度中获取
        long nextBeginOffset = offset;
        long minOffset = 0;
        long maxOffset = 0;

        GetMessageResult getResult = new GetMessageResult();

        // 获取commitLog当前最大偏移量
        final long maxOffsetPy = this.commitLog.getMaxOffset();

        // 找到要拉取消息的ConsumeQueue
        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            // 当前consumequeue最大最小offset
            minOffset = consumeQueue.getMinOffsetInQueue();
            maxOffset = consumeQueue.getMaxOffsetInQueue();

            // 客户端对异常情况的拉取偏移量校对逻辑，必须正常校对拉取偏移量，否则消息消费将出现堆积

            // 校验拉取的offset的大小是否正确，offset错误错调整
            // 队列中没有消息
            if (maxOffset == 0) {
                // maxOffset = 0表明当前消费队列中没有消息，拉取结果为NO_MESSAGE_IN_QUEUE
                status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;
                /**
                 * 校正下一次拉取的开始偏移量
                 * 如果当前broker是master，或者slave开启了offsetCheckSlave,下次从头开始拉取。
                 * 如果broker是从节点，并且没有开启offsetCheckInSlave，则使用原先的offset，因为考虑到主从同步延时的因素，
                 * 导致从节点consumequeue并没有同步到数据，offsetCheckInSlave设置为false比较保险，默认值便是false。
                 */
                nextBeginOffset = nextOffsetCorrection(offset, 0);
            }
            // 要拉取的偏移量小于队列最小的偏移量
            else if (offset < minOffset) {
                // offset < minOffset 表示待拉取消息偏移量小于队列起始偏移量，拉取结果为OFFSET_TOO_SMALL
                status = GetMessageStatus.OFFSET_TOO_SMALL;
                /**
                 * 1.设置下一次拉取的偏移量为minOffset
                 * 2.保持原先的offset
                 */
                nextBeginOffset = nextOffsetCorrection(offset, minOffset);
            }
            // 表示超出一个
            else if (offset == maxOffset) {
                // offset == maxOffset 如果待拉取偏移量等于队列的最大偏移量，拉取结果为OFFSET_OVERFLOW_ONE
                status = GetMessageStatus.OFFSET_OVERFLOW_ONE;
                // offset不变
                nextBeginOffset = nextOffsetCorrection(offset, offset);
            }
            // offset是一个非法的偏移量
            else if (offset > maxOffset) {
                // offset > maxOffset 表示偏移量越界，拉取结果：OFFSET_OVERFLOW_BADLY
                status = GetMessageStatus.OFFSET_OVERFLOW_BADLY;
                if (0 == minOffset) {
                    // 设置下一个偏移量为0
                    nextBeginOffset = nextOffsetCorrection(offset, minOffset);
                } else {
                    // 设置为最大偏移量
                    nextBeginOffset = nextOffsetCorrection(offset, maxOffset);
                }
            }
            // offset在minOffset maxOffset之间，从当前offset处尝试拉取32条消息
            else {
                // 获取consumequeue中从当前offset到当前consumequeue中最大可读消息内存
                SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(offset);
                if (bufferConsumeQueue != null) {
                    try {
                        status = GetMessageStatus.NO_MATCHED_MESSAGE;

                        // 这里只是初始化基本变量
                        // 下一个开始offset
                        long nextPhyFileStartOffset = Long.MIN_VALUE;
                        // 下一个pullOffset
                        long maxPhyOffsetPulling = 0;

                        int i = 0;
                        // 最大过滤消息字节数
                        final int maxFilterMessageCount = Math.max(16000, maxMsgNums * ConsumeQueue.CQ_STORE_UNIT_SIZE);
                        final boolean diskFallRecorded = this.messageStoreConfig.isDiskFallRecorded();
                        ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                        for (; i < bufferConsumeQueue.getSize() && i < maxFilterMessageCount; i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                            // 获取一条消息的信息 在commitLog中绝对offset size tagsCode
                            long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                            int sizePy = bufferConsumeQueue.getByteBuffer().getInt();
                            long tagsCode = bufferConsumeQueue.getByteBuffer().getLong();

                            // 将拉取最大物理位置设置为当前这条消息的offset
                            maxPhyOffsetPulling = offsetPy;

                            // 不相等，那么说明上次拉取的数据被丢弃
                            if (nextPhyFileStartOffset != Long.MIN_VALUE) {
                                // nextPhyFileStartOffset不等于Long.MIN_VALUE的情况只有数据不在本页中，需要滚动到下一页的情况，这时拿到的消息的偏移量也不可能大于本页第一条
                                if (offsetPy < nextPhyFileStartOffset)
                                    // 消息非法，不处理，开始过滤下一条消息
                                    continue;
                            }

                            // 是否超过了最大可使用内存 true-->超过
                            boolean isInDisk = checkInDiskByCommitOffset(offsetPy, maxOffsetPy);

                            // 本次拉取任务是否已经完成
                            if (this.isTheBatchFull(sizePy, maxMsgNums, getResult.getBufferTotalSize(), getResult.getMessageCount(),
                                    isInDisk)) {
                                break;
                            }

                            boolean extRet = false, isTagsCodeLegal = true;
                            if (consumeQueue.isExtAddr(tagsCode)) {
                                extRet = consumeQueue.getExt(tagsCode, cqExtUnit);
                                if (extRet) {
                                    tagsCode = cqExtUnit.getTagsCode();
                                } else {
                                    // can't find ext content.Client will filter messages by tag also.
                                    log.error("[BUG] can't find consume queue extend file content!addr={}, offsetPy={}, sizePy={}, topic={}, group={}",
                                            tagsCode, offsetPy, sizePy, topic, group);
                                    isTagsCodeLegal = false;
                                }
                            }

                            // tag过滤 如果messageFilter不为空，并且消息过滤失败(不是订阅的消息)，进入过滤下一条消息
                            if (messageFilter != null
                                    && !messageFilter.isMatchedByConsumeQueue(isTagsCodeLegal ? tagsCode : null, extRet ? cqExtUnit : null)) {
                                if (getResult.getBufferTotalSize() == 0) {
                                    status = GetMessageStatus.NO_MATCHED_MESSAGE;
                                }

                                continue;
                            }

                            // 从commitLog中获取offsetPy与sizePy对应的那一条消息
                            SelectMappedBufferResult selectResult = this.commitLog.getMessage(offsetPy, sizePy);
                            // 没有找到该消息，说明已经到了文件末尾，下次切换到下一个commitLog文件读取
                            if (null == selectResult) {
                                if (getResult.getBufferTotalSize() == 0) {
                                    status = GetMessageStatus.MESSAGE_WAS_REMOVING;
                                }
                                // 将下一次拉去偏移量更新到下一页
                                nextPhyFileStartOffset = this.commitLog.rollNextFile(offsetPy);
                                continue;
                            }

                            // SQL92过滤
                            if (messageFilter != null
                                    && !messageFilter.isMatchedByCommitLog(selectResult.getByteBuffer().slice(), null)) {
                                if (getResult.getBufferTotalSize() == 0) {
                                    status = GetMessageStatus.NO_MATCHED_MESSAGE;
                                }
                                // release...
                                selectResult.release();
                                continue;
                            }

                            // 将消息加入拉取结果
                            this.storeStatsService.getGetMessageTransferedMsgCount().incrementAndGet();
                            getResult.addMessage(selectResult);
                            status = GetMessageStatus.FOUND;
                            nextPhyFileStartOffset = Long.MIN_VALUE;
                        }

                        // 记录消息拉去滞后数据
                        if (diskFallRecorded) {
                            // 当前最大物理offset - 本次拉取到的offset
                            long fallBehind = maxOffsetPy - maxPhyOffsetPulling;
                            brokerStatsManager.recordDiskFallBehindSize(group, topic, queueId, fallBehind);
                        }

                        // 下一次拉取的offset
                        nextBeginOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);

                        // 如果当前消息积压已经超过了可用最大内存，那么建议下次从Slave拉取消息
                        long diff = maxOffsetPy - maxPhyOffsetPulling;
                        long memory = (long) (StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE
                                * (this.messageStoreConfig.getAccessMessageInMemoryMaxRatio() / 100.0));
                        getResult.setSuggestPullingFromSlave(diff > memory);
                    } finally {

                        bufferConsumeQueue.release();
                    }
                } else {
                    status = GetMessageStatus.OFFSET_FOUND_NULL;
                    nextBeginOffset = nextOffsetCorrection(offset, consumeQueue.rollNextFile(offset));
                    log.warn("consumer request topic: " + topic + "offset: " + offset + " minOffset: " + minOffset + " maxOffset: "
                            + maxOffset + ", but access logic queue failed.");
                }
            }
        } else {
            status = GetMessageStatus.NO_MATCHED_LOGIC_QUEUE;
            nextBeginOffset = nextOffsetCorrection(offset, 0);
        }

        if (GetMessageStatus.FOUND == status) {
            this.storeStatsService.getGetMessageTimesTotalFound().incrementAndGet();
        } else {
            this.storeStatsService.getGetMessageTimesTotalMiss().incrementAndGet();
        }
        long eclipseTime = this.getSystemClock().now() - beginTime;
        this.storeStatsService.setGetMessageEntireTimeMax(eclipseTime);

        getResult.setStatus(status);
        // 设置下一次拉取进度
        getResult.setNextBeginOffset(nextBeginOffset);
        getResult.setMaxOffset(maxOffset);
        getResult.setMinOffset(minOffset);
        return getResult;
    }

    public CommitLog getCommitLog() {
        return commitLog;
    }

    /**
     * 获取当前这个队列的最大偏移量
     *
     * @param topic   Topic name.
     * @param queueId Queue ID.
     * @return
     */
    public long getMaxOffsetInQueue(String topic, int queueId) {
        // 获取consumequeue
        ConsumeQueue logic = this.findConsumeQueue(topic, queueId);
        // 有consumequeue获取最大的offset
        if (logic != null) {
            long offset = logic.getMaxOffsetInQueue();
            return offset;
        }
        // 没有返回0
        return 0;
    }

    /**
     * 获取queue的最小进度
     *
     * @param topic   Topic name.
     * @param queueId Queue ID.
     * @return
     */
    public long getMinOffsetInQueue(String topic, int queueId) {
        ConsumeQueue logic = this.findConsumeQueue(topic, queueId);
        if (logic != null) {
            return logic.getMinOffsetInQueue();
        }

        return -1;
    }

    @Override
    public long getCommitLogOffsetInQueue(String topic, int queueId, long consumeQueueOffset) {
        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(consumeQueueOffset);
            if (bufferConsumeQueue != null) {
                try {
                    long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                    return offsetPy;
                } finally {
                    bufferConsumeQueue.release();
                }
            }
        }

        return 0;
    }

    /**
     * 队列在指定时间地点的最大偏移量
     *
     * @param topic     Topic of the message.
     * @param queueId   Queue ID.
     * @param timestamp Timestamp to look up.
     * @return
     */
    public long getOffsetInQueueByTime(String topic, int queueId, long timestamp) {
        ConsumeQueue logic = this.findConsumeQueue(topic, queueId);
        if (logic != null) {
            // 查询consumequeue在指定时间点的最大偏移量
            return logic.getOffsetInQueueByTime(timestamp);
        }

        return 0;
    }

    /**
     * 根据commitLogOffset查找消息
     *
     * @param commitLogOffset physical offset.
     * @return
     */
    public MessageExt lookMessageByOffset(long commitLogOffset) {
        // 读取出commitLogOffset这个位置的消息长度
        SelectMappedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, 4);
        if (null != sbr) {
            try {
                // 1 TOTALSIZE
                int size = sbr.getByteBuffer().getInt();
                // 读取到一个完整的消息
                return lookMessageByOffset(commitLogOffset, size);
            } finally {
                sbr.release();
            }
        }

        return null;
    }

    /**
     * 从offset开始截取消息
     *
     * @param commitLogOffset commit log offset.
     * @return
     */
    @Override
    public SelectMappedBufferResult selectOneMessageByOffset(long commitLogOffset) {
        SelectMappedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, 4);
        if (null != sbr) {
            try {
                // 1 TOTALSIZE
                int size = sbr.getByteBuffer().getInt();
                return this.commitLog.getMessage(commitLogOffset, size);
            } finally {
                sbr.release();
            }
        }

        return null;
    }

    @Override
    public SelectMappedBufferResult selectOneMessageByOffset(long commitLogOffset, int msgSize) {
        return this.commitLog.getMessage(commitLogOffset, msgSize);
    }

    public String getRunningDataInfo() {
        return this.storeStatsService.toString();
    }

    @Override
    public HashMap<String, String> getRuntimeInfo() {
        HashMap<String, String> result = this.storeStatsService.getRuntimeInfo();

        {
            String storePathPhysic = DefaultMessageStore.this.getMessageStoreConfig().getStorePathCommitLog();
            double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathPhysic);
            result.put(RunningStats.commitLogDiskRatio.name(), String.valueOf(physicRatio));

        }

        {

            String storePathLogics = StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir());
            double logicsRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogics);
            result.put(RunningStats.consumeQueueDiskRatio.name(), String.valueOf(logicsRatio));
        }

        {
            if (this.scheduleMessageService != null) {
                this.scheduleMessageService.buildRunningStats(result);
            }
        }

        result.put(RunningStats.commitLogMinOffset.name(), String.valueOf(DefaultMessageStore.this.getMinPhyOffset()));
        result.put(RunningStats.commitLogMaxOffset.name(), String.valueOf(DefaultMessageStore.this.getMaxPhyOffset()));

        return result;
    }

    @Override
    public long getMaxPhyOffset() {
        return this.commitLog.getMaxOffset();
    }

    @Override
    public long getMinPhyOffset() {
        return this.commitLog.getMinOffset();
    }

    @Override
    public long getEarliestMessageTime(String topic, int queueId) {
        ConsumeQueue logicQueue = this.findConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            long minLogicOffset = logicQueue.getMinLogicOffset();

            SelectMappedBufferResult result = logicQueue.getIndexBuffer(minLogicOffset / ConsumeQueue.CQ_STORE_UNIT_SIZE);
            return getStoreTime(result);
        }

        return -1;
    }

    private long getStoreTime(SelectMappedBufferResult result) {
        if (result != null) {
            try {
                final long phyOffset = result.getByteBuffer().getLong();
                final int size = result.getByteBuffer().getInt();
                long storeTime = this.getCommitLog().pickupStoreTimestamp(phyOffset, size);
                return storeTime;
            } catch (Exception e) {
            } finally {
                result.release();
            }
        }
        return -1;
    }

    @Override
    public long getEarliestMessageTime() {
        final long minPhyOffset = this.getMinPhyOffset();
        final int size = this.messageStoreConfig.getMaxMessageSize() * 2;
        return this.getCommitLog().pickupStoreTimestamp(minPhyOffset, size);
    }

    @Override
    public long getMessageStoreTimeStamp(String topic, int queueId, long consumeQueueOffset) {
        ConsumeQueue logicQueue = this.findConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            SelectMappedBufferResult result = logicQueue.getIndexBuffer(consumeQueueOffset);
            return getStoreTime(result);
        }

        return -1;
    }

    @Override
    public long getMessageTotalInQueue(String topic, int queueId) {
        ConsumeQueue logicQueue = this.findConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            return logicQueue.getMessageTotalInQueue();
        }

        return -1;
    }

    @Override
    public SelectMappedBufferResult getCommitLogData(final long offset) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so getPhyQueueData is forbidden");
            return null;
        }

        return this.commitLog.getData(offset);
    }

    /**
     * 将消息写入commitLog
     *
     * @param startOffset starting offset.
     * @param data        data to append.
     * @return
     */
    @Override
    public boolean appendToCommitLog(long startOffset, byte[] data) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so appendToPhyQueue is forbidden");
            return false;
        }
        // 将具体的消息写入commitLog
        boolean result = this.commitLog.appendData(startOffset, data);
        // 如果成功，唤醒ReputMessageService，实时将消息转发到消息消费队列和索引文件
        if (result) {
            this.reputMessageService.wakeup();
        } else {
            log.error("appendToPhyQueue failed " + startOffset + " " + data.length);
        }

        return result;
    }

    @Override
    public void executeDeleteFilesManually() {
        this.cleanCommitLogService.excuteDeleteFilesManualy();
    }

    /**
     * 根据messageKey查询消息
     * 先从IndexService中读取offset，然后到CommitLog中读取消息详情
     *
     * @param topic  topic of the message. 只能按照topic维度来查询消息，因为索引生成的时候key是用的topic + messageKey
     * @param key    message key. messageKey
     * @param maxNum maximum number of the messages possible. 最多返回的消息数，因为可以是用户设置的，并不保证唯一，所以可能取到多个消息，同时index中只存储了hash，所以hash相同的消息也会拉取出来
     * @param begin  begin timestamp. 起始时间
     * @param end    end timestamp. 结束时间  只会查询指定时间段的消息
     * @return
     */
    @Override
    public QueryMessageResult queryMessage(String topic, String key, int maxNum, long begin, long end) {
        QueryMessageResult queryMessageResult = new QueryMessageResult();

        long lastQueryMsgTime = end;

        for (int i = 0; i < 3; i++) {
            // 从indexService中查询所有offset
            QueryOffsetResult queryOffsetResult = this.indexService.queryOffset(topic, key, maxNum, begin, lastQueryMsgTime);
            if (queryOffsetResult.getPhyOffsets().isEmpty()) {
                break;
            }

            Collections.sort(queryOffsetResult.getPhyOffsets());

            queryMessageResult.setIndexLastUpdatePhyoffset(queryOffsetResult.getIndexLastUpdatePhyoffset());
            queryMessageResult.setIndexLastUpdateTimestamp(queryOffsetResult.getIndexLastUpdateTimestamp());

            for (int m = 0; m < queryOffsetResult.getPhyOffsets().size(); m++) {
                long offset = queryOffsetResult.getPhyOffsets().get(m);

                try {

                    boolean match = true;
                    MessageExt msg = this.lookMessageByOffset(offset);
                    if (0 == m) {
                        lastQueryMsgTime = msg.getStoreTimestamp();
                    }

//                    String[] keyArray = msg.getKeys().split(MessageConst.KEY_SEPARATOR);
//                    if (topic.equals(msg.getTopic())) {
//                        for (String k : keyArray) {
//                            if (k.equals(key)) {
//                                match = true;
//                                break;
//                            }
//                        }
//                    }

                    if (match) {
                        // 根据offset，到commitLog读取消息详情
                        SelectMappedBufferResult result = this.commitLog.getData(offset, false);
                        if (result != null) {
                            int size = result.getByteBuffer().getInt(0);
                            result.getByteBuffer().limit(size);
                            result.setSize(size);
                            queryMessageResult.addMessage(result);
                        }
                    } else {
                        log.warn("queryMessage hash duplicate, {} {}", topic, key);
                    }
                } catch (Exception e) {
                    log.error("queryMessage exception", e);
                }
            }

            if (queryMessageResult.getBufferTotalSize() > 0) {
                break;
            }

            if (lastQueryMsgTime < begin) {
                break;
            }
        }

        return queryMessageResult;
    }

    /**
     * 更新HAMaster地址
     *
     * @param newAddr new address.
     */
    @Override
    public void updateHaMasterAddress(String newAddr) {
        this.haService.updateMasterAddress(newAddr);
    }

    /**
     * slave落后master多少
     *
     * @return
     */
    @Override
    public long slaveFallBehindMuch() {
        return this.commitLog.getMaxOffset() - this.haService.getPush2SlaveMaxOffset().get();
    }

    @Override
    public long now() {
        return this.systemClock.now();
    }

    @Override
    public int cleanUnusedTopic(Set<String> topics) {
        Iterator<Entry<String, ConcurrentMap<Integer, ConsumeQueue>>> it = this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, ConsumeQueue>> next = it.next();
            String topic = next.getKey();

            if (!topics.contains(topic) && !topic.equals(ScheduleMessageService.SCHEDULE_TOPIC)) {
                ConcurrentMap<Integer, ConsumeQueue> queueTable = next.getValue();
                for (ConsumeQueue cq : queueTable.values()) {
                    cq.destroy();
                    log.info("cleanUnusedTopic: {} {} ConsumeQueue cleaned",
                            cq.getTopic(),
                            cq.getQueueId()
                    );

                    this.commitLog.removeQueueFromTopicQueueTable(cq.getTopic(), cq.getQueueId());
                }
                it.remove();

                log.info("cleanUnusedTopic: {},topic destroyed", topic);
            }
        }

        return 0;
    }

    public void cleanExpiredConsumerQueue() {
        long minCommitLogOffset = this.commitLog.getMinOffset();

        Iterator<Entry<String, ConcurrentMap<Integer, ConsumeQueue>>> it = this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, ConsumeQueue>> next = it.next();
            String topic = next.getKey();
            if (!topic.equals(ScheduleMessageService.SCHEDULE_TOPIC)) {
                ConcurrentMap<Integer, ConsumeQueue> queueTable = next.getValue();
                Iterator<Entry<Integer, ConsumeQueue>> itQT = queueTable.entrySet().iterator();
                while (itQT.hasNext()) {
                    Entry<Integer, ConsumeQueue> nextQT = itQT.next();
                    long maxCLOffsetInConsumeQueue = nextQT.getValue().getLastOffset();

                    if (maxCLOffsetInConsumeQueue == -1) {
                        log.warn("maybe ConsumeQueue was created just now. topic={} queueId={} maxPhysicOffset={} minLogicOffset={}.",
                                nextQT.getValue().getTopic(),
                                nextQT.getValue().getQueueId(),
                                nextQT.getValue().getMaxPhysicOffset(),
                                nextQT.getValue().getMinLogicOffset());
                    } else if (maxCLOffsetInConsumeQueue < minCommitLogOffset) {
                        log.info(
                                "cleanExpiredConsumerQueue: {} {} consumer queue destroyed, minCommitLogOffset: {} maxCLOffsetInConsumeQueue: {}",
                                topic,
                                nextQT.getKey(),
                                minCommitLogOffset,
                                maxCLOffsetInConsumeQueue);

                        DefaultMessageStore.this.commitLog.removeQueueFromTopicQueueTable(nextQT.getValue().getTopic(),
                                nextQT.getValue().getQueueId());

                        nextQT.getValue().destroy();
                        itQT.remove();
                    }
                }

                if (queueTable.isEmpty()) {
                    log.info("cleanExpiredConsumerQueue: {},topic destroyed", topic);
                    it.remove();
                }
            }
        }
    }

    public Map<String, Long> getMessageIds(final String topic, final int queueId, long minOffset, long maxOffset,
                                           SocketAddress storeHost) {
        Map<String, Long> messageIds = new HashMap<String, Long>();
        if (this.shutdown) {
            return messageIds;
        }

        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            minOffset = Math.max(minOffset, consumeQueue.getMinOffsetInQueue());
            maxOffset = Math.min(maxOffset, consumeQueue.getMaxOffsetInQueue());

            if (maxOffset == 0) {
                return messageIds;
            }

            long nextOffset = minOffset;
            while (nextOffset < maxOffset) {
                SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(nextOffset);
                if (bufferConsumeQueue != null) {
                    try {
                        int i = 0;
                        for (; i < bufferConsumeQueue.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                            long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                            final ByteBuffer msgIdMemory = ByteBuffer.allocate(MessageDecoder.MSG_ID_LENGTH);
                            String msgId =
                                    MessageDecoder.createMessageId(msgIdMemory, MessageExt.socketAddress2ByteBuffer(storeHost), offsetPy);
                            messageIds.put(msgId, nextOffset++);
                            if (nextOffset > maxOffset) {
                                return messageIds;
                            }
                        }
                    } finally {

                        bufferConsumeQueue.release();
                    }
                } else {
                    return messageIds;
                }
            }
        }
        return messageIds;
    }

    @Override
    public boolean checkInDiskByConsumeOffset(final String topic, final int queueId, long consumeOffset) {

        final long maxOffsetPy = this.commitLog.getMaxOffset();

        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(consumeOffset);
            if (bufferConsumeQueue != null) {
                try {
                    for (int i = 0; i < bufferConsumeQueue.getSize(); ) {
                        i += ConsumeQueue.CQ_STORE_UNIT_SIZE;
                        long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                        return checkInDiskByCommitOffset(offsetPy, maxOffsetPy);
                    }
                } finally {

                    bufferConsumeQueue.release();
                }
            } else {
                return false;
            }
        }
        return false;
    }

    public long dispatchBehindBytes() {
        return this.reputMessageService.behind();
    }

    @Override
    public long flush() {
        return this.commitLog.flush();
    }

    @Override
    public boolean resetWriteOffset(long phyOffset) {
        return this.commitLog.resetOffset(phyOffset);
    }

    @Override
    public long getConfirmOffset() {
        return this.commitLog.getConfirmOffset();
    }

    @Override
    public void setConfirmOffset(long phyOffset) {
        this.commitLog.setConfirmOffset(phyOffset);
    }

    /**
     * 从指定offset开始，获取指定大小的message
     *
     * @param commitLogOffset
     * @param size
     * @return
     */
    public MessageExt lookMessageByOffset(long commitLogOffset, int size) {
        SelectMappedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, size);
        if (null != sbr) {
            try {
                // 将buffer数据转化为MessageExt
                return MessageDecoder.decode(sbr.getByteBuffer(), true, false);
            } finally {
                sbr.release();
            }
        }

        return null;
    }

    /**
     * 根据topic 和 queueId查找ConsumeQueue
     * 如果不存在consumequeue，则新建一个加入
     *
     * @param topic
     * @param queueId
     * @return
     */
    public ConsumeQueue findConsumeQueue(String topic, int queueId) {
        // Map<topic, Map<queueId, ConsumeQueue>>
        ConcurrentMap<Integer, ConsumeQueue> map = consumeQueueTable.get(topic);
        // 该topic没有对应的队列
        if (null == map) {
            // 新建队列放入
            ConcurrentMap<Integer, ConsumeQueue> newMap = new ConcurrentHashMap<Integer, ConsumeQueue>(128);
            ConcurrentMap<Integer, ConsumeQueue> oldMap = consumeQueueTable.putIfAbsent(topic, newMap);
            // 如果放入新建的队列时已经有了队列，那么使用已经有的，否则使用新的
            if (oldMap != null) {
                map = oldMap;
            } else {
                map = newMap;
            }
        }

        // 从队列中获取指定队列的consumequeue
        ConsumeQueue logic = map.get(queueId);
        // 没有consumequeue
        if (null == logic) {
            // 新建consumequeue放入
            ConsumeQueue newLogic = new ConsumeQueue(
                    topic,
                    queueId,
                    StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()),
                    this.getMessageStoreConfig().getMapedFileSizeConsumeQueue(),
                    this);
            ConsumeQueue oldLogic = map.putIfAbsent(queueId, newLogic);
            // 存入新建consumequeue时已经有了consumequeue，那么返回旧的consumequeue，，否则使用新的consumequeue
            if (oldLogic != null) {
                logic = oldLogic;
            } else {
                logic = newLogic;
            }
        }

        return logic;
    }

    /**
     * 校正下一次拉取的开始偏移量
     * 如果当前broker是master，或者slave开启了offsetCheckSlave,下次从头开始拉取。
     * 如果broker是从节点，并且没有开启offsetCheckInSlave，则使用原先的offset，因为考虑到主从同步延时的因素，
     * 导致从节点consumequeue并没有同步到数据，offsetCheckInSlave设置为false比较保险，默认值便是false。
     *
     * @param oldOffset
     * @param newOffset
     * @return
     */
    private long nextOffsetCorrection(long oldOffset, long newOffset) {
        long nextOffset = oldOffset;
        // 如果当前broker是master或者允许从Slave获取偏移量
        if (this.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE || this.getMessageStoreConfig().isOffsetCheckInSlave()) {
            nextOffset = newOffset;
        }
        return nextOffset;
    }

    /**
     * 检查系统内存是否能够容纳当前偏移量差值间的数据
     * 如果当前系统不能容纳，那么说明offsetPy这个偏移量的消息已经从内存中国置换到了磁盘中
     *
     * @param offsetPy
     * @param maxOffsetPy
     * @return
     */
    private boolean checkInDiskByCommitOffset(long offsetPy, long maxOffsetPy) {
        /**
         * 最大可使用内存
         * this.messageStoreConfig.getAccessMessageInMemoryMaxRatio() 消息存储在物理内存中占用的最大比例
         */
        long memory = (long) (StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE * (this.messageStoreConfig.getAccessMessageInMemoryMaxRatio() / 100.0));
        // 是否超过了最大可使用内存
        return (maxOffsetPy - offsetPy) > memory;
    }

    /**
     * 本次拉取任务是否已经完成
     *
     * @param sizePy       一条消息在ConsumeQueue中的大小
     * @param maxMsgNums   本次拉取的消息条数
     * @param bufferTotal  已拉取消息字节总长度，不包含当前消息
     * @param messageTotal 已拉取消息总条数
     * @param isInDisk     当前消息是否已经被置换到磁盘中
     * @return
     */
    private boolean isTheBatchFull(int sizePy, int maxMsgNums, int bufferTotal, int messageTotal, boolean isInDisk) {

        // bufferTotal 与 messageTotal都等于0，那么本次拉取任务才刚开始，本次拉取任务未完成，返回false
        if (0 == bufferTotal || 0 == messageTotal) {
            return false;
        }

        // 返回true，表示已经拉取完成
        if (maxMsgNums <= messageTotal) {
            return true;
        }

        // 消息是否已经置换到磁盘
        if (isInDisk) {
            // 如果已经拉取的消息字节数+消息size > maxTransferBytesOnMessageInDisk,默认64K，则不继续拉去消息，返回拉取任务结束
            if ((bufferTotal + sizePy) > this.messageStoreConfig.getMaxTransferBytesOnMessageInDisk()) {
                return true;
            }
            // 如果已拉取的消息条数 > maxTransferCountOnMessageInDisk,默认8条，拉取任务结束
            if (messageTotal > this.messageStoreConfig.getMaxTransferCountOnMessageInDisk() - 1) {
                return true;
            }
        }
        // 消息在内存中
        else {
            if ((bufferTotal + sizePy) > this.messageStoreConfig.getMaxTransferBytesOnMessageInMemory()) {
                return true;
            }

            if (messageTotal > this.messageStoreConfig.getMaxTransferCountOnMessageInMemory() - 1) {
                return true;
            }
        }

        return false;
    }

    /**
     * 删除指定文件
     *
     * @param fileName
     */
    private void deleteFile(final String fileName) {
        File file = new File(fileName);
        boolean result = file.delete();
        log.info(fileName + (result ? " delete OK" : " delete Failed"));
    }

    /**
     * 创建一个abort文件
     *
     * @throws IOException
     */
    private void createTempFile() throws IOException {
        String fileName = StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir());
        File file = new File(fileName);
        MappedFile.ensureDirOK(file.getParent());
        // 创建一个名为abort的文件  createNewFile()当文件不存在，并且文件创建成功的情况下返回true
        boolean result = file.createNewFile();
        log.info(fileName + (result ? " create OK" : " already exists"));
    }

    /**
     * 设置定时任务
     * 1.定时清理过期的CommitLog、ConsumeQueue和IndexFile数据文件，默认文件写满后会保存72小时
     * 2.定时自检commitLog和ConsumeQueue文件，校验文件是否完整。主要用于监控，不会做修复文件的动作
     * 3.定时检查commitLog的lock时长(因为在write或者flush时候会lock)，如果lock的时间过长，则打印JVM堆栈，用于监控
     * <p>
     * 由于RocketMQ操作commitLog、ConsumeQueue文件是基于内存映射机制并在启动的时候回加载commitlog、consumequeue目录下的所有文件，
     * 为了避免内存与磁盘的浪费，不可能将消息永久存储在消息服务器上，所以需要引入一种机制来避免内存与磁盘的浪费，不能将消息永久
     * 存储在消息服务器上，所以需要引入一种机制来删除已过期的文件。
     * RocketMQ顺序写commitlog文件、consumequeue文件，所有写操作全部落在最后一个commitlog、consumequeue文件上，之前的文件在下一个
     * 文件创建后将不会再被更新，则认为是过期文件，可以被删除，RocketMQ不会关注这个文件上的消息是否全部被消费。
     * 默认每个文件的过期时间为72小时，通过在Broker配置文件中设置fileReservedTime来改变过期时间，单位为小时
     */
    private void addScheduleTask() {

        // 定时清理任务
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                DefaultMessageStore.this.cleanFilesPeriodically();
            }
        }, 1000 * 60, this.messageStoreConfig.getCleanResourceInterval(), TimeUnit.MILLISECONDS);

        // 自检
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                DefaultMessageStore.this.checkSelf();
            }
        }, 1, 10, TimeUnit.MINUTES);

        // 检查锁时长
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                if (DefaultMessageStore.this.getMessageStoreConfig().isDebugLockEnable()) {
                    try {
                        if (DefaultMessageStore.this.commitLog.getBeginTimeInLock() != 0) {
                            // 获取commitLog锁时间
                            long lockTime = System.currentTimeMillis() - DefaultMessageStore.this.commitLog.getBeginTimeInLock();
                            // 锁时间超过了1000，则打印堆栈信息
                            if (lockTime > 1000 && lockTime < 10000000) {

                                String stack = UtilAll.jstack();
                                final String fileName = System.getProperty("user.home") + File.separator + "debug/lock/stack-"
                                        + DefaultMessageStore.this.commitLog.getBeginTimeInLock() + "-" + lockTime;
                                MixAll.string2FileNotSafe(stack, fileName);
                            }
                        }
                    } catch (Exception e) {
                    }
                }
            }
        }, 1, 1, TimeUnit.SECONDS);

        // this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
        // @Override
        // public void run() {
        // DefaultMessageStore.this.cleanExpiredConsumerQueue();
        // }
        // }, 1, 1, TimeUnit.HOURS);
    }

    /**
     * 每10s执行一次，检测是否需要清除过期文件，执行频率可以通过设置cleanResourceInterval，默认10s
     * <p>
     * 由于RocketMQ操作commitLog、consumequeue文件，都是基于内存映射方法并在启动的时候，
     * 会加载commitlog、consumequeue目录下所有文件，为了避免内存与磁盘的浪费，不消可能将
     * 消息永久存储在消息服务器上，所以需要一种机制来删除过期的文件。RocketMQ顺序写commitlog、
     * consumequeue文件，所有写操作全部落在最后一个commitlog或者 consumequeue文件上，
     * 之前的文件在下一个文件创建后，将不会再被更新，RocketMQ清除过期文件的办法是：
     * 如果非当前写文件在一定时间间隔内没有再次被更新，则认为文件过期，可以删除，
     * RocketMQ不会管这个文件上的消息是否全部消费，默认每个文件的过期时间是72小时。
     * 通过在broker配置文件中设置fileReserveTime可以改变过期时间
     */
    private void cleanFilesPeriodically() {
        // 清除消息存储文件
        this.cleanCommitLogService.run();
        // 清除消息消费队列文件
        this.cleanConsumeQueueService.run();
    }

    private void checkSelf() {
        // commitLog文件自检，前一个文件与后一个文件第一条消息的offset差要等于一个完整文件大小
        this.commitLog.checkSelf();

        // Map<topic, Map<queueId, ConsumeQueue>>
        Iterator<Entry<String, ConcurrentMap<Integer, ConsumeQueue>>> it = this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, ConsumeQueue>> next = it.next();
            Iterator<Entry<Integer, ConsumeQueue>> itNext = next.getValue().entrySet().iterator();
            while (itNext.hasNext()) {
                Entry<Integer, ConsumeQueue> cq = itNext.next();
                // consumequeue自检，前一个文件与后一个文件第一条消息的offset差要等于一个完整文件大小
                cq.getValue().checkSelf();
            }
        }
    }

    private boolean isTempFileExist() {
        String fileName = StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir());
        File file = new File(fileName);
        return file.exists();
    }

    /**
     * CommitQueue文件规则
     * comsumequeue的目录结构为comsumequeue/{topicName}/{queueId}/{fileName},文件默认大小600w byte
     * 文件命名规则:00_000_000_000_000_000_000 、 00_000_000_000_006_000_000 、00_000_000_000_012_000_000
     * 文件内存储的数据格式：commitlog offset(commitlog文件物理偏移量 8byte) + size(消息大小 4byte) + tagsCode(tag的hashcode 8byte)，每条占用20byte，每个文件最多存储30w条
     * <p>
     * 消息查找
     * 从consumequeue读取消息，根据（commitLog offset）/ 1073741824 = N 得到在第N+1个commitLog文件中，
     * 消息的开始位置(commitLog offset) - (1073741824 * N) = fileOffset，从offset字节开始读取，读取长度在首个4字节中
     *
     * @return
     */
    private boolean loadConsumeQueue() {
        // comsumequeue的目录结构为comsumequeue/{topicName}/{queueId}/{fileName}
        File dirLogic = new File(StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()));
        // 所有topic目录
        File[] fileTopicList = dirLogic.listFiles();
        if (fileTopicList != null) {

            for (File fileTopic : fileTopicList) {
                // topic
                String topic = fileTopic.getName();
                // queueId目录
                File[] fileQueueIdList = fileTopic.listFiles();
                if (fileQueueIdList != null) {
                    for (File fileQueueId : fileQueueIdList) {
                        int queueId;
                        try {
                            // queueId
                            queueId = Integer.parseInt(fileQueueId.getName());
                        } catch (NumberFormatException e) {
                            continue;
                        }
                        // 创建consumequeue
                        ConsumeQueue logic = new ConsumeQueue(
                                topic,
                                queueId,
                                StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()),
                                this.getMessageStoreConfig().getMapedFileSizeConsumeQueue(),
                                this);
                        this.putConsumeQueue(topic, queueId, logic);
                        // 加载consumequeue下的mappedFile
                        if (!logic.load()) {
                            return false;
                        }
                    }
                }
            }
        }

        log.info("load logics queue all over, OK");

        return true;
    }

    /**
     * 恢复动作
     * 根据broker是否是正常停止执行不同的恢复策略
     * 存储启动时文件恢复工作主要完成flushedPosition、committedWhere指针的位置、消息消费队列最大偏移量加载到内存，并删除flushedPosition之后的所有文件。
     * 如果broker异常启动，在文件恢复过程中，RocketMQ会将最后一个有效文件中的所有消息重新转发到消息消费队列与索引队列，确保消息不丢失，但是同时会带来消息重复。
     * RocketMQ保证消息不丢失但是不保证消息不重复消费，消息消费业务方需要实现消息消费的幂等设计
     *
     * @param lastExitOK
     */
    private void recover(final boolean lastExitOK) {
        // 恢复消费队列，确定消费队列的最大物理偏移量，对应~/store/consumequeue目录下面的内容
        this.recoverConsumeQueue();

        // 根据上次进程退出的结果判断采取正常/异常commitlog恢复，对应~/store/commitlog目录下的内容
        if (lastExitOK) {
            // 正常退出，按照正常恢复
            this.commitLog.recoverNormally();
        } else {
            // 异常退出，按照异常修复逻辑恢复
            this.commitLog.recoverAbnormally();
        }
        // 恢复topic队列
        this.recoverTopicQueueTable();
    }

    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    public TransientStorePool getTransientStorePool() {
        return transientStorePool;
    }

    /**
     * 将consumeQueue放入topic集合下
     *
     * @param topic
     * @param queueId
     * @param consumeQueue
     */
    private void putConsumeQueue(final String topic, final int queueId, final ConsumeQueue consumeQueue) {
        ConcurrentMap<Integer/* queueId */, ConsumeQueue> map = this.consumeQueueTable.get(topic);
        if (null == map) {
            map = new ConcurrentHashMap<Integer/* queueId */, ConsumeQueue>();
            map.put(queueId, consumeQueue);
            this.consumeQueueTable.put(topic, map);
        } else {
            map.put(queueId, consumeQueue);
        }
    }

    /**
     * 恢复消费队列
     * 主要任务是将非法offset的mappedFile踢出
     */
    private void recoverConsumeQueue() {
        for (ConcurrentMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueue logic : maps.values()) {
                logic.recover();
            }
        }
    }

    private void recoverTopicQueueTable() {
        HashMap<String/* topic-queueid */, Long/* offset */> table = new HashMap<String, Long>(1024);
        long minPhyOffset = this.commitLog.getMinOffset();
        for (ConcurrentMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueue logic : maps.values()) {
                String key = logic.getTopic() + "-" + logic.getQueueId();
                table.put(key, logic.getMaxOffsetInQueue());
                logic.correctMinOffset(minPhyOffset);
            }
        }

        this.commitLog.setTopicQueueTable(table);
    }

    public AllocateMappedFileService getAllocateMappedFileService() {
        return allocateMappedFileService;
    }

    public StoreStatsService getStoreStatsService() {
        return storeStatsService;
    }

    public RunningFlags getAccessRights() {
        return runningFlags;
    }

    public ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> getConsumeQueueTable() {
        return consumeQueueTable;
    }

    public StoreCheckpoint getStoreCheckpoint() {
        return storeCheckpoint;
    }

    public HAService getHaService() {
        return haService;
    }

    public ScheduleMessageService getScheduleMessageService() {
        return scheduleMessageService;
    }

    public RunningFlags getRunningFlags() {
        return runningFlags;
    }

    /**
     * 转发
     *
     * @param req
     */
    public void doDispatch(DispatchRequest req) {
        for (CommitLogDispatcher dispatcher : this.dispatcherList) {
            dispatcher.dispatch(req);
        }
    }

    /**
     * 将commitLog中数据更新到ConsumeQueue中
     *
     * @param dispatchRequest
     */
    public void putMessagePositionInfo(DispatchRequest dispatchRequest) {
        // 找到对应的ConsumeQueue文件。consumeQueue的数据存储结构，每个topicId + queueId对应一个ConsumeQueue，每个ConsumeQueue包含了一系列MappedFile。如果不存在ConsumeQueue就会新建一个
        ConsumeQueue cq = this.findConsumeQueue(dispatchRequest.getTopic(), dispatchRequest.getQueueId());
        cq.putMessagePositionInfoWrapper(dispatchRequest);
    }

    public BrokerStatsManager getBrokerStatsManager() {
        return brokerStatsManager;
    }

    public int remainTransientStoreBufferNumbs() {
        return this.transientStorePool.remainBufferNumbs();
    }

    @Override
    public boolean isTransientStorePoolDeficient() {
        return remainTransientStoreBufferNumbs() == 0;
    }

    @Override
    public LinkedList<CommitLogDispatcher> getDispatcherList() {
        return this.dispatcherList;
    }

    @Override
    public ConsumeQueue getConsumeQueue(String topic, int queueId) {
        ConcurrentMap<Integer, ConsumeQueue> map = consumeQueueTable.get(topic);
        if (map == null) {
            return null;
        }
        return map.get(queueId);
    }

    public void unlockMappedFile(final MappedFile mappedFile) {
        this.scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                mappedFile.munlock();
            }
        }, 6, TimeUnit.SECONDS);
    }

    /**
     * Dispatcher构建ConsumeQueue
     */
    class CommitLogDispatcherBuildConsumeQueue implements CommitLogDispatcher {

        /**
         * broker在构造ConsumeQueue的时候会判断是否是prepare或者rollback消息，如果是这两种中的一种则不会将该消息放入ConsumeQueue，
         * consumer在拉取消息的时候也就不会拉取到prepare和rollback的消息
         *
         * @param request
         */
        @Override
        public void dispatch(DispatchRequest request) {
            final int tranType = MessageSysFlag.getTransactionValue(request.getSysFlag());
            switch (tranType) {
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                    DefaultMessageStore.this.putMessagePositionInfo(request);
                    break;
                // broker在构造ConsumeQueue的时候会判断是否是prepare或者rollback消息，如果是这两种中的一种则不会将该消息放入ConsumeQueue，consumer在拉取消息的时候也就不会拉取到prepare和rollback的消息
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    break;
            }
        }
    }

    class CommitLogDispatcherBuildIndex implements CommitLogDispatcher {

        /**
         * 调用indexService创建index
         *
         * @param request
         */
        @Override
        public void dispatch(DispatchRequest request) {
            // 如果messageIndexEnable设置为true，则垫佣IndexService#buildIndex构建hash索引，否则忽略背刺转发任务
            if (DefaultMessageStore.this.messageStoreConfig.isMessageIndexEnable()) {
                DefaultMessageStore.this.indexService.buildIndex(request);
            }
        }
    }

    class CleanCommitLogService {

        private final static int MAX_MANUAL_DELETE_FILE_TIMES = 20;
        // 如果磁盘分区使用率超过了该阈值，将设置磁盘不可写，此时会拒绝新消息写入
        private final double diskSpaceWarningLevelRatio =
                Double.parseDouble(System.getProperty("rocketmq.broker.diskSpaceWarningLevelRatio", "0.90"));

        // 如果磁盘分区使用超过了该阈值，建议立即执行过期文件删除，但不会拒绝新消息的写入
        private final double diskSpaceCleanForciblyRatio =
                Double.parseDouble(System.getProperty("rocketmq.broker.diskSpaceCleanForciblyRatio", "0.85"));
        private long lastRedeleteTimestamp = 0;

        private volatile int manualDeleteFileSeveralTimes = 0;

        private volatile boolean cleanImmediately = false;

        public void excuteDeleteFilesManualy() {
            this.manualDeleteFileSeveralTimes = MAX_MANUAL_DELETE_FILE_TIMES;
            DefaultMessageStore.log.info("executeDeleteFilesManually was invoked");
        }

        public void run() {
            try {
                // 尝试删除过期文件
                this.deleteExpiredFiles();
                // 重试删除被hang的文件(由于被其他线程引用在第一阶段未删除的文件)
                this.redeleteHangedFile();
            } catch (Throwable e) {
                DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        private void deleteExpiredFiles() {
            int deleteCount = 0;
            // 文件保留时间，也就是从最后一次更新时间到现在，如果超过了该时间，则认为文件过期，可以删除
            long fileReservedTime = DefaultMessageStore.this.getMessageStoreConfig().getFileReservedTime();
            // 删除物理文件的间隔，因为在一次清除过程中，可能需需要删除的文件不止一个，该值指定两次删除文件的间隔时间
            int deletePhysicFilesInterval = DefaultMessageStore.this.getMessageStoreConfig().getDeleteCommitLogFilesInterval();
            /**
             * 在清除过期文件时，如果该文件被其他线程占用(引用次数大于0.比如读取消息)，此时会阻止此次删除任务。
             * 同时在第一次试图删除该文件时记录当前时间戳，destroyMappedFileIntervalForcibly第一次拒绝删除
             * 之后能保留的最大时间，在此时间内，同样可以被拒绝删除，同样会将引用减少1000个，超过该时间间隔后，
             * 文件将会被强制删除
             */
            int destroyMapedFileIntervalForcibly = DefaultMessageStore.this.getMessageStoreConfig().getDestroyMapedFileIntervalForcibly();


            // -------------------------------------------------------------------------------
            // RocketMQ在如下三种情况任意之一满足的情况下将继续执行删除文件操作
            // 1、指定删除文件的时间点、RocketMQ通过deleteWhen设置一天的固定时间执行一次删除过期文件的操作，默认凌晨4点
            // 2、磁盘空间是否充足，如果磁盘空间不足，则返回true，表示应该触发过期文件删除操作
            // 3、预留，手工触发，可以通过调用executeDeleteFilesManually方法手动触发过期文件删除、目前暂未封装手动触发文件删除的指令
            // 是否到了删除时间点 默认凌晨4点
            boolean timeup = this.isTimeToDelete();
            // 判断磁盘空间是否充足，如果不充足，返回true，表示应该触发过期文件删除操作
            boolean spacefull = this.isSpaceToDelete();
            // 预留，手动触发，可以通过调用executeDeleteFilesManualy方法触发过期文件删除
            boolean manualDelete = this.manualDeleteFileSeveralTimes > 0;

            // --------------------------------------------------------------------------------

            // 只要时间、磁盘、手动触发三个中有一个满足便会触发清理
            if (timeup || spacefull || manualDelete) {

                if (manualDelete)
                    this.manualDeleteFileSeveralTimes--;

                boolean cleanAtOnce = DefaultMessageStore.this.getMessageStoreConfig().isCleanFileForciblyEnable() && this.cleanImmediately;

                log.info("begin to delete before {} hours file. timeup: {} spacefull: {} manualDeleteFileSeveralTimes: {} cleanAtOnce: {}",
                        fileReservedTime,
                        timeup,
                        spacefull,
                        manualDeleteFileSeveralTimes,
                        cleanAtOnce);

                fileReservedTime *= 60 * 60 * 1000;

                deleteCount = DefaultMessageStore.this.commitLog.deleteExpiredFile(fileReservedTime, deletePhysicFilesInterval,
                        destroyMapedFileIntervalForcibly, cleanAtOnce);
                if (deleteCount > 0) {
                } else if (spacefull) {
                    log.warn("disk space will be full soon, but delete file failed.");
                }
            }
        }

        /**
         * 部分MappedFile会在第一次删除时无法删除掉，会在这里再删除
         */
        private void redeleteHangedFile() {
            int interval = DefaultMessageStore.this.getMessageStoreConfig().getRedeleteHangedFileInterval();
            long currentTimestamp = System.currentTimeMillis();
            if ((currentTimestamp - this.lastRedeleteTimestamp) > interval) {
                this.lastRedeleteTimestamp = currentTimestamp;
                int destroyMapedFileIntervalForcibly =
                        DefaultMessageStore.this.getMessageStoreConfig().getDestroyMapedFileIntervalForcibly();
                if (DefaultMessageStore.this.commitLog.retryDeleteFirstFile(destroyMapedFileIntervalForcibly)) {
                }
            }
        }

        public String getServiceName() {
            return CleanCommitLogService.class.getSimpleName();
        }

        private boolean isTimeToDelete() {
            String when = DefaultMessageStore.this.getMessageStoreConfig().getDeleteWhen();
            if (UtilAll.isItTimeToDo(when)) {
                DefaultMessageStore.log.info("it's time to reclaim disk space, " + when);
                return true;
            }

            return false;
        }

        /**
         * 磁盘占用率是否已经达到了删除阈值
         *
         * @return
         */
        private boolean isSpaceToDelete() {
            // 获取maxUsedSpaceRatio，表示commitlog、consumequeue文件所在磁盘分区的最大使用量，如果超过该值，则需要立即清除过期文件
            double ratio = DefaultMessageStore.this.getMessageStoreConfig().getDiskMaxUsedSpaceRatio() / 100.0;

            // 是否需要立即执行清除过期文件操作
            cleanImmediately = false;

            // commitlog磁盘状态
            {
                String storePathPhysic = DefaultMessageStore.this.getMessageStoreConfig().getStorePathCommitLog();
                // 计算当前commitlog文件所在磁盘分区磁盘使用率
                double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathPhysic);
                if (physicRatio > diskSpaceWarningLevelRatio) {
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskFull();
                    if (diskok) {
                        DefaultMessageStore.log.error("physic disk maybe full soon " + physicRatio + ", so mark disk full");
                    }

                    cleanImmediately = true;
                } else if (physicRatio > diskSpaceCleanForciblyRatio) {
                    cleanImmediately = true;
                } else {
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskOK();
                    if (!diskok) {
                        DefaultMessageStore.log.info("physic disk space OK " + physicRatio + ", so mark disk ok");
                    }
                }

                if (physicRatio < 0 || physicRatio > ratio) {
                    DefaultMessageStore.log.info("physic disk maybe full soon, so reclaim space, " + physicRatio);
                    return true;
                }
            }

            // consumequeue磁盘状态
            {
                String storePathLogics = StorePathConfigHelper
                        .getStorePathConsumeQueue(DefaultMessageStore.this.getMessageStoreConfig().getStorePathRootDir());
                double logicsRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogics);
                if (logicsRatio > diskSpaceWarningLevelRatio) {
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskFull();
                    if (diskok) {
                        DefaultMessageStore.log.error("logics disk maybe full soon " + logicsRatio + ", so mark disk full");
                    }

                    cleanImmediately = true;
                } else if (logicsRatio > diskSpaceCleanForciblyRatio) {
                    cleanImmediately = true;
                } else {
                    // 标记或恢复磁盘可写
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskOK();
                    if (!diskok) {
                        DefaultMessageStore.log.info("logics disk space OK " + logicsRatio + ", so mark disk ok");
                    }
                }

                if (logicsRatio < 0 || logicsRatio > ratio) {
                    DefaultMessageStore.log.info("logics disk maybe full soon, so reclaim space, " + logicsRatio);
                    return true;
                }
            }

            return false;
        }

        public int getManualDeleteFileSeveralTimes() {
            return manualDeleteFileSeveralTimes;
        }

        public void setManualDeleteFileSeveralTimes(int manualDeleteFileSeveralTimes) {
            this.manualDeleteFileSeveralTimes = manualDeleteFileSeveralTimes;
        }
    }

    class CleanConsumeQueueService {
        private long lastPhysicalMinOffset = 0;

        public void run() {
            try {
                this.deleteExpiredFiles();
            } catch (Throwable e) {
                DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        private void deleteExpiredFiles() {
            int deleteLogicsFilesInterval = DefaultMessageStore.this.getMessageStoreConfig().getDeleteConsumeQueueFilesInterval();

            long minOffset = DefaultMessageStore.this.commitLog.getMinOffset();
            if (minOffset > this.lastPhysicalMinOffset) {
                this.lastPhysicalMinOffset = minOffset;

                ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> tables = DefaultMessageStore.this.consumeQueueTable;

                for (ConcurrentMap<Integer, ConsumeQueue> maps : tables.values()) {
                    for (ConsumeQueue logic : maps.values()) {
                        int deleteCount = logic.deleteExpiredFile(minOffset);

                        if (deleteCount > 0 && deleteLogicsFilesInterval > 0) {
                            try {
                                Thread.sleep(deleteLogicsFilesInterval);
                            } catch (InterruptedException ignored) {
                            }
                        }
                    }
                }

                DefaultMessageStore.this.indexService.deleteExpiredFile(minOffset);
            }
        }

        public String getServiceName() {
            return CleanConsumeQueueService.class.getSimpleName();
        }
    }

    /**
     * 刷盘服务
     */
    class FlushConsumeQueueService extends ServiceThread {
        private static final int RETRY_TIMES_OVER = 3;
        private long lastFlushTimestamp = 0;

        private void doFlush(int retryTimes) {
            // 2
            int flushConsumeQueueLeastPages = DefaultMessageStore.this.getMessageStoreConfig().getFlushConsumeQueueLeastPages();

            // 重试次数已经到达了RETRY_TIMES_OVER，立即刷盘
            if (retryTimes == RETRY_TIMES_OVER) {
                flushConsumeQueueLeastPages = 0;
            }

            long logicsMsgTimestamp = 0;

            // 60s
            int flushConsumeQueueThoroughInterval = DefaultMessageStore.this.getMessageStoreConfig().getFlushConsumeQueueThoroughInterval();
            long currentTimeMillis = System.currentTimeMillis();
            // 距离上次刷盘已经到达最大刷盘间隔，立即刷盘
            if (currentTimeMillis >= (this.lastFlushTimestamp + flushConsumeQueueThoroughInterval)) {
                this.lastFlushTimestamp = currentTimeMillis;
                flushConsumeQueueLeastPages = 0;
                // 上一次checkpoint点
                logicsMsgTimestamp = DefaultMessageStore.this.getStoreCheckpoint().getLogicsMsgTimestamp();
            }

            // Map<topic, Map<queueId, ConsumeQueue>>
            ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> tables = DefaultMessageStore.this.consumeQueueTable;

            // 刷新consumequeue
            for (ConcurrentMap<Integer, ConsumeQueue> maps : tables.values()) {
                for (ConsumeQueue cq : maps.values()) {
                    boolean result = false;
                    // 如果不成功，继续尝试刷盘
                    for (int i = 0; i < retryTimes && !result; i++) {
                        // 执行刷新
                        result = cq.flush(flushConsumeQueueLeastPages);
                    }
                }
            }

            // 刷新checkpoint
            if (0 == flushConsumeQueueLeastPages) {
                if (logicsMsgTimestamp > 0) {
                    DefaultMessageStore.this.getStoreCheckpoint().setLogicsMsgTimestamp(logicsMsgTimestamp);
                }
                DefaultMessageStore.this.getStoreCheckpoint().flush();
            }
        }

        public void run() {
            DefaultMessageStore.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 刷盘间隔1s
                    int interval = DefaultMessageStore.this.getMessageStoreConfig().getFlushIntervalConsumeQueue();
                    this.waitForRunning(interval);
                    // 执行刷盘，最多执行一次
                    this.doFlush(1);
                } catch (Exception e) {
                    DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            this.doFlush(RETRY_TIMES_OVER);

            DefaultMessageStore.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return FlushConsumeQueueService.class.getSimpleName();
        }

        @Override
        public long getJointime() {
            return 1000 * 60;
        }
    }

    /**
     * 单线程任务
     * 根据commitLog offset将commitLog转发给ConsumeQueue、index
     */
    class ReputMessageService extends ServiceThread {

        // 从commitLog开始拉取的初始偏移量
        private volatile long reputFromOffset = 0;

        public long getReputFromOffset() {
            return reputFromOffset;
        }

        public void setReputFromOffset(long reputFromOffset) {
            this.reputFromOffset = reputFromOffset;
        }

        @Override
        public void shutdown() {
            for (int i = 0; i < 50 && this.isCommitLogAvailable(); i++) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignored) {
                }
            }

            if (this.isCommitLogAvailable()) {
                log.warn("shutdown ReputMessageService, but commitlog have not finish to be dispatched, CL: {} reputFromOffset: {}",
                        DefaultMessageStore.this.commitLog.getMaxOffset(), this.reputFromOffset);
            }

            super.shutdown();
        }

        public long behind() {
            return DefaultMessageStore.this.commitLog.getMaxOffset() - this.reputFromOffset;
        }

        /**
         * commitLog中是否有数据需要刷
         *
         * @return
         */
        private boolean isCommitLogAvailable() {
            return this.reputFromOffset < DefaultMessageStore.this.commitLog.getMaxOffset();
        }

        /**
         * 单线程任务
         */
        private void doReput() {
            for (boolean doNext = true; this.isCommitLogAvailable() && doNext; ) {

                // 判断commitlog的maxOffset是否比上次读取的offset大。 每次处理完读取消息后，都将当前已经处理的最大的offset记录下来，下次处理这个从这个offset开始读取消息
                if (DefaultMessageStore.this.getMessageStoreConfig().isDuplicationEnable()
                        && this.reputFromOffset >= DefaultMessageStore.this.getConfirmOffset()) {
                    break;
                }

                // 返回从reputFromOffset偏移量开始的全部有效数据（commitlog文件）。然后循环读取每一条消息
                SelectMappedBufferResult result = DefaultMessageStore.this.commitLog.getData(reputFromOffset);
                if (result != null) {
                    try {
                        // 开始处理位置
                        this.reputFromOffset = result.getStartOffset();


                        // 从result返回的ByteBuffer中循环读取消息，一次读取一条，创建DispatcherRequest对象，如果消息长度大于0，则调用doDispatch方法，
                        // 最终将分别调用CommitLogDispatcherBuildConsumerQueue、CommitLogDispatcherBuildIndex
                        // result.getSize() 最大可读字节数
                        for (int readSize = 0; readSize < result.getSize() && doNext; ) {
                            // 检查message数据完整性并封装成DispatchRequest。
                            DispatchRequest dispatchRequest =
                                    DefaultMessageStore.this.commitLog.checkMessageAndReturnSize(result.getByteBuffer(), false, false);
                            // 消息总长度
                            int size = dispatchRequest.getMsgSize();

                            // 校验消息成功
                            if (dispatchRequest.isSuccess()) {
                                // 有消息
                                if (size > 0) {
                                    /**
                                     * 分发消息到commitLogDispatcher
                                     *    1.构建索引
                                     *    2.更新ConsumeQueue
                                     */
                                    DefaultMessageStore.this.doDispatch(dispatchRequest);

                                    // 分发消息到MessageArrivingListener，唤醒等待的PullRequest接收消息，OnlyMaster
                                    if (BrokerRole.SLAVE != DefaultMessageStore.this.getMessageStoreConfig().getBrokerRole()
                                            && DefaultMessageStore.this.brokerConfig.isLongPollingEnable()) {
                                        // 有新到的消息达到，通知正在等待的拉取线程
                                        DefaultMessageStore.this.messageArrivingListener.arriving(dispatchRequest.getTopic(),
                                                dispatchRequest.getQueueId(), dispatchRequest.getConsumeQueueOffset() + 1,
                                                dispatchRequest.getTagsCode(), dispatchRequest.getStoreTimestamp(),
                                                dispatchRequest.getBitMap(), dispatchRequest.getPropertiesMap());
                                    }

                                    // 更新offset
                                    this.reputFromOffset += size;
                                    // 先前移动，读取下一条消息
                                    readSize += size;
                                    if (DefaultMessageStore.this.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE) {
                                        // topic下消息增加1  用于统计
                                        DefaultMessageStore.this.storeStatsService
                                                .getSinglePutMessageTopicTimesTotal(dispatchRequest.getTopic()).incrementAndGet();
                                        // topic的消息总大小增加+msgSize
                                        DefaultMessageStore.this.storeStatsService
                                                .getSinglePutMessageTopicSizeTotal(dispatchRequest.getTopic())
                                                .addAndGet(dispatchRequest.getMsgSize());
                                    }
                                }
                                // 读取成功，但是没有消息
                                else if (size == 0) {
                                    // 如果读取到文件结尾，则切换到新文件
                                    this.reputFromOffset = DefaultMessageStore.this.commitLog.rollNextFile(this.reputFromOffset);
                                    readSize = result.getSize();
                                }
                            }
                            // 校验消息出错
                            else if (!dispatchRequest.isSuccess()) {

                                // 解析消息出错，跳过。commitLog文件中消息数据损坏的情况下才会执行到这里
                                if (size > 0) {
                                    log.error("[BUG]read total count not equals msg total size. reputFromOffset={}", reputFromOffset);
                                    this.reputFromOffset += size;
                                } else {
                                    doNext = false;
                                    if (DefaultMessageStore.this.brokerConfig.getBrokerId() == MixAll.MASTER_ID) {
                                        log.error("[BUG]the master dispatch message to consume queue error, COMMITLOG OFFSET: {}",
                                                this.reputFromOffset);

                                        this.reputFromOffset += result.getSize() - readSize;
                                    }
                                }
                            }
                        }
                    } finally {
                        // release对MappedFile的引用
                        result.release();
                    }
                } else {
                    doNext = false;
                }
            }
        }

        @Override
        public void run() {
            DefaultMessageStore.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 每隔1ms执行一次doReput，其实就是实时将消息转发给消息消费队里与索引文件，更新dispatcherPosition，并向服务端即使反馈当前已存储进度
                    Thread.sleep(1);
                    this.doReput();
                } catch (Exception e) {
                    DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            DefaultMessageStore.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return ReputMessageService.class.getSimpleName();
        }

    }
}
