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

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.*;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.ha.HAService;
import org.apache.rocketmq.store.schedule.ScheduleMessageService;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * CommitLog存储，主要是先写入到MappedFile(MappedByteBuffer或者FileChannel中(内存))，
 * 此过程多个线程是串行执行，然后根据不同的磁盘刷写方法进行刷盘操作，主从同步，然后返回，最后返回结果
 *
 * 消息存储到CommitLog文件的过程，大概可以分为三步：
 *    1.消息追加，也就是将消息追加到CommitLog文件对应的内存映射区(这个过程是加锁的，非并发)
 *    2.刷盘阶段(并发)，将内存区数据刷写到磁盘文件(支持同步、异步)
 *    3.主从同步
 *
 *
 *  刷盘机制
 *     1.同步刷盘，每次发送消息，消息都直接存储在FileChannel中，使用的是(MappedByteBuffer),然后直接调用force()
 *     方法刷写磁盘，等到force刷盘成功后，再返回给调用方({@link GroupCommitRequest#waitForFlush(long)}),就是同步调用的实现
 *     2.异步刷盘
 *     2.1是否开启内存缓存池，具体参数为{@link org.apache.rocketmq.store.config.MessageStoreConfig#transientStorePoolEnable = true },
 *     消息在追加时，先放到writeBuffer中，然后定时commit到FileChannel，然后定时flush。
 *     2.2.未开启内存缓存池，直接存入MappedByteBuffer，然后定时flush，
 * Store all metadata downtime for recovery, data protection reliability
 */
public class CommitLog {
    // Message's MAGIC CODE daa320a7
    public final static int MESSAGE_MAGIC_CODE = -626843481;
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    // End of file empty MAGIC CODE cbd43194
    private final static int BLANK_MAGIC_CODE = -875286124;
    private final MappedFileQueue mappedFileQueue;
    private final DefaultMessageStore defaultMessageStore;
    private final FlushCommitLogService flushCommitLogService;

    //If TransientStorePool enabled, we must flush message to FileChannel at fixed periods
    private final FlushCommitLogService commitLogService;

    private final AppendMessageCallback appendMessageCallback;
    private final ThreadLocal<MessageExtBatchEncoder> batchEncoderThreadLocal;
    private HashMap<String/* topic-queueid */, Long/* offset */> topicQueueTable = new HashMap<String, Long>(1024);
    private volatile long confirmOffset = -1L;

    private volatile long beginTimeInLock = 0;
    private final PutMessageLock putMessageLock;

    public CommitLog(final DefaultMessageStore defaultMessageStore) {
        this.mappedFileQueue = new MappedFileQueue(defaultMessageStore.getMessageStoreConfig().getStorePathCommitLog(),
            defaultMessageStore.getMessageStoreConfig().getMapedFileSizeCommitLog(), defaultMessageStore.getAllocateMappedFileService());
        this.defaultMessageStore = defaultMessageStore;

        if (FlushDiskType.SYNC_FLUSH == defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
            this.flushCommitLogService = new GroupCommitService();
        } else {
            this.flushCommitLogService = new FlushRealTimeService();
        }

        this.commitLogService = new CommitRealTimeService();

        this.appendMessageCallback = new DefaultAppendMessageCallback(defaultMessageStore.getMessageStoreConfig().getMaxMessageSize());
        batchEncoderThreadLocal = new ThreadLocal<MessageExtBatchEncoder>() {
            @Override
            protected MessageExtBatchEncoder initialValue() {
                return new MessageExtBatchEncoder(defaultMessageStore.getMessageStoreConfig().getMaxMessageSize());
            }
        };
        this.putMessageLock = defaultMessageStore.getMessageStoreConfig().isUseReentrantLockWhenPutMessage() ? new PutMessageReentrantLock() : new PutMessageSpinLock();

    }

    /**
     * 加载commitlog文件
     *  commitlog文件实际是由一系列的mappedFile文件构成的，所以最终会加载mappedFile
     * @return
     */
    public boolean load() {
        boolean result = this.mappedFileQueue.load();
        log.info("load commit log " + (result ? "OK" : "Failed"));
        return result;
    }

    /**
     * 启动commitLog服务
     */
    public void start() {
        /**
         * 启动刷盘服务,负责将CommitLog的数据flush到磁盘，针对同步刷盘和异步刷盘，有两种实现方式
         * org.apache.rocketmq.store.CommitLog#CommitLog(org.apache.rocketmq.store.DefaultMessageStore)
         */
        this.flushCommitLogService.start();

        if (defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
            this.commitLogService.start();
        }
    }

    public void shutdown() {
        if (defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
            this.commitLogService.shutdown();
        }

        this.flushCommitLogService.shutdown();
    }

    /**
     * commit --> flush
     * @return
     */
    public long flush() {
        this.mappedFileQueue.commit(0);
        this.mappedFileQueue.flush(0);
        // 返回flush到的位置
        return this.mappedFileQueue.getFlushedWhere();
    }

    /**
     * 返回mappedFileQueue的最大offset
     * @return
     */
    public long getMaxOffset() {
        return this.mappedFileQueue.getMaxOffset();
    }

    /**
     * 还有多少数据没有提交
     *   wrotePosition - commitWhere
     * @return
     */
    public long remainHowManyDataToCommit() {
        return this.mappedFileQueue.remainHowManyDataToCommit();
    }

    /**
     * 还有多少数据据没有flush
     *    maxOffset - flushOffset
     * @return
     */
    public long remainHowManyDataToFlush() {
        return this.mappedFileQueue.remainHowManyDataToFlush();
    }

    /**
     * 删除过期文件
     * @param expiredTime 默认72小时
     * @param deleteFilesInterval 两次删除文件的间隔
     * @param intervalForcibly 删除时允许的超时时间
     * @param cleanImmediately
     * @return
     */
    public int deleteExpiredFile(
        final long expiredTime,
        final int deleteFilesInterval,
        final long intervalForcibly,
        final boolean cleanImmediately
    ) {
        return this.mappedFileQueue.deleteExpiredFileByTime(expiredTime, deleteFilesInterval, intervalForcibly, cleanImmediately);
    }

    /**
     * 从offset开始读取数据
     * Read CommitLog data, use data replication
     */
    public SelectMappedBufferResult getData(final long offset) {
        return this.getData(offset, offset == 0);
    }

    /**
     * 根据计算物理文件的大小与准备发送数据的偏移量得到具体要传送消息的位置
     * @param offset
     * @param returnFirstOnNotFound
     * @return
     */
    public SelectMappedBufferResult getData(final long offset, final boolean returnFirstOnNotFound) {
        // 每个文件的大小
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMapedFileSizeCommitLog();
        // 找到当前offset所属的mappedFile
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, returnFirstOnNotFound);
        if (mappedFile != null) {
            // 获取到相对offset 商N是第N+1个文件，余数是相对第N+1个文件的偏移量
            int pos = (int) (offset % mappedFileSize);
            // 截取mappedFile pos到该文件最大可读位置
            SelectMappedBufferResult result = mappedFile.selectMappedBuffer(pos);
            return result;
        }

        return null;
    }

    /**
     * 正常退出，对commitlog恢复
     * When the normal exit, data recovery, all memory data have been flush
     */
    public void recoverNormally() {
        // 在进行文件恢复时查找消息时是否验证CRC
        boolean checkCRCOnRecover = this.defaultMessageStore.getMessageStoreConfig().isCheckCRCOnRecover();
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            // Began to recover from the last third file
            // 3 经验值 从倒数第三个文件开始恢复，如果不足3个，从第一个文件开始恢复
            int index = mappedFiles.size() - 3;
            if (index < 0)
                index = 0;

            MappedFile mappedFile = mappedFiles.get(index);
            // 当前mappedFile对应的offset
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            // 当前mappedFile第一条记录的offset
            long processOffset = mappedFile.getFileFromOffset();
            long mappedFileOffset = 0;
            // 遍历单个mappedFile，当单个mappedFile检查完后接着检查下一个，直到检查完三个文件或者出错
            // 每次去除一条消息，如果查找结果为true并且消息的长度大于0表示消息正确，mappedFileOffset指针向前移动本消息的长度。
            // 如果查找结果为true，并且消息长度等于0，表示已经到该文件的末尾，如果还有下一个文件，则重置processOffset、mappedFileOffset，
            // 从下一个MappedFile开始查找，否则，查找完成，退出循环。
            // 如果查找结果为false，表明该文件未填满所有消息，跳出循环，结束遍历文件
            while (true) {
                // 取出一条消息进行检查
                DispatchRequest dispatchRequest = this.checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover);
                // 消息大小
                int size = dispatchRequest.getMsgSize();
                // Normal data   查找结果为true，并且消息长度大于0
                if (dispatchRequest.isSuccess() && size > 0) {
                    // mappedFileOffset指针向前移动本消息的长度
                    mappedFileOffset += size;
                }

                // 到了当前文件的末尾，切换到下一个文件
                // Come the end of the file, switch to the next file Since the
                // return 0 representatives met last hole,
                // this can not be included in truncate offset
                else if (dispatchRequest.isSuccess() && size == 0) {
                    index++;
                    // 到了最后一个文件
                    if (index >= mappedFiles.size()) {
                        // Current branch can not happen
                        log.info("recover last 3 physics file over, last mapped file " + mappedFile.getFileName());
                        break;
                    }
                    // 检查下一个文件
                    else {
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        log.info("recover next physics file, " + mappedFile.getFileName());
                    }
                }
                // Intermediate file read error 检查过程中出现错误
                else if (!dispatchRequest.isSuccess()) {
                    log.info("recover physics file end, " + mappedFile.getFileName());
                    break;
                }
            }

            // 最后一个文件第一条记录的offset + 检查成功的最后一条消息的offset
            processOffset += mappedFileOffset;
            // 设置mappedFileQueue的刷新位置、提交位置
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            // 删除offset之后的所有文件。
            this.mappedFileQueue.truncateDirtyFiles(processOffset);
        }
    }

    public DispatchRequest checkMessageAndReturnSize(java.nio.ByteBuffer byteBuffer, final boolean checkCRC) {
        return this.checkMessageAndReturnSize(byteBuffer, checkCRC, true);
    }

    private void doNothingForDeadCode(final Object obj) {
        if (obj != null) {
            log.debug(String.valueOf(obj.hashCode()));
        }
    }

    /**
     * 检查数据完整性
     * check the message and returns the message size
     *
     * @return 0 Come the end of the file // >0 Normal messages // -1 Message checksum failure
     */
    public DispatchRequest checkMessageAndReturnSize(java.nio.ByteBuffer byteBuffer, final boolean checkCRC,
        final boolean readBody) {
        try {
            // 1 TOTAL SIZE 整个消息长度，包括所有
            int totalSize = byteBuffer.getInt();

            // 2 MAGIC CODE 魔数
            int magicCode = byteBuffer.getInt();
            switch (magicCode) {
                case MESSAGE_MAGIC_CODE:
                    break;
                case BLANK_MAGIC_CODE:
                    return new DispatchRequest(0, true /* success */);
                default:
                    log.warn("found a illegal magic code 0x" + Integer.toHexString(magicCode));
                    return new DispatchRequest(-1, false /* success */);
            }

            byte[] bytesContent = new byte[totalSize];

            // CRC
            int bodyCRC = byteBuffer.getInt();
            // queueId
            int queueId = byteBuffer.getInt();
            // FLAG
            int flag = byteBuffer.getInt();
            // queueOffset
            long queueOffset = byteBuffer.getLong();
            // physicOffset
            long physicOffset = byteBuffer.getLong();
            // sysFlag
            int sysFlag = byteBuffer.getInt();
            // bornTimestamp
            long bornTimeStamp = byteBuffer.getLong();
            // bornHost 字符串
            ByteBuffer byteBuffer1 = byteBuffer.get(bytesContent, 0, 8);
            // storeTimestamp
            long storeTimestamp = byteBuffer.getLong();
            // storeHostAddress 字符串
            ByteBuffer byteBuffer2 = byteBuffer.get(bytesContent, 0, 8);
            // reconsumeTimes
            int reconsumeTimes = byteBuffer.getInt();

            // preparedTransactionOffset
            long preparedTransactionOffset = byteBuffer.getLong();
            // bodyLength
            int bodyLen = byteBuffer.getInt();
            if (bodyLen > 0) {
                if (readBody) {
                    // 读取body
                    byteBuffer.get(bytesContent, 0, bodyLen);

                    // CRC校验
                    if (checkCRC) {
                        int crc = UtilAll.crc32(bytesContent, 0, bodyLen);
                        if (crc != bodyCRC) {
                            log.warn("CRC check failed. bodyCRC={}, currentCRC={}", crc, bodyCRC);
                            return new DispatchRequest(-1, false/* success */);
                        }
                    }
                } else {
                    byteBuffer.position(byteBuffer.position() + bodyLen);
                }
            }
            // topicLength
            byte topicLen = byteBuffer.get();
            // 读取topic
            byteBuffer.get(bytesContent, 0, topicLen);
            // 拿到topicString
            String topic = new String(bytesContent, 0, topicLen, MessageDecoder.CHARSET_UTF8);

            long tagsCode = 0;
            String keys = "";
            String uniqKey = null;
            // propertiesLength
            short propertiesLength = byteBuffer.getShort();
            Map<String, String> propertiesMap = null;
            if (propertiesLength > 0) {
                // properties
                byteBuffer.get(bytesContent, 0, propertiesLength);
                // propertiesString
                String properties = new String(bytesContent, 0, propertiesLength, MessageDecoder.CHARSET_UTF8);
                // 将propertiesString转换为Map
                propertiesMap = MessageDecoder.string2messageProperties(properties);
                // 获取keys属性
                keys = propertiesMap.get(MessageConst.PROPERTY_KEYS);
                // 获取UniqKey属性
                uniqKey = propertiesMap.get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
                // 获取tags属性
                String tags = propertiesMap.get(MessageConst.PROPERTY_TAGS);
                // 计算tag的hashCode作为tagsCode
                if (tags != null && tags.length() > 0) {
                    tagsCode = MessageExtBrokerInner.tagsString2tagsCode(MessageExt.parseTopicFilterType(sysFlag), tags);
                }

                // Timing message processing
                {
                    // 获取延时等级
                    String t = propertiesMap.get(MessageConst.PROPERTY_DELAY_TIME_LEVEL);
                    // 如果topic是SCHEDULE_TOPIC_XXXX那么说明是延时消息
                    if (ScheduleMessageService.SCHEDULE_TOPIC.equals(topic) && t != null) {
                        int delayLevel = Integer.parseInt(t);

                        // 校正延时消息，大于最大等级的设置为最大等级
                        if (delayLevel > this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel()) {
                            delayLevel = this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel();
                        }
                        // 延时消息到的tagsCode为发送时间
                        if (delayLevel > 0) {
                            tagsCode = this.defaultMessageStore.getScheduleMessageService().computeDeliverTimestamp(delayLevel,
                                storeTimestamp);
                        }
                    }
                }
            }

            // 消息总长度
            int readLength = calMsgLength(bodyLen, topicLen, propertiesLength);
            // 如果消息总长度与计算的长度不相等，读取数据错误
            if (totalSize != readLength) {
                doNothingForDeadCode(reconsumeTimes);
                doNothingForDeadCode(flag);
                doNothingForDeadCode(bornTimeStamp);
                doNothingForDeadCode(byteBuffer1);
                doNothingForDeadCode(byteBuffer2);
                log.error(
                    "[BUG]read total count not equals msg total size. totalSize={}, readTotalCount={}, bodyLen={}, topicLen={}, propertiesLength={}",
                    totalSize, readLength, bodyLen, topicLen, propertiesLength);
                return new DispatchRequest(totalSize, false/* success */);
            }

            return new DispatchRequest(
                topic,
                queueId,
                physicOffset,
                totalSize,
                tagsCode,
                storeTimestamp,
                queueOffset,
                keys,
                uniqKey,
                sysFlag,
                preparedTransactionOffset,
                propertiesMap
            );
        } catch (Exception e) {
        }

        return new DispatchRequest(-1, false /* success */);
    }

    /**
     * 计算消息存储长度 消息存储格式
     * @param bodyLength
     * @param topicLength
     * @param propertiesLength
     * @return
     */
    private static int calMsgLength(int bodyLength, int topicLength, int propertiesLength) {
        final int msgLen = 4 //TOTALSIZE  消息总长度
            + 4 //MAGICCODE
            + 4 //BODYCRC
            + 4 //QUEUEID 消息队列Id
            + 4 //FLAG 标记位
            + 8 //QUEUEOFFSET 消息队列偏移量
            + 8 //PHYSICALOFFSET 物理偏移量
            + 4 //SYSFLAG 系统标记
            + 8 //BORNTIMESTAMP born存储时间戳
            + 8 //BORNHOST broker地址，包含端口
            + 8 //STORETIMESTAMP 存储时间戳
            + 8 //STOREHOSTADDRESS 存储地址，包含端口
            + 4 //RECONSUMETIMES 消息消费重试次数
            + 8 //Prepared Transaction Offset prepare消息偏移量
            + 4 + (bodyLength > 0 ? bodyLength : 0) //BODY 4字节body长度 + 具体的消息内容
            + 1 + topicLength //TOPIC  1字节topic长度域topic内容
            + 2 + (propertiesLength > 0 ? propertiesLength : 0) //propertiesLength 2字节消息属性长度 + 具体的扩展属性
            + 0;
        return msgLen;
    }

    public long getConfirmOffset() {
        return this.confirmOffset;
    }

    public void setConfirmOffset(long phyOffset) {
        this.confirmOffset = phyOffset;
    }

    /**
     * 上次异常关闭，文件恢复
     * 异常文件恢复的步骤与正常停止文件恢复的流程基本相同，主要差别有两点：
     *   1.正常停止默认从倒数第三个文件开始进行恢复，而异常停止则需要从最后一个文件往前走，直到找到第一个消息存储正常的文件
     *   2.如果commitlog目录没有消息文件，那么消息队列目录下存储的文件需要销毁
     */
    public void recoverAbnormally() {
        // recover by the minimum time stamp
        boolean checkCRCOnRecover = this.defaultMessageStore.getMessageStoreConfig().isCheckCRCOnRecover();
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            // Looking beginning to recover from which file
            // 从最后一个文件开始检查
            int index = mappedFiles.size() - 1;
            MappedFile mappedFile = null;
            for (; index >= 0; index--) {
                mappedFile = mappedFiles.get(index);
                // 从最后一个文件开始检测，先找到第一个正常的commitLog文件，然后从该文件开始恢复
                if (this.isMappedFileMatchedRecover(mappedFile)) {
                    log.info("recover from this mapped file " + mappedFile.getFileName());
                    // 找到正常的commitlog文件，跳出循环，开始从index文件查找
                    break;
                }
            }

            // 如果index小于0，则从第一个mappedFile开始检查
            if (index < 0) {
                index = 0;
                mappedFile = mappedFiles.get(index);
            }

            // 逐条开始检查mappedFile中数据

            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            long processOffset = mappedFile.getFileFromOffset();
            long mappedFileOffset = 0;
            while (true) {
                // 创建转发对象
                DispatchRequest dispatchRequest = this.checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover);
                int size = dispatchRequest.getMsgSize();

                // Normal data
                // 消息合法，将消息重新转发到消息消费队列与索引文件
                if (size > 0) {
                    mappedFileOffset += size;

                    // 转发给对象，会同步更新commitqueue、index文件
                    if (this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
                        if (dispatchRequest.getCommitLogOffset() < this.defaultMessageStore.getConfirmOffset()) {
                            this.defaultMessageStore.doDispatch(dispatchRequest);
                        }
                    } else {
                        this.defaultMessageStore.doDispatch(dispatchRequest);
                    }
                }
                // Intermediate file read error
                else if (size == -1) {
                    log.info("recover physics file end, " + mappedFile.getFileName());
                    break;
                }
                // Come the end of the file, switch to the next file
                // Since the return 0 representatives met last hole, this can
                // not be included in truncate offset
                else if (size == 0) {
                    index++;
                    if (index >= mappedFiles.size()) {
                        // The current branch under normal circumstances should
                        // not happen
                        log.info("recover physics file over, last mapped file " + mappedFile.getFileName());
                        break;
                    } else {
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        log.info("recover next physics file, " + mappedFile.getFileName());
                    }
                }
            }

            processOffset += mappedFileOffset;
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            this.mappedFileQueue.truncateDirtyFiles(processOffset);

            // Clear ConsumeQueue redundant data
            this.defaultMessageStore.truncateDirtyLogicFiles(processOffset);
        }
        // Commitlog case files are deleted
        // 如果未找到有效的MappedFile，则设置commitlog目录的flushedWhere、committedWhere指针都为0，并销毁消息消费队列文件
        else {
            this.mappedFileQueue.setFlushedWhere(0);
            this.mappedFileQueue.setCommittedWhere(0);
            this.defaultMessageStore.destroyLogics();
        }
    }

    /**
     * 查找正常的文件
     *    1.魔数正确
     *    2.消息存储时间不为0
     *    3.存储时间小于等于检测时间点
     * 三个条件都满足即为正常文件
     * @param mappedFile
     * @return
     */
    private boolean isMappedFileMatchedRecover(final MappedFile mappedFile) {
        ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();

        // 魔数正确
        int magicCode = byteBuffer.getInt(MessageDecoder.MESSAGE_MAGIC_CODE_POSTION);
        if (magicCode != MESSAGE_MAGIC_CODE) {
            return false;
        }

        // 消息存储时间不为0
        long storeTimestamp = byteBuffer.getLong(MessageDecoder.MESSAGE_STORE_TIMESTAMP_POSTION);
        if (0 == storeTimestamp) {
            return false;
        }

        // 存储时间小于等于检测点
        if (this.defaultMessageStore.getMessageStoreConfig().isMessageIndexEnable()
            && this.defaultMessageStore.getMessageStoreConfig().isMessageIndexSafe()) {
            if (storeTimestamp <= this.defaultMessageStore.getStoreCheckpoint().getMinTimestampIndex()) {
                log.info("find check timestamp, {} {}",
                    storeTimestamp,
                    UtilAll.timeMillisToHumanString(storeTimestamp));
                return true;
            }
        } else {
            if (storeTimestamp <= this.defaultMessageStore.getStoreCheckpoint().getMinTimestamp()) {
                log.info("find check timestamp, {} {}",
                    storeTimestamp,
                    UtilAll.timeMillisToHumanString(storeTimestamp));
                return true;
            }
        }

        return false;
    }

    private void notifyMessageArriving() {

    }

    public boolean resetOffset(long offset) {
        return this.mappedFileQueue.resetOffset(offset);
    }

    public long getBeginTimeInLock() {
        return beginTimeInLock;
    }

    /**
     * commit log保存消息
     * broker处理延时消息
     * broker收到延时消息和正常消息在前置处理的流程是一致的，对于延时消息的特殊处理体现在将消息写入存储的时候
     *
     * 延时topic org.apache.rocketmq.store.schedule.ScheduleMessageService#SCHEDULE_TOPIC=SCHEDULE_TOPIC_XXXX
     * 这个topic是一个特殊的topic，和正常的topic不同的地方是：
     *    1.不会创建TopicConfig，因此也不需要consumer直接消费这个topic下的消息
     *    2.不会将这个topic注册到namesrv
     *    3.这个topic的队列个数和延时等级的个数是相同的
     *
     * 主要点：
     *   1.所有的消息在存储时都是按顺序存在一起的，不会按照topic和queueId做物理隔离
     *   2.每条消息存储时都会有一个offset，通过offset是定位到消息位置并获取消息详情的唯一办法，所有的消息查询操作最终会转化成通过offset查询消息详情
     *   3.每条消息存储前都会产生一个messageId，这个MessageId可以快速的得到消息存储的broker和他在commitLog中的offset
     *   4.Broker收到消息后的处理线程只负责消息存储，不负责通知consumer或者其他逻辑，最大化消息吞吐量
     *   5.Broker返回成功不代表消息已经写入磁盘，如果对消息的可靠性要求高的话，可以讲FlushDiskType设置成SYNC_FLUSH，装每次收到消息写入文件后都会flush操作。
     * @param msg
     * @return
     */
    public PutMessageResult putMessage(final MessageExtBrokerInner msg) {
        // Set the storage time
        msg.setStoreTimestamp(System.currentTimeMillis());
        // Set the message body BODY CRC (consider the most appropriate setting
        // on the client)
        msg.setBodyCRC(UtilAll.crc32(msg.getBody()));
        // Back to Results
        AppendMessageResult result = null;

        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

        // 拿到原始topic和对应的queueId
        String topic = msg.getTopic();
        int queueId = msg.getQueueId();

        final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());
        // 非事务消息和事务的commit消息才会进一步判断delayLevel
        if (tranType == MessageSysFlag.TRANSACTION_NOT_TYPE
            || tranType == MessageSysFlag.TRANSACTION_COMMIT_TYPE) {
            // Delay Delivery
            /**
             * 延迟消息（非延迟消息的DelayTimeLevel为0）
             * 所有的延迟消息都投入 SCHEDULE_TOPIC="SCHEDULE_TOPIC_XXX" topic下，共有18个延迟，每个延迟对应一个队列，即18个队列
             *     private String messageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h";
             * 在消息存入commitlog之前，如果发现延时level大于0，会将消息的主题设置为SCHEDULE_TOPIC="SCHEDULE_TOPIC_XXX",
             * 然后备份原主题名称，也就是说，延时消息统一由ScheduleMessageService来处理
             */
            if (msg.getDelayTimeLevel() > 0) {
                // 纠正设置过大的level，就是delayLevel设置都大于延时时间等级的最大等级  maxDelayLevel = 18
                if (msg.getDelayTimeLevel() > this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel()) {
                    msg.setDelayTimeLevel(this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel());
                }

                // 设置为延时队里的topic SCHEDULE_TOPIC_XXXX
                topic = ScheduleMessageService.SCHEDULE_TOPIC;
                // 每个延时等级一个queue，queueId = delayLevel - 1
                queueId = ScheduleMessageService.delayLevel2QueueId(msg.getDelayTimeLevel());

                // Backup real topic, queueId
                // 备份原始的topic和queueId
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_TOPIC, msg.getTopic());
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_QUEUE_ID, String.valueOf(msg.getQueueId()));
                // 更新properties
                msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));

                msg.setTopic(topic);
                msg.setQueueId(queueId);
            }
        }

        long eclipseTimeInLock = 0;
        MappedFile unlockMappedFile = null;
        // 获取当前正在写入文件。commitLog是由连续的MappedFile的列表组成的，同一时间，只有最后一个MappedFile有写入，因为之前的文件都已经写满了，所以这里是最后一个
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        // 获取写Message的锁
        putMessageLock.lock(); //spin or ReentrantLock ,depending on store config
        try {
            long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
            // 记录lock time
            this.beginTimeInLock = beginLockTimestamp;

            // Here settings are stored timestamp, in order to ensure an orderly
            // global 设置消息存储时间
            msg.setStoreTimestamp(beginLockTimestamp);

            // 创建一个MappedFile，如果文件不存在(commitLog第一次启动)或者文件已经写满
            if (null == mappedFile || mappedFile.isFull()) {
                mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
            }
            if (null == mappedFile) {
                // 文件创建失败，返回错误
                log.error("create mapped file1 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                beginTimeInLock = 0;
                return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, null);
            }

            // 消息写文件,这里会传入一个callback参数，真正的写入实在callback中实现的
            result = mappedFile.appendMessage(msg, this.appendMessageCallback);
            switch (result.getStatus()) {
                case PUT_OK:
                    break;
                case END_OF_FILE:
                    /**
                     *  如果文件已满，则新建一个文件继续
                     *  在上一步中的appendMessage()方法中，如果文件剩余的空间已经不足以写下这条消息，则会用一个EOF消息补齐文件，
                     *  然后返回EOF错误，收到这个错误时，会新建一个文件，然后重写一次。
                     */
                    unlockMappedFile = mappedFile;
                    // Create a new file, re-write the message
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                    if (null == mappedFile) {
                        // XXX: warn and notify me
                        log.error("create mapped file2 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                        beginTimeInLock = 0;
                        return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, result);
                    }
                    result = mappedFile.appendMessage(msg, this.appendMessageCallback);
                    break;
                case MESSAGE_SIZE_EXCEEDED:
                case PROPERTIES_SIZE_EXCEEDED:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result);
                case UNKNOWN_ERROR:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
                default:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
            }

            eclipseTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
            beginTimeInLock = 0;
        } finally {
            // 释放锁
            putMessageLock.unlock();
        }

        // 写消息时间过长
        if (eclipseTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessage in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", eclipseTimeInLock, msg.getBody().length, result);
        }

        // unlock已经写满的文件，释放内存锁
        if (null != unlockMappedFile && this.defaultMessageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
            this.defaultMessageStore.unlockMappedFile(unlockMappedFile);
        }

        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // Statistics
        // topic消息进来消息总数
        storeStatsService.getSinglePutMessageTopicTimesTotal(msg.getTopic()).incrementAndGet();
        // topic总字节数
        storeStatsService.getSinglePutMessageTopicSizeTotal(topic).addAndGet(result.getWroteBytes());

        /**
         * flush数据到磁盘，分异步和同步
         * 用户可以使用MessageStore的FlushDiskType参数来控制数据flush到磁盘的方式，
         * 如果参数值SYNC_FLUSH，则每次写完消息都会做一次flush，完成才会返回结果。
         * 如果是ASYNC_FLUSH，只会唤醒flushCommitLogService，由它异步去检查是否做flush
         */
        handleDiskFlush(result, putMessageResult, msg);
        /**
         * 如果broker设置成SYNC_MASTER，则等Slave接收到数据后才返回(接收到数据是指offset延后没有超过指定的字节数)
         * broker的主从数据同步也是可以有两种方式
         * 如果是SYNC_MASTER，则master保存消息之后，需要将消息同步到slave之后才会返回结果
         * 如果是ASYNC_MASTER，这里不会做任何操作，由HAService的后台线程做同步操作
         */
        handleHA(result, putMessageResult, msg);

        return putMessageResult;
    }

    /**
     * 刷盘
     * RocketMQ的存储于读写是基于JDK NIO的内存映射机制MappedByteBuffer的，消息存储时首先将消息追加到内存中，再根据配置的刷盘策略在不同时间进行刷写磁盘。
     * 如果是同步刷盘，消息追加到内存之后，将同步调用{@link MappedByteBuffer#force()}方法
     * 如果是异步刷盘，在消息追加到内存之后立即返回给消息发送端，
     * RocketMQ使用一个单独的线程按照某一个设置的频率执行刷盘操作。通过在broker配置文件中配置flushDiskType来设定刷盘方式，可选值为ASYNC_FLUSH、SYNC_FLUSH，默认为异步刷盘。
     *
     *
     * @param result 写入到MappedFile(内存映射文件中、ByteBuffer中)的结果
     * @param putMessageResult
     * @param messageExt
     */
    public void handleDiskFlush(AppendMessageResult result, PutMessageResult putMessageResult, MessageExt messageExt) {
        // Synchronization flush
        // 同步刷盘，指的是在消息追加到内存映射文件的内存中后，立即将数据从内存刷写到磁盘文件
        if (FlushDiskType.SYNC_FLUSH == this.defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
            final GroupCommitService service = (GroupCommitService) this.flushCommitLogService;
            /**
             * 如果消息属性WAIT为ture
             * 是否等待服务器将消息存储完毕再返回(等待刷盘完成)
             */
            if (messageExt.isWaitStoreMsgOK()) {
                // 1、构建GroupCommitRequest同步任务并提交到GroupCommitRequest
                // 2、等待同步刷盘任务完成，如果超时返回刷盘错误，刷盘成功后正常返回给调用方
                // 创建刷盘任务
                GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes());
                // 添加刷盘任务 将任务放入写队列立即返回
                service.putRequest(request);
                // 进入等待 超时5s org.apache.rocketmq.store.CommitLog.GroupCommitService.doCommit 执行完之后会唤醒本线程
                // 消息发送线程将消息追加到内存映射文件之后，将同步任务GroupCommitRequest提交到GroupCommitService线程，然后调用阻塞等待刷盘结果，超时默认5s
                boolean flushOK = request.waitForFlush(this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout());
                if (!flushOK) {
                    log.error("do groupcommit, wait for flush failed, topic: " + messageExt.getTopic() + " tags: " + messageExt.getTags()
                        + " client address: " + messageExt.getBornHostString());
                    putMessageResult.setPutMessageStatus(PutMessageStatus.FLUSH_DISK_TIMEOUT);
                }
            } else {
                service.wakeup();
            }
        }
        // Asynchronous flush 异步刷盘
        else {
            /**
             * 根据是否开启transientStorePoolEnable机制，刷盘实现会有一定的差别。
             * 如果transientStorePoolEnable=true，RocketMQ会单独申请一个与目标文件(commitlog)同样大小的堆外内存，该堆外内存将使用内存锁定，确保不会被置换到虚拟内存中，
             * 消息首先会追加到堆外内存，然后提交到物理文件的内存映射内存中，再flush到磁盘。
             * 如果transientStorePoolEnable=false，消息直接追加到与物理文件直接映射的内存中，然后flush到磁盘
             *
             * transientStorePoolEnable = true的磁盘刷写流程：
             * 1、首先将消息直接追加到ByteBuffer（堆外内存DirectByteBuffer），wrotePosition随着消息的不断追加向后移动
             * 2、CommitRealTimeService线程默认每200ms将ByteBuffer新追加的内容(wrotePosition - committedPosition)的数据提交到MappedByteBuffer中
             * 3、MappedByteBuffer在内存中追加提交的内容，wrotePosition指针向前移动，然后返回
             * 4、commit操作成功返回，将committedPosition向前移动本次提交的内容长度，此时wrotePosition指针已然可以向前推进
             * 5、FlushRealTimeService线程默认每500ms将MappedByteBuffer中新追加的内存(wrotePosition - flushedPosition)通过调用{@link MappedByteBuffer#force()}方法将数据刷写到磁盘
             */
            if (!this.defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
                flushCommitLogService.wakeup();
            } else {
                commitLogService.wakeup();
            }
        }
    }

    /**
     * SYNC_MASTER同步数据到slave
     * SYNC_MASTER和ASYNC_MASTER传输数据到slave的过程是一致的，只是时机不一样。SYNC_MASTER接收到producer发送来的消息时候，会同步等待消息也传输到slave
     *    1.master将需要传输到slave到的数据构造为GroupCommitRequest交给GroupTransferService
     *    2.唤醒传输数据的线程(如果没有更多数据需要传输的时候，HAClient.run方法会等待新的消息)
     *    3.等待当前的传输请求完成
     *
     *  在HAService启动的时候会启动GroupTransferService线程，GroupTransferService并没有真正的传输数据，传数据还是HAConnection。
     *  GroupTransferService只是将push2SlaveMaxOffset和需要传输到slave的消息的offset比较，如果 push2SlaveMaxOffset > req.getNextOffset() 则说明slave已经收到该消息，
     *  这个时候会通知request，消息已经传输完成
     *  push2SlaveMaxOffset这个字段表示当前slave收到消息的最大的offset，每次master收到slave的ACK之后会更新这个值。
     * @param result
     * @param putMessageResult
     * @param messageExt
     */
    public void handleHA(AppendMessageResult result, PutMessageResult putMessageResult, MessageExt messageExt) {
        // 如果是SYNC_MASTER，马上将信息同步到slave
        if (BrokerRole.SYNC_MASTER == this.defaultMessageStore.getMessageStoreConfig().getBrokerRole()) {
            HAService service = this.defaultMessageStore.getHaService();
            /**
             * 如果消息属性WAIT为true
             * 是否等待服务器将消息存储完毕再返回(可能是等待刷盘完成或者等待同步复制到其他服务器完成)
             */
            if (messageExt.isWaitStoreMsgOK()) {
                // Determine whether to wait
                /**
                 * 存在slave并且slave不能落后master太多
                 * isSlaveOK满足如下两个条件
                 *     1.slave和master的进度相差小于256M，则认为正常
                 *     2.slave连接数大于0，则认为正常
                 *     getWroteOffset() 从什么位置开始写   getWroteBytes() 写多少字节
                 */
                if (service.isSlaveOK(result.getWroteOffset() + result.getWroteBytes())) {
                    // groupCommitRequest中的offset代表了当时写commitLog之后commitLog offset的位置
                    GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes());
                    // 最终将会调用GroupTransferService的putMessage方法，即doWaitTransfer方法的那个集合
                    service.putRequest(request);
                    // 唤醒可能等待新的消息数据到的传输数据线程  唤醒WriteSocketService
                    service.getWaitNotifyObject().wakeupAll();
                    // 等待当前的消息被传输到slave，等待slave收到该消息的确认后则flushOK=true
                    boolean flushOK =
                        request.waitForFlush(this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout());
                    if (!flushOK) {
                        log.error("do sync transfer other node, wait return, but failed, topic: " + messageExt.getTopic() + " tags: "
                            + messageExt.getTags() + " client address: " + messageExt.getBornHostNameString());
                        putMessageResult.setPutMessageStatus(PutMessageStatus.FLUSH_SLAVE_TIMEOUT);
                    }
                }
                // Slave problem
                else {
                    // Tell the producer, slave not available
                    /**
                     * 如果没有Slave的情况下，还配置了SYNC_MASTERd的模式
                     * 那么isSlaveOK中的第二个条件失败，producer发送消息就会一直报这个错
                     */
                    putMessageResult.setPutMessageStatus(PutMessageStatus.SLAVE_NOT_AVAILABLE);
                }
            }
        }

    }

    /**
     * 保存多条消息
     * @param messageExtBatch
     * @return
     */
    public PutMessageResult putMessages(final MessageExtBatch messageExtBatch) {
        // 设置消息存储时间为当前时间
        messageExtBatch.setStoreTimestamp(System.currentTimeMillis());
        AppendMessageResult result;

        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

        // 如果msg是非事务消息，那么结果为0
        final int tranType = MessageSysFlag.getTransactionValue(messageExtBatch.getSysFlag());

        // 不处理事务消息
        if (tranType != MessageSysFlag.TRANSACTION_NOT_TYPE) {
            // 事务消息直接返回
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }
        // 不处理延迟消息
        if (messageExtBatch.getDelayTimeLevel() > 0) {
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        long eclipseTimeInLock = 0;
        MappedFile unlockMappedFile = null;
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

        //fine-grained lock instead of the coarse-grained
        MessageExtBatchEncoder batchEncoder = batchEncoderThreadLocal.get();

        messageExtBatch.setEncodedBuff(batchEncoder.encode(messageExtBatch));

        putMessageLock.lock();
        try {
            long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
            this.beginTimeInLock = beginLockTimestamp;

            // Here settings are stored timestamp, in order to ensure an orderly
            // global
            messageExtBatch.setStoreTimestamp(beginLockTimestamp);

            if (null == mappedFile || mappedFile.isFull()) {
                mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
            }
            if (null == mappedFile) {
                log.error("Create mapped file1 error, topic: {} clientAddr: {}", messageExtBatch.getTopic(), messageExtBatch.getBornHostString());
                beginTimeInLock = 0;
                return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, null);
            }

            // 写入消息
            result = mappedFile.appendMessages(messageExtBatch, this.appendMessageCallback);
            switch (result.getStatus()) {
                // 写入成功
                case PUT_OK:
                    break;
                // 文件剩余空间不够
                case END_OF_FILE:
                    unlockMappedFile = mappedFile;
                    // Create a new file, re-write the message
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                    if (null == mappedFile) {
                        // XXX: warn and notify me
                        log.error("Create mapped file2 error, topic: {} clientAddr: {}", messageExtBatch.getTopic(), messageExtBatch.getBornHostString());
                        beginTimeInLock = 0;
                        return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, result);
                    }
                    // 重新写入
                    result = mappedFile.appendMessages(messageExtBatch, this.appendMessageCallback);
                    break;
                case MESSAGE_SIZE_EXCEEDED:
                case PROPERTIES_SIZE_EXCEEDED:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result);
                case UNKNOWN_ERROR:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
                default:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
            }

            // 记录写消息的耗时
            eclipseTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
            beginTimeInLock = 0;
        } finally {
            putMessageLock.unlock();
        }

        // 耗时超过500ms，打印日志
        if (eclipseTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessages in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", eclipseTimeInLock, messageExtBatch.getBody().length, result);
        }

        if (null != unlockMappedFile && this.defaultMessageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
            this.defaultMessageStore.unlockMappedFile(unlockMappedFile);
        }

        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // Statistics
        storeStatsService.getSinglePutMessageTopicTimesTotal(messageExtBatch.getTopic()).addAndGet(result.getMsgNum());
        storeStatsService.getSinglePutMessageTopicSizeTotal(messageExtBatch.getTopic()).addAndGet(result.getWroteBytes());

        // 执行刷盘
        handleDiskFlush(result, putMessageResult, messageExtBatch);

        // 同步到slave
        handleHA(result, putMessageResult, messageExtBatch);

        return putMessageResult;
    }

    /**
     * 获取消息的存储时间
     * According to receive certain message or offset storage time if an error occurs, it returns -1
     */
    public long pickupStoreTimestamp(final long offset, final int size) {
        if (offset >= this.getMinOffset()) {
            SelectMappedBufferResult result = this.getMessage(offset, size);
            if (null != result) {
                try {
                    return result.getByteBuffer().getLong(MessageDecoder.MESSAGE_STORE_TIMESTAMP_POSTION);
                } finally {
                    result.release();
                }
            }
        }

        return -1;
    }

    /**
     * 获取最小offset
     * @return
     */
    public long getMinOffset() {
        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        if (mappedFile != null) {
            if (mappedFile.isAvailable()) {
                return mappedFile.getFileFromOffset();
            } else {
                return this.rollNextFile(mappedFile.getFileFromOffset());
            }
        }

        return -1;
    }

    /**
     * 根据offset找到所在的commitLog文件，commitLog文件封装成MappedFile(内存映射文件)，
     * 然后直接从相对offset开始，读取指定的字节(消息的长度，从相对offset开始先读取4个字节，就能获取到该消息的从长度)
     * @param offset
     * @param size
     * @return
     */
    public SelectMappedBufferResult getMessage(final long offset, final int size) {
        // 每个MappedFile的大小 1G
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMapedFileSizeCommitLog();
        // 根据绝对offset找到对应的MappedFile 如果offset等于0返回第一个
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, offset == 0);
        if (mappedFile != null) {
            // pos：相对offset
            int pos = (int) (offset % mappedFileSize);
            return mappedFile.selectMappedBuffer(pos, size);
        }
        return null;
    }

    /**
     * 切换到下一页
     * @param offset
     * @return
     */
    public long rollNextFile(final long offset) {
        // 每个mappedFile文件大小 1G
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMapedFileSizeCommitLog();
        return offset + mappedFileSize - offset % mappedFileSize;
    }

    public HashMap<String, Long> getTopicQueueTable() {
        return topicQueueTable;
    }

    public void setTopicQueueTable(HashMap<String, Long> topicQueueTable) {
        this.topicQueueTable = topicQueueTable;
    }

    public void destroy() {
        this.mappedFileQueue.destroy();
    }

    /**
     * 将具体的消息写入commitLog
     * @param startOffset
     * @param data
     * @return
     */
    public boolean appendData(long startOffset, byte[] data) {
        putMessageLock.lock();
        try {
            // 根据startOffset得到具体的mappedFile，再向其中写数据。
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(startOffset);
            if (null == mappedFile) {
                log.error("appendData getLastMappedFile error  " + startOffset);
                return false;
            }

            return mappedFile.appendMessage(data);
        } finally {
            putMessageLock.unlock();
        }
    }

    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        return this.mappedFileQueue.retryDeleteFirstFile(intervalForcibly);
    }

    public void removeQueueFromTopicQueueTable(final String topic, final int queueId) {
        String key = topic + "-" + queueId;
        synchronized (this) {
            this.topicQueueTable.remove(key);
        }

        log.info("removeQueueFromTopicQueueTable OK Topic: {} QueueId: {}", topic, queueId);
    }

    /**
     * commitLog文件自检，前一个文件与后一个文件第一条消息的offset差要等于一个完整文件大小
     */
    public void checkSelf() {
        mappedFileQueue.checkSelf();
    }

    public long lockTimeMills() {
        long diff = 0;
        long begin = this.beginTimeInLock;
        if (begin > 0) {
            diff = this.defaultMessageStore.now() - begin;
        }

        if (diff < 0) {
            diff = 0;
        }

        return diff;
    }

    /**
     * 子类
     *   CommitRealTimeService
     *   FlushRealTimeService
     *   GroupCommitService
     */
    abstract class FlushCommitLogService extends ServiceThread {
        protected static final int RETRY_TIMES_OVER = 10;
    }


    /**
     * 提交
     */
    class CommitRealTimeService extends FlushCommitLogService {

        private long lastCommitTimestamp = 0;

        @Override
        public String getServiceName() {
            return CommitRealTimeService.class.getSimpleName();
        }

        @Override
        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");
            while (!this.isStopped()) {
                // 线程循环间隔，默认200ms  即默认每200ms对堆外内存中数据做一次commit
                int interval = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitIntervalCommitLog();
                // 每次提交到文件中，至少需要多少个页，默认4页
                int commitDataLeastPages = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitCommitLogLeastPages();

                // 两次真实提交最大间隔，默认200ms
                int commitDataThoroughInterval =
                    CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitCommitLogThoroughInterval();

                long begin = System.currentTimeMillis();
                if (begin >= (this.lastCommitTimestamp + commitDataThoroughInterval)) {
                    this.lastCommitTimestamp = begin;
                    // 如果提交时间差已经超过了两次真实提交最大间隔，那么将忽略最小提交页数限制
                    commitDataLeastPages = 0;
                }

                try {
                    // 提交。将待提交数据提交到物理文件的存储映射内存映射内存区，如果返回false，并不代表提交失败，而是只提交了一部分数据
                    boolean result = CommitLog.this.mappedFileQueue.commit(commitDataLeastPages);
                    long end = System.currentTimeMillis();
                    if (!result) {
                        this.lastCommitTimestamp = end; // result = false means some data committed.
                        //now wake up flush thread. 唤醒刷盘线程执行刷盘操作，该线程每完成一次提交动作，将等待200ms再继续执行下一次提交任务
                        flushCommitLogService.wakeup();
                    }

                    if (end - begin > 500) {
                        log.info("Commit data to file costs {} ms", end - begin);
                    }
                    this.waitForRunning(interval);
                } catch (Throwable e) {
                    CommitLog.log.error(this.getServiceName() + " service has exception. ", e);
                }
            }

            boolean result = false;
            for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
                result = CommitLog.this.mappedFileQueue.commit(0);
                CommitLog.log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
            }
            CommitLog.log.info(this.getServiceName() + " service end");
        }
    }

    /**
     * 刷盘
     */
    class FlushRealTimeService extends FlushCommitLogService {
        private long lastFlushTimestamp = 0;
        private long printTimes = 0;

        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                /**
                 * flushCommitLogTimed默认为false，表示使用await方法进行等待，如果为true，表示使用Thread.sleep进行等待
                 */
                boolean flushCommitLogTimed = CommitLog.this.defaultMessageStore.getMessageStoreConfig().isFlushCommitLogTimed();

                /**
                 * flushIntervalCommitLog:FlushRealTimeService线程任务运行间隔
                 * 刷盘间隔时间，异步刷新线程，每次处理完一批任务后的等待时间，默认500ms
                 */
                int interval = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushIntervalCommitLog();
                // 每次刷写到磁盘(commitLog)，至少需要多个少个页，默认4页
                int flushPhysicQueueLeastPages = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushCommitLogLeastPages();

                /**
                 * 两次真实刷写任务最大间隔，默认10s
                 * 如果上次刷新时间+该值 小于 当前时间，则改变flushPhysicQueueLeastPages = 0,
                 * 并每10次输出刷盘进度
                 */
                int flushPhysicQueueThoroughInterval =
                    CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushCommitLogThoroughInterval();

                boolean printFlushProgress = false;

                // Print flush progress 如果距离上次提交间隔超过了flushPhysicQueueThoroughInterval，则本次刷盘任务将忽略flushPhysicQueueLeastPages，
                // 也就是如果待刷写数据小于指定页数也执行刷写磁盘操作
                long currentTimeMillis = System.currentTimeMillis();
                if (currentTimeMillis >= (this.lastFlushTimestamp + flushPhysicQueueThoroughInterval)) {
                    this.lastFlushTimestamp = currentTimeMillis;
                    flushPhysicQueueLeastPages = 0;
                    printFlushProgress = (printTimes++ % 10) == 0;
                }

                try {
                    // 指定等待方式。flushCommitLogTimed为true使用Thread.sleep(), false使用waitForRun
                    if (flushCommitLogTimed) {
                        Thread.sleep(interval);
                    } else {
                        this.waitForRunning(interval);
                    }

                    if (printFlushProgress) {
                        this.printFlushProgress();
                    }

                    long begin = System.currentTimeMillis();
                    // 调用刷盘任务，将内存中数据刷写到磁盘，
                    CommitLog.this.mappedFileQueue.flush(flushPhysicQueueLeastPages);
                    long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
                    if (storeTimestamp > 0) {
                        // 设置检测点StoreCheckPoint的physicMsgTimestamp(commitLog文件的检测点，也就是记录最新刷盘时间的时间戳)
                        // 文件检测点文件（checkpoint）的刷盘动作在刷盘消息消费队列线程中执行，入口在DefaultMessageStore#FlushConsumeQueueService
                        CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                    }
                    long past = System.currentTimeMillis() - begin;
                    if (past > 500) {
                        log.info("Flush data to disk costs {} ms", past);
                    }
                } catch (Throwable e) {
                    CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                    this.printFlushProgress();
                }
            }

            // Normal shutdown, to ensure that all the flush before exit
            boolean result = false;
            for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
                result = CommitLog.this.mappedFileQueue.flush(0);
                CommitLog.log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
            }

            this.printFlushProgress();

            CommitLog.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return FlushRealTimeService.class.getSimpleName();
        }

        private void printFlushProgress() {
            // CommitLog.log.info("how much disk fall behind memory, "
            // + CommitLog.this.mappedFileQueue.howMuchFallBehind());
        }

        @Override
        public long getJointime() {
            return 1000 * 60 * 5;
        }
    }

    public static class GroupCommitRequest {
        // 刷盘点偏移量
        private final long nextOffset;
        // 倒计数锁存器
        private final CountDownLatch countDownLatch = new CountDownLatch(1);
        // 刷盘结果，初始为false
        private volatile boolean flushOK = false;

        public GroupCommitRequest(long nextOffset) {
            this.nextOffset = nextOffset;
        }

        public long getNextOffset() {
            return nextOffset;
        }

        /**
         * 同步完成，唤醒等待的线程
         * @param flushOK
         */
        public void wakeupCustomer(final boolean flushOK) {
            this.flushOK = flushOK;
            this.countDownLatch.countDown();
        }

        /**
         * 等待主从同步完成
         * 在 org.apache.rocketmq.store.ha.HAService.GroupTransferService#doWaitTransfer() 调用
         * org.apache.rocketmq.store.CommitLog.GroupCommitRequest#wakeupCustomer(boolean) 被唤醒
         * @param timeout
         * @return
         */
        public boolean waitForFlush(long timeout) {
            try {
                // 这里会进入等待
                this.countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
                return this.flushOK;
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
                return false;
            }
        }
    }

    /**
     * 同步刷写服务，一个线程一直的处理同步刷写任务，每处理一个循环后等待10ms，一旦新任务到达，立即唤醒执行任务
     * GroupCommit Service
     */
    class GroupCommitService extends FlushCommitLogService {
        // 写队列，主要用于向该线程添加刷盘任务
        private volatile List<GroupCommitRequest> requestsWrite = new ArrayList<GroupCommitRequest>();
        // 读队列，主要用于执行特定的刷盘任务，这是GroupCommitService设计的一个亮点，，把读写分离，每次处理完requestRead中的任务，就交换两个队列
        private volatile List<GroupCommitRequest> requestsRead = new ArrayList<GroupCommitRequest>();

        /**
         * 添加刷盘任务
         * 客户端提交同步刷盘任务到GroupCommitService线程，如果该线程处于等待状态则将其唤醒
         * @param request
         */
        public synchronized void putRequest(final GroupCommitRequest request) {
            synchronized (this.requestsWrite) {
                this.requestsWrite.add(request);
            }
            if (hasNotified.compareAndSet(false, true)) {
            }
            waitPoint.countDown(); // notify
        }

        /**
         * 交换读写队列
         * 由于避免同步刷盘消费任务与其他消息生产者提交任务直接的锁竞争，GroupCommitService提供了读容器与写容器，
         * 这两个容器每执行完一次任务之后，交换，继续消费任务
         */
        private void swapRequests() {
            List<GroupCommitRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        }

        /**
         * 执行刷盘操作，调用{@link MappedByteBuffer#force()}
         * 1、遍历同步刷盘任务列表，根据加入顺序逐一执行刷盘逻辑
         * 2、调用{@link MappedFileQueue#flush(int)}方法执行刷盘操作，最终会调用{@link MappedByteBuffer#force()}。如果刷盘指正已经大于提交的刷盘点，表示刷盘成熟，没执行一次刷盘操作，立即调用{@link GroupCommitRequest#wakeupCustomer(boolean)}唤醒消息发送线程并通知刷盘结果
         * 3、处理完所有同步刷盘任务后，更新刷盘监测点StoreCheckPoint中的physicMsgTimestamp，但是并没有执行监测点的刷盘操作，刷盘监测点的刷盘操作将在刷写消息队列文件时触发。
         *
         * 同步刷盘即消息生产者在消息服务端将消息内容追加到内存映射文件中(内存)后，需要同步将内存的内容立即刷写到磁盘。通过调用内存映射文件{@link MappedByteBuffer#force()}可将内存中数据写入到磁盘。
         */
        private void doCommit() {
            synchronized (this.requestsRead) {
                if (!this.requestsRead.isEmpty()) {
                    for (GroupCommitRequest req : this.requestsRead) {
                        // There may be a message in the next file, so a maximum of
                        // two times the flush
                        boolean flushOK = false;
                        for (int i = 0; i < 2 && !flushOK; i++) {
                            flushOK = CommitLog.this.mappedFileQueue.getFlushedWhere() >= req.getNextOffset();

                            if (!flushOK) {
                                // 执行刷盘，这里只是刷写到内存，
                                CommitLog.this.mappedFileQueue.flush(0);
                            }
                        }
                        // 完成一次数据刷盘便唤醒用户线程
                        req.wakeupCustomer(flushOK);
                    }

                    // 更新checkPoint
                    long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
                    if (storeTimestamp > 0) {
                        // 处理完已提交的刷盘任务后，更新刷盘监测点，但是并没有执行监测点的刷盘操作，刷盘检测点的刷盘操作将在刷写消息队列文件时触发
                        CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                    }

                    this.requestsRead.clear();
                } else {
                    // Because of individual messages is set to not sync flush, it
                    // will come to this process
                    CommitLog.this.mappedFileQueue.flush(0);
                }
            }
        }

        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // GroupCommitService没处理一批同步刷盘请求(requestRead容器中的请求)后，等待10ms，然后继续处理下一批
                    this.waitForRunning(10);
                    this.doCommit();
                } catch (Exception e) {
                    CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            // Under normal circumstances shutdown, wait for the arrival of the
            // request, and then flush
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                CommitLog.log.warn("GroupCommitService Exception, ", e);
            }

            synchronized (this) {
                this.swapRequests();
            }

            this.doCommit();

            CommitLog.log.info(this.getServiceName() + " service end");
        }

        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }

        @Override
        public String getServiceName() {
            return GroupCommitService.class.getSimpleName();
        }

        @Override
        public long getJointime() {
            return 1000 * 60 * 5;
        }
    }

    class DefaultAppendMessageCallback implements AppendMessageCallback {
        // File at the end of the minimum fixed length empty
        private static final int END_FILE_MIN_BLANK_LENGTH = 4 + 4;
        private final ByteBuffer msgIdMemory;
        // Store the message content
        private final ByteBuffer msgStoreItemMemory;
        // The maximum length of the message
        private final int maxMessageSize;
        // Build Message Key
        private final StringBuilder keyBuilder = new StringBuilder();

        private final StringBuilder msgIdBuilder = new StringBuilder();

        private final ByteBuffer hostHolder = ByteBuffer.allocate(8);

        DefaultAppendMessageCallback(final int size) {
            this.msgIdMemory = ByteBuffer.allocate(MessageDecoder.MSG_ID_LENGTH);
            this.msgStoreItemMemory = ByteBuffer.allocate(size + END_FILE_MIN_BLANK_LENGTH);
            this.maxMessageSize = size;
        }

        public ByteBuffer getMsgStoreItemMemory() {
            return msgStoreItemMemory;
        }

        /**
         * 回调callback类将数据写入到buffer中，消息的序列化和保存在这里完成
         * @param fileFromOffset 该文件在整个文件序列中的偏移量
         * @param byteBuffer buffer
         * @param maxBlank 最大可写字节数
         * @param msgInner msgInner
         * @return
         */
        @Override
        public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank,
                                            final MessageExtBrokerInner msgInner) {
            // STORETIMESTAMP + STOREHOSTADDRESS + OFFSET <br>

            // PHY OFFSET
            /**
             * 消息偏移
             * 计算消息在CommitLog的offset，这个偏移量是一个全局的，而不是只针对这个文件，这里的fileFromOffset就是文件第一条消息的offset，也就是文件名
             * byteBuffer.position返回的是当前MappedFile写到的位置
             */
            long wroteOffset = fileFromOffset + byteBuffer.position();

            this.resetByteBuffer(hostHolder, 8);
            /**
             * 16 byte
             * 生成messageID， 前8位是host(ip 4byte )，后8位是wroteOffset，目的是便于使用msgId来查询消息
             * 这个id是由broker的host和offset拼接起来的，所以很容易根据id得到这个消息存储在哪个broker的哪个文件中，根据id查询消息速度非常快
             */
            String msgId = MessageDecoder.createMessageId(this.msgIdMemory, msgInner.getStoreHostBytes(hostHolder), wroteOffset);

            // Record ConsumeQueue information   key = topic-queueId
            keyBuilder.setLength(0);
            keyBuilder.append(msgInner.getTopic());
            keyBuilder.append('-');
            keyBuilder.append(msgInner.getQueueId());
            String key = keyBuilder.toString();
            /**
             * key : topic-queueId
             * 通过topic-queueId获取该队里的偏移地址，待写入的地址，新增一个键值对，当前偏移量为0
             * 取得具体queue的offset，值是当前queue里的第几条消息
             * 消息在queue中的偏移量，这个偏移量值是消息在queue中的序号，从0开始。比如queue中第10条消息的offset就是9
             */
            Long queueOffset = CommitLog.this.topicQueueTable.get(key);
            if (null == queueOffset) {
                // 如果是这个queue的第一条消息，需要初始化
                queueOffset = 0L;
                CommitLog.this.topicQueueTable.put(key, queueOffset);
            }

            // Transaction messages that require special handling
            final int tranType = MessageSysFlag.getTransactionValue(msgInner.getSysFlag());
            switch (tranType) {
                // Prepared and Rollback message is not consumed, will not enter the
                // consumer queuec
                // prepare rollback类型的消息进入consum队列
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    queueOffset = 0L;
                    break;
                // 非事务消息
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                default:
                    break;
            }

            /**
             * Serialize message
             */
            final byte[] propertiesData =
                msgInner.getPropertiesString() == null ? null : msgInner.getPropertiesString().getBytes(MessageDecoder.CHARSET_UTF8);

            final int propertiesLength = propertiesData == null ? 0 : propertiesData.length;

            // 消息的附加属性长度不能超过65536个字节
            if (propertiesLength > Short.MAX_VALUE) {
                log.warn("putMessage message properties length too long. length={}", propertiesData.length);
                return new AppendMessageResult(AppendMessageStatus.PROPERTIES_SIZE_EXCEEDED);
            }

            final byte[] topicData = msgInner.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);
            final int topicLength = topicData.length;

            final int bodyLength = msgInner.getBody() == null ? 0 : msgInner.getBody().length;

            // 计算消息实际存储长度
            final int msgLen = calMsgLength(bodyLength, topicLength, propertiesLength);

            // Exceeds the maximum message 消息长度超限
            if (msgLen > this.maxMessageSize) {
                CommitLog.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLength
                    + ", maxMessageSize: " + this.maxMessageSize);
                return new AppendMessageResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED);
            }

            // Determines whether there is sufficient free space
            /**
             * 如果空间不足，magic code设置为EOF，然后剩余字节随机，保证所有文件大小都是FileSize
             * 如果文件剩余空降不足以存下这条消息，则剩余空间用一条EOF填充，然后返回EOF错误
             */
            if ((msgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
                this.resetByteBuffer(this.msgStoreItemMemory, maxBlank);
                // 1 TOTALSIZE
                this.msgStoreItemMemory.putInt(maxBlank);
                // 2 MAGICCODE
                this.msgStoreItemMemory.putInt(CommitLog.BLANK_MAGIC_CODE);
                // 3 The remaining space may be any value
                // Here the length of the specially set maxBlank
                final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
                byteBuffer.put(this.msgStoreItemMemory.array(), 0, maxBlank);
                return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset, maxBlank, msgId, msgInner.getStoreTimestamp(),
                    queueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
            }

            // Initialization of storage space
            // 按照commitLog的格式要求拼装写入ByteBuffer(mappedFile内存中)
            this.resetByteBuffer(msgStoreItemMemory, msgLen);
            // 1 TOTALSIZE，该消息条目总长度
            this.msgStoreItemMemory.putInt(msgLen);
            // 2 MAGICCODE，魔数-626843481
            this.msgStoreItemMemory.putInt(CommitLog.MESSAGE_MAGIC_CODE);
            // 3 BODYCRC 消息体crc校验码
            this.msgStoreItemMemory.putInt(msgInner.getBodyCRC());
            // 4 QUEUEID 消费队列id
            this.msgStoreItemMemory.putInt(msgInner.getQueueId());
            // 5 FLAG RocketMQ不做处理，仅供程序使用
            this.msgStoreItemMemory.putInt(msgInner.getFlag());
            // 6 QUEUEOFFSET 消息在消费队列的偏移量
            this.msgStoreItemMemory.putLong(queueOffset);
            // 7 PHYSICALOFFSET 消息在commitlog文件的偏移量
            this.msgStoreItemMemory.putLong(fileFromOffset + byteBuffer.position());
            // 8 SYSFLAG 消息系统flag,例如是否压缩、是否是事务消息
            this.msgStoreItemMemory.putInt(msgInner.getSysFlag());
            // 9 BORNTIMESTAMP 生产者调用消息发送API的时间戳
            this.msgStoreItemMemory.putLong(msgInner.getBornTimestamp());
            // 10 BORNHOST 消息发送者IP、端口
            this.resetByteBuffer(hostHolder, 8);
            this.msgStoreItemMemory.put(msgInner.getBornHostBytes(hostHolder));
            // 11 STORETIMESTAMP 消息存储时间戳
            this.msgStoreItemMemory.putLong(msgInner.getStoreTimestamp());
            // 12 STOREHOSTADDRESS broker服务器IP、端口
            this.resetByteBuffer(hostHolder, 8);
            this.msgStoreItemMemory.put(msgInner.getStoreHostBytes(hostHolder));
            //this.msgBatchMemory.put(msgInner.getStoreHostBytes());
            // 13 RECONSUMETIMES 消息重试次数
            this.msgStoreItemMemory.putInt(msgInner.getReconsumeTimes());
            // 14 Prepared Transaction Offset 事务消息物理偏移量
            this.msgStoreItemMemory.putLong(msgInner.getPreparedTransactionOffset());
            // 15 BODY 消息体长度
            this.msgStoreItemMemory.putInt(bodyLength);
            if (bodyLength > 0)
                this.msgStoreItemMemory.put(msgInner.getBody());
            // 16 TOPIC 主题，长度为TopicLength中存储的值
            this.msgStoreItemMemory.put((byte) topicLength);
            this.msgStoreItemMemory.put(topicData);
            // 17 PROPERTIES 消息属性长度
            this.msgStoreItemMemory.putShort((short) propertiesLength);
            if (propertiesLength > 0)
                this.msgStoreItemMemory.put(propertiesData);

            final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
            // Write messages to the queue buffer
            byteBuffer.put(this.msgStoreItemMemory.array(), 0, msgLen);

            AppendMessageResult result = new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, msgLen, msgId,
                msgInner.getStoreTimestamp(), queueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);

            switch (tranType) {
                // broker在将消息写入commitLog的时候会判断消息类型，如果是prepare或者rollback消息，ConsumeQueue的offset不会增加
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    break;
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                    // The next update ConsumeQueue information
                    // key=topic-queueId  更新消息队列逻辑偏移量
                    CommitLog.this.topicQueueTable.put(key, ++queueOffset);
                    break;
                default:
                    break;
            }
            return result;
        }

        public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank,
            final MessageExtBatch messageExtBatch) {
            byteBuffer.mark();
            //physical offset
            long wroteOffset = fileFromOffset + byteBuffer.position();
            // Record ConsumeQueue information
            keyBuilder.setLength(0);
            keyBuilder.append(messageExtBatch.getTopic());
            keyBuilder.append('-');
            keyBuilder.append(messageExtBatch.getQueueId());
            String key = keyBuilder.toString();
            Long queueOffset = CommitLog.this.topicQueueTable.get(key);
            if (null == queueOffset) {
                queueOffset = 0L;
                CommitLog.this.topicQueueTable.put(key, queueOffset);
            }
            long beginQueueOffset = queueOffset;
            int totalMsgLen = 0;
            int msgNum = 0;
            msgIdBuilder.setLength(0);
            final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
            ByteBuffer messagesByteBuff = messageExtBatch.getEncodedBuff();
            this.resetByteBuffer(hostHolder, 8);
            ByteBuffer storeHostBytes = messageExtBatch.getStoreHostBytes(hostHolder);
            messagesByteBuff.mark();
            while (messagesByteBuff.hasRemaining()) {
                // 1 TOTALSIZE
                final int msgPos = messagesByteBuff.position();
                final int msgLen = messagesByteBuff.getInt();
                final int bodyLen = msgLen - 40; //only for log, just estimate it
                // Exceeds the maximum message
                if (msgLen > this.maxMessageSize) {
                    CommitLog.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLen
                        + ", maxMessageSize: " + this.maxMessageSize);
                    return new AppendMessageResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED);
                }
                totalMsgLen += msgLen;
                // Determines whether there is sufficient free space
                if ((totalMsgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
                    this.resetByteBuffer(this.msgStoreItemMemory, 8);
                    // 1 TOTALSIZE
                    this.msgStoreItemMemory.putInt(maxBlank);
                    // 2 MAGICCODE
                    this.msgStoreItemMemory.putInt(CommitLog.BLANK_MAGIC_CODE);
                    // 3 The remaining space may be any value
                    //ignore previous read
                    messagesByteBuff.reset();
                    // Here the length of the specially set maxBlank
                    byteBuffer.reset(); //ignore the previous appended messages
                    byteBuffer.put(this.msgStoreItemMemory.array(), 0, 8);
                    return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset, maxBlank, msgIdBuilder.toString(), messageExtBatch.getStoreTimestamp(),
                        beginQueueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
                }
                //move to add queue offset and commitlog offset
                messagesByteBuff.position(msgPos + 20);
                messagesByteBuff.putLong(queueOffset);
                messagesByteBuff.putLong(wroteOffset + totalMsgLen - msgLen);

                storeHostBytes.rewind();
                String msgId = MessageDecoder.createMessageId(this.msgIdMemory, storeHostBytes, wroteOffset + totalMsgLen - msgLen);
                if (msgIdBuilder.length() > 0) {
                    msgIdBuilder.append(',').append(msgId);
                } else {
                    msgIdBuilder.append(msgId);
                }
                queueOffset++;
                msgNum++;
                messagesByteBuff.position(msgPos + msgLen);
            }

            messagesByteBuff.position(0);
            messagesByteBuff.limit(totalMsgLen);
            byteBuffer.put(messagesByteBuff);
            messageExtBatch.setEncodedBuff(null);
            AppendMessageResult result = new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, totalMsgLen, msgIdBuilder.toString(),
                messageExtBatch.getStoreTimestamp(), beginQueueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
            result.setMsgNum(msgNum);
            CommitLog.this.topicQueueTable.put(key, queueOffset);

            return result;
        }

        private void resetByteBuffer(final ByteBuffer byteBuffer, final int limit) {
            byteBuffer.flip();
            byteBuffer.limit(limit);
        }

    }

    public static class MessageExtBatchEncoder {
        // Store the message content
        private final ByteBuffer msgBatchMemory;
        // The maximum length of the message
        private final int maxMessageSize;

        private final ByteBuffer hostHolder = ByteBuffer.allocate(8);

        MessageExtBatchEncoder(final int size) {
            this.msgBatchMemory = ByteBuffer.allocateDirect(size);
            this.maxMessageSize = size;
        }

        public ByteBuffer encode(final MessageExtBatch messageExtBatch) {
            msgBatchMemory.clear(); //not thread-safe
            int totalMsgLen = 0;
            ByteBuffer messagesByteBuff = messageExtBatch.wrap();
            while (messagesByteBuff.hasRemaining()) {
                // 1 TOTALSIZE
                messagesByteBuff.getInt();
                // 2 MAGICCODE
                messagesByteBuff.getInt();
                // 3 BODYCRC
                messagesByteBuff.getInt();
                // 4 FLAG
                int flag = messagesByteBuff.getInt();
                // 5 BODY
                int bodyLen = messagesByteBuff.getInt();
                int bodyPos = messagesByteBuff.position();
                int bodyCrc = UtilAll.crc32(messagesByteBuff.array(), bodyPos, bodyLen);
                messagesByteBuff.position(bodyPos + bodyLen);
                // 6 properties
                short propertiesLen = messagesByteBuff.getShort();
                int propertiesPos = messagesByteBuff.position();
                messagesByteBuff.position(propertiesPos + propertiesLen);

                final byte[] topicData = messageExtBatch.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);

                final int topicLength = topicData.length;

                final int msgLen = calMsgLength(bodyLen, topicLength, propertiesLen);

                // Exceeds the maximum message
                if (msgLen > this.maxMessageSize) {
                    CommitLog.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLen
                        + ", maxMessageSize: " + this.maxMessageSize);
                    throw new RuntimeException("message size exceeded");
                }

                totalMsgLen += msgLen;
                // Determines whether there is sufficient free space
                if (totalMsgLen > maxMessageSize) {
                    throw new RuntimeException("message size exceeded");
                }

                // 1 TOTALSIZE
                this.msgBatchMemory.putInt(msgLen);
                // 2 MAGICCODE
                this.msgBatchMemory.putInt(CommitLog.MESSAGE_MAGIC_CODE);
                // 3 BODYCRC
                this.msgBatchMemory.putInt(bodyCrc);
                // 4 QUEUEID
                this.msgBatchMemory.putInt(messageExtBatch.getQueueId());
                // 5 FLAG
                this.msgBatchMemory.putInt(flag);
                // 6 QUEUEOFFSET
                this.msgBatchMemory.putLong(0);
                // 7 PHYSICALOFFSET
                this.msgBatchMemory.putLong(0);
                // 8 SYSFLAG
                this.msgBatchMemory.putInt(messageExtBatch.getSysFlag());
                // 9 BORNTIMESTAMP
                this.msgBatchMemory.putLong(messageExtBatch.getBornTimestamp());
                // 10 BORNHOST
                this.resetByteBuffer(hostHolder, 8);
                this.msgBatchMemory.put(messageExtBatch.getBornHostBytes(hostHolder));
                // 11 STORETIMESTAMP
                this.msgBatchMemory.putLong(messageExtBatch.getStoreTimestamp());
                // 12 STOREHOSTADDRESS
                this.resetByteBuffer(hostHolder, 8);
                this.msgBatchMemory.put(messageExtBatch.getStoreHostBytes(hostHolder));
                // 13 RECONSUMETIMES
                this.msgBatchMemory.putInt(messageExtBatch.getReconsumeTimes());
                // 14 Prepared Transaction Offset, batch does not support transaction
                this.msgBatchMemory.putLong(0);
                // 15 BODY
                this.msgBatchMemory.putInt(bodyLen);
                if (bodyLen > 0)
                    this.msgBatchMemory.put(messagesByteBuff.array(), bodyPos, bodyLen);
                // 16 TOPIC
                this.msgBatchMemory.put((byte) topicLength);
                this.msgBatchMemory.put(topicData);
                // 17 PROPERTIES
                this.msgBatchMemory.putShort(propertiesLen);
                if (propertiesLen > 0)
                    this.msgBatchMemory.put(messagesByteBuff.array(), propertiesPos, propertiesLen);
            }
            msgBatchMemory.flip();
            return msgBatchMemory;
        }

        private void resetByteBuffer(final ByteBuffer byteBuffer, final int limit) {
            byteBuffer.flip();
            byteBuffer.limit(limit);
        }

    }
}
