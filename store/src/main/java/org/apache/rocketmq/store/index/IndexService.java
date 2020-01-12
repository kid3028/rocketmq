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
package org.apache.rocketmq.store.index;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 索引文件结构
 * +----------------+---------------------+------------------------------+
 * | header 40byte  |  Slot Table(4*500w) |  Index Linked List(20*2000w) |
 * +----------------+---------------------+------------------------------+
 * 一个索引文件由文件头(header)、slotTable、Index List组成
 *
 * header中存储的信息有：
 * +--------------------------+--------------------------+-------------------------------+--------------------------------+----------------------+-----------------+
 * | 第一个message时间戳 8byte  |最后一个message时间戳 8byte  | 第一个message的物理offset 8byte | 最后一个message的物理offset 8byte | hash slot的个数 4byte | 目前索引个数 4byte |
 * +-------------------------+--------------------------+-------------------------------+--------------------------------+----------------------+------------------+
 * 整个slotTable + index Linked List可以理解成Java的HashMap。每当放一个新的消息的index进来，首先去MessageKey的hashCode，然后用hashCode对slot总数取模，
 * 得到应该放在哪个slot中，slot总数系统默认500w个。只要是取hash就必然会面临hash冲突的问题，跟hashMap一样，IndexFile也是使用一个链表结构来解决hash冲突，
 * slot中放的是最新的index的指针，这个是因为一般查询的时候肯定是优先查询最近的消息。
 *
 * 每个slot中放的指针值是索引在indexFile中偏移量，如下图，每个索引大小是20bytes，所以根据当前索引是这个文件中的第几个(偏移量),
 * 就很容易定位到索引的位置，然后每个索引都保存在了跟它同一个slot的前一个索引的位置，以此类推形成一链表的结构
 * +------------------+-------------------------+-------------------+---------------------------+
 * | key hash 4bytes  | commit log offset 8bytes| timestamp 4bytes  | next Index offset 4bytes  |
 * +------------------+-------------------------+-------------------+--------------------------+
 *
 * +--+----+      +--+-----+          +--+-------+
 * |  |    |<---  |  |    |<-- ...<-- |  |      |
 * +--+----+     +--+-----+           +--+-----+
 */
public class IndexService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    /**
     * Maximum times to attempt index file creation.
     */
    private static final int MAX_TRY_IDX_CREATE = 3;
    private final DefaultMessageStore defaultMessageStore;
    // hash槽数量，默认是500w
    private final int hashSlotNum;
    // index条数个数，默认是2000w
    private final int indexNum;
    // index存储路径 ${ROCKET_HOME}/store/index
    private final String storePath;
    private final ArrayList<IndexFile> indexFileList = new ArrayList<IndexFile>();
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    public IndexService(final DefaultMessageStore store) {
        this.defaultMessageStore = store;
        // 500w
        this.hashSlotNum = store.getMessageStoreConfig().getMaxHashSlotNum();
        // 2000w
        this.indexNum = store.getMessageStoreConfig().getMaxIndexNum();
        // ${ROCKET_HOME}/store/index
        this.storePath =
            StorePathConfigHelper.getStorePathIndex(store.getMessageStoreConfig().getStorePathRootDir());
    }

    /**
     * 加载Index文件
     * index
     * @param lastExitOK
     * @return
     */
    public boolean load(final boolean lastExitOK) {
        File dir = new File(this.storePath);
        File[] files = dir.listFiles();
        if (files != null) {
            // ascending order
            Arrays.sort(files);
            for (File file : files) {
                try {
                    // index 500w 2000w
                    IndexFile f = new IndexFile(file.getPath(), this.hashSlotNum, this.indexNum, 0, 0);
                    // 加载index
                    f.load();

                    if (!lastExitOK) {
                        // 如果上次非正常停机，index的最后一个message落盘时间大于checkpoint,删除该文件，并跳过该文件的加载
                        if (f.getEndTimestamp() > this.defaultMessageStore.getStoreCheckpoint()
                            .getIndexMsgTimestamp()) {
                            f.destroy(0);
                            continue;
                        }
                    }

                    log.info("load index file OK, " + f.getFileName());
                    this.indexFileList.add(f);
                } catch (IOException e) {
                    log.error("load file {} error", file, e);
                    return false;
                } catch (NumberFormatException e) {
                    log.error("load file {} error", file, e);
                }
            }
        }

        return true;
    }

    /**
     * 删除offset在执行offset之前的index文件
     *   在offset之前的视为过期文件
     * @param offset
     */
    public void deleteExpiredFile(long offset) {
        Object[] files = null;
        try {
            this.readWriteLock.readLock().lock();
            if (this.indexFileList.isEmpty()) {
                return;
            }

            // 获取第一个文件的endPhyOffset
            long endPhyOffset = this.indexFileList.get(0).getEndPhyOffset();
            // endPhyOffset小于offset，说明文件已经过期
            if (endPhyOffset < offset) {
                files = this.indexFileList.toArray();
            }
        } catch (Exception e) {
            log.error("destroy exception", e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        if (files != null) {
            List<IndexFile> fileList = new ArrayList<IndexFile>();
            // 将过期的文件放入一个list中
            for (int i = 0; i < (files.length - 1); i++) {
                IndexFile f = (IndexFile) files[i];
                if (f.getEndPhyOffset() < offset) {
                    fileList.add(f);
                } else {
                    break;
                }
            }
            // 执行删除
            this.deleteExpiredFile(fileList);
        }
    }

    /**
     * 删除过期文件
     * @param files
     */
    private void deleteExpiredFile(List<IndexFile> files) {
        if (!files.isEmpty()) {
            try {
                this.readWriteLock.writeLock().lock();
                for (IndexFile file : files) {
                    boolean destroyed = file.destroy(3000);
                    destroyed = destroyed && this.indexFileList.remove(file);
                    if (!destroyed) {
                        log.error("deleteExpiredFile remove failed.");
                        break;
                    }
                }
            } catch (Exception e) {
                log.error("deleteExpiredFile has exception.", e);
            } finally {
                this.readWriteLock.writeLock().unlock();
            }
        }
    }

    /**
     * 删除indexFileList中的文件
     */
    public void destroy() {
        try {
            this.readWriteLock.writeLock().lock();
            for (IndexFile f : this.indexFileList) {
                f.destroy(1000 * 3);
            }
            this.indexFileList.clear();
        } catch (Exception e) {
            log.error("destroy exception", e);
        } finally {
            this.readWriteLock.writeLock().unlock();
        }
    }

    /**
     * 根据messageKey读取消息
     *    1.循环扫描indexFile，先扫描最近的
     *    2.从header中读出最小和最大timestamp，然后看是否符合请求的时间范围
     *    3.从符合条件的index文件中获取offset
     * @param topic
     * @param key
     * @param maxNum
     * @param begin
     * @param end
     * @return
     */
    public QueryOffsetResult queryOffset(String topic, String key, int maxNum, long begin, long end) {
        List<Long> phyOffsets = new ArrayList<Long>(maxNum);

        long indexLastUpdateTimestamp = 0;
        long indexLastUpdatePhyoffset = 0;
        maxNum = Math.min(maxNum, this.defaultMessageStore.getMessageStoreConfig().getMaxMsgsNumBatch());
        try {
            this.readWriteLock.readLock().lock();
            if (!this.indexFileList.isEmpty()) {
                for (int i = this.indexFileList.size(); i > 0; i--) {
                    // 从最新的index开始向前寻找
                    IndexFile f = this.indexFileList.get(i - 1);
                    boolean lastFile = i == this.indexFileList.size();
                    if (lastFile) {
                        indexLastUpdateTimestamp = f.getEndTimestamp();
                        indexLastUpdatePhyoffset = f.getEndPhyOffset();
                    }

                    // index文件的时间包含了begin，end的全部或者部分
                    if (f.isTimeMatched(begin, end)) {
                        // 从文件中读取index中的offset
                        f.selectPhyOffset(phyOffsets, buildKey(topic, key), maxNum, begin, end, lastFile);
                    }

                    if (f.getBeginTimestamp() < begin) {
                        break;
                    }

                    if (phyOffsets.size() >= maxNum) {
                        break;
                    }
                }
            }
        } catch (Exception e) {
            log.error("queryMsg exception", e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        return new QueryOffsetResult(phyOffsets, indexLastUpdateTimestamp, indexLastUpdatePhyoffset);
    }

    private String buildKey(final String topic, final String key) {
        return topic + "#" + key;
    }

    /**
     * 创建index
     * @param req
     */
    public void buildIndex(DispatchRequest req) {
        /**
         * 获取或者新建当前可写入的index file
         * 与commitLog和ConsumeQueue一样，IndexFile也是使用MappedFile来存储数据的，每个MappedFile大小也是固定的。
         * 所以这里第一步获取当前正在写入的文件，没有的话则新建，IndexFile是使用当前时间戳作为文件名的
         */
        IndexFile indexFile = retryGetAndCreateIndexFile();
        if (indexFile != null) {
            // 获取当前indexFile中记录的最大offset
            long endPhyOffset = indexFile.getEndPhyOffset();
            DispatchRequest msg = req;
            String topic = msg.getTopic();
            String keys = msg.getKeys();
            // 新来消息是之前的，不应该出现，忽略
            if (msg.getCommitLogOffset() < endPhyOffset) {
                return;
            }

            final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());
            switch (tranType) {
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                    break;
                // rollback消息不更新index
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    return;
            }

            // 单条消息
            if (req.getUniqKey() != null) {
                indexFile = putKey(indexFile, msg, buildKey(topic, req.getUniqKey()));
                if (indexFile == null) {
                    log.error("putKey error commitlog {} uniqkey {}", req.getCommitLogOffset(), req.getUniqKey());
                    return;
                }
            }

            // 将消息中的keys、uniq_keys写入index文件
            if (keys != null && keys.length() > 0) {
                /**
                 * keys
                 * {@link org.apache.rocketmq.common.message.Message#Message(String, String, String, byte[])}
                 * org.apache.rocketmq.common.message.Message.Message(java.lang.String, java.lang.String, java.lang.String, int, byte[], boolean)
                 * 用户在发送消息的时候可以指定多个key，多个key使用空格分隔{@link MessageConst#KEY_SEPARATOR}
                 *
                 * UniqKey:
                 *    消息唯一键，与消息Id不一样，因为消息ID在commitLog文件中并不是唯一的，消息消费重试时，发送的消息的消息id与原先的一样（？？？待确认）
                 *    具体算法：{@link MessageClientIDSetter#createUniqID()}
                 */
                String[] keyset = keys.split(MessageConst.KEY_SEPARATOR);
                for (int i = 0; i < keyset.length; i++) {
                    String key = keyset[i];
                    if (key.length() > 0) {
                        indexFile = putKey(indexFile, msg, buildKey(topic, key));
                        if (indexFile == null) {
                            log.error("putKey error commitlog {} uniqkey {}", req.getCommitLogOffset(), req.getUniqKey());
                            return;
                        }
                    }
                }
            }
        } else {
            log.error("build index error, stop building index");
        }
    }

    /**
     * putKey方法包含了重试逻辑，因为有可能在写index的时候，上一个文件已经写满，需要创建一个新的文件写入
     * @param indexFile
     * @param msg
     * @param idxKey
     * @return
     */
    private IndexFile putKey(IndexFile indexFile, DispatchRequest msg, String idxKey) {
        for (boolean ok = indexFile.putKey(idxKey, msg.getCommitLogOffset(), msg.getStoreTimestamp()); !ok; ) {
            log.warn("Index file [" + indexFile.getFileName() + "] is full, trying to create another one");

            indexFile = retryGetAndCreateIndexFile();
            if (null == indexFile) {
                return null;
            }
            // 如果失败了，会进行重试
            ok = indexFile.putKey(idxKey, msg.getCommitLogOffset(), msg.getStoreTimestamp());
        }

        return indexFile;
    }

    /**
     * 如果有可用的返回，没有则新建
     * Retries to get or create index file.
     *
     * @return {@link IndexFile} or null on failure.
     */
    public IndexFile retryGetAndCreateIndexFile() {
        IndexFile indexFile = null;

        for (int times = 0; null == indexFile && times < MAX_TRY_IDX_CREATE; times++) {
            // 获取最后一个indexFile，如果indexFile满了，重新创建一个
            indexFile = this.getAndCreateLastIndexFile();
            if (null != indexFile)
                break;

            try {
                log.info("Tried to create index file " + times + " times");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
            }
        }

        if (null == indexFile) {
            this.defaultMessageStore.getAccessRights().makeIndexFileError();
            log.error("Mark index file cannot build flag");
        }

        return indexFile;
    }

    /**
     * 获取最后一个indexFile，如果indexFile满了，则创建新的，否则返回
     * @return
     */
    public IndexFile getAndCreateLastIndexFile() {
        IndexFile indexFile = null;
        IndexFile prevIndexFile = null;
        long lastUpdateEndPhyOffset = 0;
        long lastUpdateIndexTimestamp = 0;

        {
            this.readWriteLock.readLock().lock();
            if (!this.indexFileList.isEmpty()) {
                // 获取最后一个index
                IndexFile tmp = this.indexFileList.get(this.indexFileList.size() - 1);
                // 文件没有满
                if (!tmp.isWriteFull()) {
                    indexFile = tmp;
                }
                // 文件满了
                else {
                    lastUpdateEndPhyOffset = tmp.getEndPhyOffset();
                    lastUpdateIndexTimestamp = tmp.getEndTimestamp();
                    prevIndexFile = tmp;
                }
            }

            this.readWriteLock.readLock().unlock();
        }

        // 文件满了
        if (indexFile == null) {
            try {
                // 创建一个新的indexFile
                String fileName =
                    this.storePath + File.separator
                        + UtilAll.timeMillisToHumanString(System.currentTimeMillis());
                indexFile =
                    new IndexFile(fileName, this.hashSlotNum, this.indexNum, lastUpdateEndPhyOffset,
                        lastUpdateIndexTimestamp);
                this.readWriteLock.writeLock().lock();
                // 保存新建的indexFile
                this.indexFileList.add(indexFile);
            } catch (Exception e) {
                log.error("getLastIndexFile exception ", e);
            } finally {
                this.readWriteLock.writeLock().unlock();
            }

            // 将上一个已经满的IndexFile flush掉
            if (indexFile != null) {
                final IndexFile flushThisFile = prevIndexFile;
                Thread flushThread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        IndexService.this.flush(flushThisFile);
                    }
                }, "FlushIndexFileThread");

                flushThread.setDaemon(true);
                flushThread.start();
            }
        }

        return indexFile;
    }

    /**
     * flush文件
     * @param f
     */
    public void flush(final IndexFile f) {
        if (null == f)
            return;

        long indexMsgTimestamp = 0;

        // indexFile是否已经写满   indexCount已经>=indexNum
        if (f.isWriteFull()) {
            indexMsgTimestamp = f.getEndTimestamp();
        }

        // flush
        f.flush();

        // 更新index检测点
        if (indexMsgTimestamp > 0) {
            this.defaultMessageStore.getStoreCheckpoint().setIndexMsgTimestamp(indexMsgTimestamp);
            this.defaultMessageStore.getStoreCheckpoint().flush();
        }
    }

    public void start() {

    }

    public void shutdown() {

    }
}
