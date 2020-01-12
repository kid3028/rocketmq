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

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MappedFile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.List;

/**
 *
 * 索引文件结构
 * 一个索引文件由文件头(header)、slotTable、Index List组成
 * +--+--+--+--+--+--+-+-+-+-+-+-+-+-+-+-+-+-+---+---+---+---+---+---+---+
 * |  |  |  |  |  |  | | | | | | | | | | | | |   |   |   |   |   |   |   |
 * +--+--+--+--+--+--+-+-+-+-+-+-+-+-+-+-+-+-+---+---+---+---+---+---+---+
 * |<-header 40byte->|<-Slot Table(4*500w)->|<-IndexLinked List(20*2000w)->|
 *
 * header中存储的信息有：
 * +--------------------------+--------------------------+-------------------------------+--------------------------------+----------------------+-----------------+
 * | 第一个message时间戳 8byte  |最后一个message时间戳 8byte  | 第一个message的物理offset 8byte | 最后一个message的物理offset 8byte | hash slot的个数 4byte | 目前索引个数 4byte |
 * +-------------------------+--------------------------+-------------------------------+--------------------------------+----------------------+------------------+
 *
 * hashSlot每个槽4个字节，存放的是对应hashCode最新的index条目的位置(顺序号或者说是偏移量)
 *
 * 整个slotTable + index Linked List可以理解成Java的HashMap。每当放一个新的消息的index进来，首先去MessageKey的hashCode，然后用hashCode对slot总数取模，
 * 得到应该放在哪个slot中，slot总数系统默认500w个。只要是取hash就必然会面临hash冲突的问题，跟hashMap一样，IndexFile也是使用一个链表结构来解决hash冲突，
 * slot中放的是最新的index的指针，这个是因为一般查询的时候肯定是优先查询最近的消息。
 *
 * 每个slot中放的指针值是索引在indexFile中偏移量，如下图，每个索引大小是20bytes，所以根据当前索引是这个文件中的第几个(偏移量),
 * 就很容易定位到索引的位置，然后每个索引都保存在了跟它同一个slot的前一个索引的位置，以此类推形成一链表的结构
 * IndexLinked List结构
 * +------------------+-------------------------+-------------------+----------------------------------------+
 * | key hash 4bytes  | commit log offset 8bytes| timeDiff  4bytes  | prev Index order with same key 4bytes  |
 * +------------------+-------------------------+-------------------+----------------------------------------+
 *
 * 相同hashcode的index条目之间将会形成一个逻辑链表
 * +--+----+      +--+-----+          +--+-------+
 * |  |    |<---  |  |    |<-- ...<-- |  |      |
 * +--+----+     +--+-----+           +--+-----+
 *
 * index存储路径：${ROCKET_HOME}/store/index/年月日时分秒
 *
 * 上面的设计，可以支持hashcode冲突，多个不同的key，相同的hashcode，index条目其实是一个逻辑链表的概念，因为每个index条目的最后
 * 4个字节存放的就是上一个的位置。在检索时，根据key得到hashCode，然后从最新的条目开始找，匹配时间戳是否有效，得到消息的物理地址(存放在commitLog文件中的位置)，
 * 然后便可以根据commitLog偏移量找到具体的消息，从而得到最终的key-value
 *
 */
public class IndexFile {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static int hashSlotSize = 4;
    // 每条indexFile条目占用字节数
    private static int indexSize = 20;
    private static int invalidIndex = 0;
    private final int hashSlotNum;
    private final int indexNum;
    private final MappedFile mappedFile;
    private final FileChannel fileChannel;
    private final MappedByteBuffer mappedByteBuffer;
    // 每个indexFile的头部信息
    private final IndexHeader indexHeader;

    public IndexFile(final String fileName, final int hashSlotNum, final int indexNum,
        final long endPhyOffset, final long endTimestamp) throws IOException {
        // 单个indexFile总大小
        int fileTotalSize =
            IndexHeader.INDEX_HEADER_SIZE + (hashSlotNum * hashSlotSize) + (indexNum * indexSize);
        this.mappedFile = new MappedFile(fileName, fileTotalSize);
        this.fileChannel = this.mappedFile.getFileChannel();
        this.mappedByteBuffer = this.mappedFile.getMappedByteBuffer();
        this.hashSlotNum = hashSlotNum;
        this.indexNum = indexNum;

        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        this.indexHeader = new IndexHeader(byteBuffer);

        if (endPhyOffset > 0) {
            this.indexHeader.setBeginPhyOffset(endPhyOffset);
            this.indexHeader.setEndPhyOffset(endPhyOffset);
        }

        if (endTimestamp > 0) {
            this.indexHeader.setBeginTimestamp(endTimestamp);
            this.indexHeader.setEndTimestamp(endTimestamp);
        }
    }

    public String getFileName() {
        return this.mappedFile.getFileName();
    }

    /**
     * 加载index
     */
    public void load() {
        this.indexHeader.load();
    }

    /**
     * 刷新index
     */
    public void flush() {
        long beginTime = System.currentTimeMillis();
        if (this.mappedFile.hold()) {
            this.indexHeader.updateByteBuffer();
            // flush
            this.mappedByteBuffer.force();
            this.mappedFile.release();
            log.info("flush index file eclipse time(ms) " + (System.currentTimeMillis() - beginTime));
        }
    }

    /**
     * indexFile是否已经写满  index count > indexNum
     * @return
     */
    public boolean isWriteFull() {
        return this.indexHeader.getIndexCount() >= this.indexNum;
    }

    public boolean destroy(final long intervalForcibly) {
        return this.mappedFile.destroy(intervalForcibly);
    }

    /**
     * IndexFile数据写入
     * @param key
     * @param phyOffset 消息存储在commitLog中的偏移量
     * @param storeTimestamp 消息存入commitLog的时间戳
     * @return
     */
    public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
        // 判断index是否已满，已满返回失败，由调用方来处理，IndexService有重试机制，默认会重试3次
        if (this.indexHeader.getIndexCount() < this.indexNum) {
            // 计算可以的非负hashCode，调用Java String的hashCode方法
            int keyHash = indexKeyHashMethod(key);
            // key应该放在哪个slot hashcode % hashSlotNum，得到该key所在的hashSlot下标，hashSlotNum默认500w个
            int slotPos = keyHash % this.hashSlotNum;
            // slot的绝对位置
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;

            try {

                // fileLock = this.fileChannel.lock(absSlotPos, hashSlotSize,
                // false);
                // 如果存在hash冲突，获得这个slot存的当前Index在IndexLinked中的偏移量IndexPosition，如果没有则值为0
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()) {
                    slotValue = invalidIndex;
                }

                // 计算当前msg的存储时间和第一条msg相差秒数
                long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();

                timeDiff = timeDiff / 1000;

                if (this.indexHeader.getBeginTimestamp() <= 0) {
                    timeDiff = 0;
                } else if (timeDiff > Integer.MAX_VALUE) {
                    timeDiff = Integer.MAX_VALUE;
                } else if (timeDiff < 0) {
                    timeDiff = 0;
                }

                // 获取该条index实际存储position
                int absIndexPos =
                    IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                        + this.indexHeader.getIndexCount() * indexSize;

                // 生成一个index的unit内容
                // key的hash，不会记录完整的key
                this.mappedByteBuffer.putInt(absIndexPos, keyHash);
                // 消息在commitLog中的偏移
                this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);
                // 时间差
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);
                // 相同hashCode的前一条index的顺序号即IndexPosition  slotValue为0表示没有上一个
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue);

                // 更新slot中的值为本条消息的顺序号IndexPosition
                this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());

                // 如果是第一条消息，更新header中的起始offset和起始time
                if (this.indexHeader.getIndexCount() <= 1) {
                    this.indexHeader.setBeginPhyOffset(phyOffset);
                    this.indexHeader.setBeginTimestamp(storeTimestamp);
                }

                // 更新header中的计数器
                this.indexHeader.incHashSlotCount();
                // 增加index计数
                this.indexHeader.incIndexCount();
                // 最后一个消息的offset
                this.indexHeader.setEndPhyOffset(phyOffset);
                // 最后一个index的时间
                this.indexHeader.setEndTimestamp(storeTimestamp);

                return true;
            } catch (Exception e) {
                log.error("putKey exception, Key: " + key + " KeyHashCode: " + key.hashCode(), e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }
            }
        } else {
            log.warn("Over index file capacity: index count = " + this.indexHeader.getIndexCount()
                + "; index max num = " + this.indexNum);
        }

        return false;
    }

    /**
     * 计算Key的hashcode，直接使用string的hashcode
     * key -> topic#key
     * @param key
     * @return
     */
    public int indexKeyHashMethod(final String key) {
        int keyHash = key.hashCode();
        int keyHashPositive = Math.abs(keyHash);
        if (keyHashPositive < 0)
            keyHashPositive = 0;
        return keyHashPositive;
    }

    public long getBeginTimestamp() {
        return this.indexHeader.getBeginTimestamp();
    }

    public long getEndTimestamp() {
        return this.indexHeader.getEndTimestamp();
    }

    public long getEndPhyOffset() {
        return this.indexHeader.getEndPhyOffset();
    }

    public boolean isTimeMatched(final long begin, final long end) {
        boolean result = begin < this.indexHeader.getBeginTimestamp() && end > this.indexHeader.getEndTimestamp();
        result = result || (begin >= this.indexHeader.getBeginTimestamp() && begin <= this.indexHeader.getEndTimestamp());
        result = result || (end >= this.indexHeader.getBeginTimestamp() && end <= this.indexHeader.getEndTimestamp());
        return result;
    }

    /**
     * indexFile的读取步骤
     *    1.首先根据key获取索引对应的slot，这里的逻辑与存入索引时的一样
     *    2.slot中value存储的的是最后一个index的顺序号
     *    3.将符合条件的offset加入返回列表
     *    4.如果存在相同hash的前一条index，并且返回列表没有到最大值，则继续向前搜索
     *
     * 通过index的查询消息的逻辑可以看出，相同hashCode的message都会返回客户端，如果调用这个接口通过可以来查询消息，
     * 需要在客户端再做一次过滤。为了提高查询效率，在发送消息时应该在保证便于查询的同时，尽量在一段时间内让消息有不同的key
     * @param phyOffsets 符合查找条件的物理偏移量（commitLog文件中的偏移量） 实际就是用来接收结果集
     * @param key 索引键值，待查找的key
     * @param maxNum 最大搜索结果条数，达到该限制值后，立即返回
     * @param begin 搜索开始时间戳ms
     * @param end 搜索结束时间戳ms
     * @param lock
     */
    public void selectPhyOffset(final List<Long> phyOffsets, final String key, final int maxNum,
        final long begin, final long end, boolean lock) {
        if (this.mappedFile.hold()) {
            // 跟生成索引时一样,根据key计算出hashcode，然后定位到hash槽的位置
            int keyHash = indexKeyHashMethod(key);
            int slotPos = keyHash % this.hashSlotNum;
            // hashSlot位置
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;
            try {
                if (lock) {
                    // fileLock = this.fileChannel.lock(absSlotPos,
                    // hashSlotSize, true);
                }

                // 获取该slot槽位上的最后一条索引的序号
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                // if (fileLock != null) {
                // fileLock.release();
                // fileLock = null;
                // }

                /**
                 * 如果该位置存储的值《= 0或者大于当前indexCount的值，或者当前的IndexCount的值《= 1，
                 * 则视为无效，也就是该hashCode值并没有对应的index，
                 */
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()
                    || this.indexHeader.getIndexCount() <= 1) {
                } else {
                    for (int nextIndexToRead = slotValue; ; ) {
                        // 到达最大返回条数
                        if (phyOffsets.size() >= maxNum) {
                            break;
                        }

                        // 找到index的位置
                        int absIndexPos =
                            IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                                + nextIndexToRead * indexSize;

                        int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
                        // 在commitLog中的偏移
                        long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);

                        long timeDiff = (long) this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);
                        // 相同hashcode的前一条消息的序号
                        int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);

                        ///////////////////////////////////////////////////////////////////////
                        // 找到条目内容，然后与查询条件进行匹配，如果符合，则将物理偏移量加入phyOffset中，否则继续查找
                        // 存储到commitLog的时间戳-beginTimestamp < 0 数据错误，结束循环
                        if (timeDiff < 0) {
                            break;
                        }

                        // 转化为ms
                        timeDiff *= 1000L;

                        // beginTimestamp + timeDiff = commitLog storeTimestamp
                        long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff;
                        // 是否在查询时间范围内
                        boolean timeMatched = (timeRead >= begin) && (timeRead <= end);

                        ///////////////////////////////////////////////////////////////////////

                        // hash和time都符合条件，加入返回列表
                        if (keyHash == keyHashRead && timeMatched) {
                            phyOffsets.add(phyOffsetRead);
                        }

                        if (prevIndexRead <= invalidIndex
                            || prevIndexRead > this.indexHeader.getIndexCount()
                            || prevIndexRead == nextIndexToRead || timeRead < begin) {
                            break;
                        }

                        // 前一条不等于0，继续读入前一条
                        nextIndexToRead = prevIndexRead;
                    }
                }
            } catch (Exception e) {
                log.error("selectPhyOffset exception ", e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }

                this.mappedFile.release();
            }
        }
    }
}
