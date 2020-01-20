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
package org.apache.rocketmq.store.schedule;

import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.running.RunningStats;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.*;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 处理延时消息队列中的消息
 * broker启动的时候DefaultMessageStore会调用start方法来启动处理延时消息队列的服务
 *
 * 构造方法 --> load()  -->  start()
 *
 * 定时消息的第一个设计关键点：定时消息单独一个主题:SCHEDULE_TOPIC_XXXX，该主题下队列数量等于{@link org.apache.rocketmq.store.config.MessageStoreConfig#messageDelayLevel}
 * 配置的延迟级别数量，其对应关系为queueId等于延迟级别-1.ScheduleMessageService为每一个延迟级别创建一个定时Timer，根据延迟级别对应的延迟时间进行延迟调度。在消息发送时，如果消息的
 * 延迟级别delayLevel大于0，将消息的原主题名称、队列id存入消息属性中，然后改变消息的主题，队列与延迟主题与延迟主题所属队列，消息最终转发到延迟消息的消费队列
 *
 * 定时消息的第二个设计关键点：消息存储是如果消息的延迟级别属性delayLevel大于0，则会备份圆柱体、原队列到消息属性中，其键分别为PROPERTY_REAL_TOPIC/PROPERTY_REAL_QUEUE_ID，通过为
 * 不同的延迟级别创建不同的调度任务，当时间到达后执行调度任务，调度任务主要就是根据延迟拉取消息消费进度从延迟队列中拉取消息，然后从commitlog中加载完整消息，清除延迟级别属性并恢复原先的主题，队列，
 * 再次创建一条新的消息存入到commitlog中并转发到消息消费队列供消息消费者消费
 *
 *
 * 定时消息实现的原理：
 *  1.消息生产者发送消息，如果发送消息的delayLevel大于0，则改变消息主题为SCHEDULE_TOPIC_XXXX，消息队列为delayLevel-1
 *  2.消息经由commitlog转发到消息消费队列SCHEDULE_TOPIC_XXXX的消息消费队里
 *  3.定时任务Timer每隔1s根据上次拉取偏移量从消费队列中取出所有消息
 *  4.根据消息的物理偏移量与消息大小从commitlog中拉取消息
 *  5.根据消息属性重新创建消息，并恢复原主题、原队列，清除delayLevel属性，存入commitlog
 *  6.转发到原主题的消息消费队列，供消费者消费
 */
public class ScheduleMessageService extends ConfigManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 定时消息统一主题
     */
    public static final String SCHEDULE_TOPIC = "SCHEDULE_TOPIC_XXXX";
    /**
     * 第一次调度时的延迟时间，默认1s
     */
    private static final long FIRST_DELAY_TIME = 1000L;
    /**
     * 每一延时级别调度一次后延迟该时间间隔后再放入调度池
     */
    private static final long DELAY_FOR_A_WHILE = 100L;
    /**
     * 发送异常后延迟该时间后再继续参与调度
     */
    private static final long DELAY_FOR_A_PERIOD = 10000L;

    /**
     * 保存了具体的延时等级  ---  延迟时间
     */
    private final ConcurrentMap<Integer /* level */, Long/* delay timeMillis */> delayLevelTable =
            new ConcurrentHashMap<Integer, Long>(32);

    /**
     * 延迟等级  --- 消费进度
     */
    private final ConcurrentMap<Integer /* level */, Long/* offset */> offsetTable =
            new ConcurrentHashMap<Integer, Long>(32);

    private final Timer timer = new Timer("ScheduleMessageTimerThread", true);

    /**
     * 默认消息存储器
     */
    private final DefaultMessageStore defaultMessageStore;

    /**
     * {@link org.apache.rocketmq.store.config.MessageStoreConfig#messageDelayLevel} 中最大的消息延迟等级
     */
    private int maxDelayLevel;

    public ScheduleMessageService(final DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
    }

    public static int queueId2DelayLevel(final int queueId) {
        return queueId + 1;
    }

    public static int delayLevel2QueueId(final int delayLevel) {
        return delayLevel - 1;
    }

    public void buildRunningStats(HashMap<String, String> stats) {
        Iterator<Entry<Integer, Long>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Integer, Long> next = it.next();
            int queueId = delayLevel2QueueId(next.getKey());
            long delayOffset = next.getValue();
            // 获取topic queueId最大offset
            long maxOffset = this.defaultMessageStore.getMaxOffsetInQueue(SCHEDULE_TOPIC, queueId);
            String value = String.format("%d,%d", delayOffset, maxOffset);
            String key = String.format("%s_%d", RunningStats.scheduleMessageOffset.name(), next.getKey());
            stats.put(key, value);
        }
    }

    /**
     * 更新offset
     * @param delayLevel
     * @param offset
     */
    private void updateOffset(int delayLevel, long offset) {
        this.offsetTable.put(delayLevel, offset);
    }

    /**
     * 计算延时消息的到期时间
     * @param delayLevel 延时等级存储时间
     * @param storeTimestamp
     * @return
     */
    public long computeDeliverTimestamp(final int delayLevel, final long storeTimestamp) {
        // 获取到延时等级对应的延时时间
        Long time = this.delayLevelTable.get(delayLevel);
        if (time != null) {
            // delayTime + storeTime
            return time + storeTimestamp;
        }

        return storeTimestamp + 1000;
    }

    /**
     * broker启动的时候DefaultMessageStore会调用start方法来启动处理延时消息队列的服务
     */
    public void start() {

        for (Map.Entry<Integer, Long> entry : this.delayLevelTable.entrySet()) {
            Integer level = entry.getKey();
            Long timeDelay = entry.getValue();
            // 记录队列的处理进度
            Long offset = this.offsetTable.get(level);
            if (null == offset) {
                offset = 0L;
            }

            if (timeDelay != null) {
                // 每一个延迟级别对应一个延迟队列，每个延时队列启动一个定时任务来处理该队列的延时消息  1000ms执行一次
                this.timer.schedule(new DeliverDelayedMessageTimerTask(level, offset), FIRST_DELAY_TIME);
            }
        }

        this.timer.scheduleAtFixedRate(new TimerTask() {

            @Override
            public void run() {
                try {
                    // 持久化offsetTable（保存了每个延时队列对应的处理进度offset）
                    ScheduleMessageService.this.persist();
                } catch (Throwable e) {
                    log.error("scheduleAtFixedRate flush exception", e);
                }
            }
        }, 10000, this.defaultMessageStore.getMessageStoreConfig().getFlushDelayOffsetInterval());
    }

    public void shutdown() {
        this.timer.cancel();
    }

    public int getMaxDelayLevel() {
        return maxDelayLevel;
    }

    public String encode() {
        return this.encode(false);
    }

    /**
     * 1.加载delayOffset.json
     * 2.将配置的延迟时间转化为对应的毫秒值
     * @return
     */
    public boolean load() {
        // 主要完成延迟消息消费队列消息进度的加载与delayLevel数据的构造，延迟消息队列消息消费进度默认存储路径为${ROCKETMQ_HOME}/store/config/delayOffset.json
        // {"offsetTable":{12:0, 6:0, 13:0, 5:1....}}
        boolean result = super.load();
        // 将配置的延时时间转化为对应的毫秒值
        result = result && this.parseDelayLevel();
        return result;
    }

    /**
     * delayOffset.json
     * @return
     */
    @Override
    public String configFilePath() {
        return StorePathConfigHelper.getDelayOffsetStorePath(this.defaultMessageStore.getMessageStoreConfig()
                .getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            DelayOffsetSerializeWrapper delayOffsetSerializeWrapper =
                    DelayOffsetSerializeWrapper.fromJson(jsonString, DelayOffsetSerializeWrapper.class);
            if (delayOffsetSerializeWrapper != null) {
                this.offsetTable.putAll(delayOffsetSerializeWrapper.getOffsetTable());
            }
        }
    }

    public String encode(final boolean prettyFormat) {
        DelayOffsetSerializeWrapper delayOffsetSerializeWrapper = new DelayOffsetSerializeWrapper();
        delayOffsetSerializeWrapper.setOffsetTable(this.offsetTable);
        return delayOffsetSerializeWrapper.toJson(prettyFormat);
    }

    /**
     * delayLevelTable初始化
     * 在broker启动的时候DefaultMessageStore会调用来初始化延时等级
     *
     * @return
     */
    public boolean parseDelayLevel() {
        HashMap<String, Long> timeUnitTable = new HashMap<String, Long>();
        // 每个延时等级延时时间的单位对应的ms数
        timeUnitTable.put("s", 1000L);
        timeUnitTable.put("m", 1000L * 60);
        timeUnitTable.put("h", 1000L * 60 * 60);
        timeUnitTable.put("d", 1000L * 60 * 60 * 24);

        // 每个延时等级在MessageStoreConfig中配置
        // org.apache.rocketmq.store.config.MessageStoreConfig.messageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h"
        String levelString = this.defaultMessageStore.getMessageStoreConfig().getMessageDelayLevel();
        try {
            // 根据空格将配置分隔出每个等级 length = 18
            String[] levelArray = levelString.split(" ");
            for (int i = 0; i < levelArray.length; i++) {
                String value = levelArray[i];
                String ch = value.substring(value.length() - 1);
                // 获取时间单位对应的ms数
                Long tu = timeUnitTable.get(ch);

                // 延时等级从1开始
                int level = i + 1;
                if (level > this.maxDelayLevel) {
                    // 找出最大的延时等级
                    this.maxDelayLevel = level;
                }
                // 计算延时时间 ms
                long num = Long.parseLong(value.substring(0, value.length() - 1));
                long delayTimeMillis = tu * num;
                this.delayLevelTable.put(level, delayTimeMillis);
            }
        } catch (Exception e) {
            log.error("parseDelayLevel exception", e);
            log.info("levelString String = {}", levelString);
            return false;
        }

        return true;
    }

    /**
     * TimerTask,启动以后不断处理延时队列中的消息，直到出现异常终止该线程重新启动一个新的TimerTask
     */
    class DeliverDelayedMessageTimerTask extends TimerTask {
        private final int delayLevel;
        private final long offset;

        public DeliverDelayedMessageTimerTask(int delayLevel, long offset) {
            this.delayLevel = delayLevel;
            this.offset = offset;
        }

        @Override
        public void run() {
            try {
                // 执行延时消息处理，将到期的消息进行投递
                this.executeOnTimeup();
            } catch (Exception e) {
                // XXX: warn and notify me
                log.error("ScheduleMessageService, executeOnTimeup exception", e);
                ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(
                        this.delayLevel, this.offset), DELAY_FOR_A_PERIOD);
            }
        }

        /**
         * @return
         */
        private long correctDeliverTimestamp(final long now, final long deliverTimestamp) {

            long result = deliverTimestamp;

            long maxTimestamp = now + ScheduleMessageService.this.delayLevelTable.get(this.delayLevel);
            if (deliverTimestamp > maxTimestamp) {
                result = now;
            }

            return result;
        }


        /**
         * 将到期的消息进行投递
         */
        public void executeOnTimeup() {
            // 找到该延时等级（实际是将延时等级转化为queueId）对应的ConsumeQueue
            ConsumeQueue cq =
                    ScheduleMessageService.this.defaultMessageStore.findConsumeQueue(SCHEDULE_TOPIC,
                            delayLevel2QueueId(delayLevel));
            // 记录下异常情况下一次启动TimerTask开始处理的offset
            long failScheduleOffset = offset;

            if (cq != null) {
                // 根据offset从消息消费队列中获取当前队列中所有有效的信息。如果没有找到，则更新一下延迟队列定时拉取进度并创建定时任务待下一次尝试
                SelectMappedBufferResult bufferCQ = cq.getIndexBuffer(this.offset);
                if (bufferCQ != null) {
                    try {
                        long nextOffset = offset;
                        int i = 0;
                        ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                        for (; i < bufferCQ.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                            // 这三个字段信息是ConsumerQueue物理存储的信息
                            long offsetPy = bufferCQ.getByteBuffer().getLong();
                            int sizePy = bufferCQ.getByteBuffer().getInt();
                            /**
                            *  延时消息的tagCode和普通消息不一样
                             *  延时消息的tagCode：存储的是消息到期的时间
                             *  非延时消息的tagCode：tags字符串的hashCode
                             *  对于延时消息到的tagCode的特别处理是在{@link CommitLog#checkMessageAndReturnSize(ByteBuffer, boolean, boolean)}
                             *  方法中完成到的，也就是在build ConsumeQueue信息的时候
                            */
                            long tagsCode = bufferCQ.getByteBuffer().getLong(); // 这个tagCode不再是普通的tag的hashCode，而是该延时消息到期到的时间

                            // 检查tagsCode是否是扩展文件地址
                            if (cq.isExtAddr(tagsCode)) {
                                if (cq.getExt(tagsCode, cqExtUnit)) {
                                    tagsCode = cqExtUnit.getTagsCode();
                                } else {
                                    //can't find ext content.So re compute tags code.
                                    log.error("[BUG] can't find consume queue extend file content!addr={}, offsetPy={}, sizePy={}",
                                            tagsCode, offsetPy, sizePy);
                                    long msgStoreTime = defaultMessageStore.getCommitLog().pickupStoreTimestamp(offsetPy, sizePy);
                                    tagsCode = computeDeliverTimestamp(delayLevel, msgStoreTime);
                                }
                            }

                            long now = System.currentTimeMillis();
                            // 计算应该投递该消息的时间，如果超过则立即投递
                            long deliverTimestamp = this.correctDeliverTimestamp(now, tagsCode);
                            // 计算下一个消息的开始位置，用来寻找下一个消息的位置(如果有的话)
                            nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);

                            // 判断消息是否到期
                            long countdown = deliverTimestamp - now;

                            // countdown小于0，说明消息已经到期或者过期，立即执行投递
                            if (countdown <= 0) {
                                // 从commitlog中加载具体的消息，从指定位置offsetPy开始，获取sizePy大小的数据
                                MessageExt msgExt =
                                        ScheduleMessageService.this.defaultMessageStore.lookMessageByOffset(
                                                offsetPy, sizePy);

                                if (msgExt != null) {
                                    try {
                                        // 将消息恢复到原始消息的格式，恢复topic，queueId，tagCode等，清除DELAY属性
                                        MessageExtBrokerInner msgInner = this.messageTimeup(msgExt);
                                        // 将恢复后的消息进行投递到commitLog 如果是重试消息，现在topic已经是%RETRY%_GROUPNAME
                                        PutMessageResult putMessageResult =
                                                ScheduleMessageService.this.defaultMessageStore
                                                        .putMessage(msgInner);

                                        if (putMessageResult != null
                                                && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                                            // 投递成功，处理下一个
                                            continue;
                                        } else {
                                            // XXX: warn and notify me
                                            log.error(
                                                    "ScheduleMessageService, a message time up, but reput it failed, topic: {} msgId {}",
                                                    msgExt.getTopic(), msgExt.getMsgId());
                                            // 投递失败，结束当前task，重新启动TimerTask，从下一个消息开始处理，也就是说当前消息丢弃
                                            // 更新offsetTable中当前队列的offset为下一个消息的offset
                                            ScheduleMessageService.this.timer.schedule(
                                                    new DeliverDelayedMessageTimerTask(this.delayLevel,
                                                            nextOffset), DELAY_FOR_A_PERIOD);
                                            ScheduleMessageService.this.updateOffset(this.delayLevel,
                                                    nextOffset);
                                            return;
                                        }
                                    } catch (Exception e) {
                                        /*
                                         * XXX: warn and notify me



                                         */
                                        // 重新投递期间出现任何异常，结束当前的task，重新启动TimerTask，从当前消息开始重试
                                        log.error(
                                                "ScheduleMessageService, messageTimeup execute error, drop it. msgExt="
                                                        + msgExt + ", nextOffset=" + nextOffset + ",offsetPy="
                                                        + offsetPy + ",sizePy=" + sizePy, e);
                                    }
                                }
                            } else {
                                // 如果还有消息到期，休息countdown之后再启动任务
                                ScheduleMessageService.this.timer.schedule(
                                        new DeliverDelayedMessageTimerTask(this.delayLevel, nextOffset),
                                        countdown);
                                // 更新offset
                                ScheduleMessageService.this.updateOffset(this.delayLevel, nextOffset);
                                return;
                            }
                        } // end of for

                        // 已经读取完所有可读取的
                        // 更新offset
                        nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);
                        // 休息100ms后继续
                        ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(
                                this.delayLevel, nextOffset), DELAY_FOR_A_WHILE);
                        // 更新offset
                        ScheduleMessageService.this.updateOffset(this.delayLevel, nextOffset);
                        return;
                    } finally {

                        bufferCQ.release();
                    }
                } // end of if (bufferCQ != null)
                // 如果没有找到，则更新一下延迟队列定时拉取进度并创建定时任务待下一次尝试
                else {
                    // 如果根据offsetTable中的offset没有找到对应的消息(可能被删除了)，则按照当前ConsumeQueue的最小offset开始处理
                    long cqMinOffset = cq.getMinOffsetInQueue();
                    if (offset < cqMinOffset) {
                        failScheduleOffset = cqMinOffset;
                        log.error("schedule CQ offset invalid. offset=" + offset + ", cqMinOffset="
                                + cqMinOffset + ", queueId=" + cq.getQueueId());
                    }
                }
            } // end of if (cq != null)

            // cq == null 说明目前不存在该延迟级别的消息，忽略本次任务，

            // 休息100ms后再执行
            ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(this.delayLevel,
                    failScheduleOffset), DELAY_FOR_A_WHILE);
        }

        /**
         * 将消息恢复到原始消息格式，恢复topic，queueId,tagCode等，清除属性DELAY
         *
         * @param msgExt
         * @return
         */
        private MessageExtBrokerInner messageTimeup(MessageExt msgExt) {
            MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
            msgInner.setBody(msgExt.getBody());
            msgInner.setFlag(msgExt.getFlag());
            MessageAccessor.setProperties(msgInner, msgExt.getProperties());

            TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());
            long tagsCodeValue =
                    MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
            msgInner.setTagsCode(tagsCodeValue);
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

            msgInner.setSysFlag(msgExt.getSysFlag());
            msgInner.setBornTimestamp(msgExt.getBornTimestamp());
            msgInner.setBornHost(msgExt.getBornHost());
            msgInner.setStoreHost(msgExt.getStoreHost());
            msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());

            msgInner.setWaitStoreMsgOK(false);
            MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_DELAY_TIME_LEVEL);

            msgInner.setTopic(msgInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC));

            String queueIdStr = msgInner.getProperty(MessageConst.PROPERTY_REAL_QUEUE_ID);
            int queueId = Integer.parseInt(queueIdStr);
            msgInner.setQueueId(queueId);

            return msgInner;
        }
    }
}
