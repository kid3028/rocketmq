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
package org.apache.rocketmq.remoting.protocol;

import com.alibaba.fastjson.annotation.JSONField;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyDecoder;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 在消息传输过程中对所有数据内容的封装。不但包含了所有的数据接口，还包含了编解码操作
 * 总长度 4+4+消息头长度+消息体长度
 * +----------------------+---------------------------------+-----------------------+---------------------------+
 * |  消息长度           | 序列化类型&头部长度              |  消息头数据           |  消息主体数据              |
 * |  4个字节           | 4字节(2,3,4个字节表示消息头长度) | 头部长度中描述的长度   |  消息长度 - 消息头长度     |
 * +-------------------+---------------------------------+------------------------+---------------------------+
 */
public class RemotingCommand {
    public static final String SERIALIZE_TYPE_PROPERTY = "rocketmq.serialize.type";
    public static final String SERIALIZE_TYPE_ENV = "ROCKETMQ_SERIALIZE_TYPE";
    public static final String REMOTING_VERSION_KEY = "rocketmq.remoting.version";
    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);
    private static final int RPC_TYPE = 0; // 0, REQUEST_COMMAND
    private static final int RPC_ONEWAY = 1; // 0, RPC
    private static final Map<Class<? extends CommandCustomHeader>, Field[]> CLASS_HASH_MAP =
        new HashMap<Class<? extends CommandCustomHeader>, Field[]>();
    private static final Map<Class, String> CANONICAL_NAME_CACHE = new HashMap<Class, String>();
    // 1, Oneway
    // 1, RESPONSE_COMMAND
    private static final Map<Field, Boolean> NULLABLE_FIELD_CACHE = new HashMap<Field, Boolean>();
    private static final String STRING_CANONICAL_NAME = String.class.getCanonicalName();
    private static final String DOUBLE_CANONICAL_NAME_1 = Double.class.getCanonicalName();
    private static final String DOUBLE_CANONICAL_NAME_2 = double.class.getCanonicalName();
    private static final String INTEGER_CANONICAL_NAME_1 = Integer.class.getCanonicalName();
    private static final String INTEGER_CANONICAL_NAME_2 = int.class.getCanonicalName();
    private static final String LONG_CANONICAL_NAME_1 = Long.class.getCanonicalName();
    private static final String LONG_CANONICAL_NAME_2 = long.class.getCanonicalName();
    private static final String BOOLEAN_CANONICAL_NAME_1 = Boolean.class.getCanonicalName();
    private static final String BOOLEAN_CANONICAL_NAME_2 = boolean.class.getCanonicalName();
    private static volatile int configVersion = -1;
    private static AtomicInteger requestId = new AtomicInteger(0);

    private static SerializeType serializeTypeConfigInThisServer = SerializeType.JSON;

    static {
        final String protocol = System.getProperty(SERIALIZE_TYPE_PROPERTY, System.getenv(SERIALIZE_TYPE_ENV));
        if (!isBlank(protocol)) {
            try {
                serializeTypeConfigInThisServer = SerializeType.valueOf(protocol);
            } catch (IllegalArgumentException e) {
                throw new RuntimeException("parser specified protocol error. protocol=" + protocol, e);
            }
        }
    }

    /**
     * request 请求操作码，请求接受方根据不同的代码做不同的操作
     * response 应答结果代码，0表示成功，非0表示各种错误代码
     */
    private int code;
    /**
     * request 和 response 请求方和应答方实现语言
     */
    private LanguageCode language = LanguageCode.JAVA;
    /**
     * request 和 response 请求方和应答方程序版本
     */
    private int version = 0;
    /**
     * request 请求方在同一连接上不同的请求标识代码，多线程连接复用使用
     * response 应答方不修改直接返回
     */
    private int opaque = requestId.getAndIncrement();
    /**
     * request & response 通信层的标识位
     * 倒数第一位表示请求类型类型 0：请求 1：返回
     * 倒数第二位 1：表示oneway
     */
    private int flag = 0;
    /**
     * request 传输自定义文本信息
     * response 错误详细的描述信息
     */
    private String remark;
    /**
     * 可以存储用户定义的键值对信息
     * request 请求自定义字段
     * response 应答自定义字段
     */
    private HashMap<String, String> extFields;
    /**
     * 每个请求对应的请求头信息
     */
    private transient CommandCustomHeader customHeader;

    private SerializeType serializeTypeCurrentRPC = serializeTypeConfigInThisServer;

    // 消息体内容 不对body进行序列化
    private transient byte[] body;

    protected RemotingCommand() {
    }

    public static RemotingCommand createRequestCommand(int code, CommandCustomHeader customHeader) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.setCode(code);
        cmd.customHeader = customHeader;
        setCmdVersion(cmd);
        return cmd;
    }

    private static void setCmdVersion(RemotingCommand cmd) {
        if (configVersion >= 0) {
            cmd.setVersion(configVersion);
        } else {
            String v = System.getProperty(REMOTING_VERSION_KEY);
            if (v != null) {
                int value = Integer.parseInt(v);
                cmd.setVersion(value);
                configVersion = value;
            }
        }
    }

    public static RemotingCommand createResponseCommand(Class<? extends CommandCustomHeader> classHeader) {
        return createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR, "not set any response code", classHeader);
    }

    public static RemotingCommand createResponseCommand(int code, String remark,
        Class<? extends CommandCustomHeader> classHeader) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.markResponseType();
        cmd.setCode(code);
        cmd.setRemark(remark);
        setCmdVersion(cmd);

        if (classHeader != null) {
            try {
                CommandCustomHeader objectHeader = classHeader.newInstance();
                cmd.customHeader = objectHeader;
            } catch (InstantiationException e) {
                return null;
            } catch (IllegalAccessException e) {
                return null;
            }
        }

        return cmd;
    }

    public static RemotingCommand createResponseCommand(int code, String remark) {
        return createResponseCommand(code, remark, null);
    }

    public static RemotingCommand decode(final byte[] array) {
        // 字节数组转化为ByteBuffer进行编码
        ByteBuffer byteBuffer = ByteBuffer.wrap(array);
        return decode(byteBuffer);
    }

    /**
     * 对消息进行解码
     * +----------------------+---------------------------------+-----------------------+---------------------------+
     * |  消息长度           | 序列化类型&头部长度              |  消息头数据           |  消息主体数据              |
     * |  4个字节           | 4字节(2,3,4个字节表示消息头长度) | 头部长度中描述的长度   |  消息长度 - 消息头长度     |
     * +-------------------+---------------------------------+------------------------+---------------------------+
     * @param byteBuffer
     * @return
     */
    public static RemotingCommand decode(final ByteBuffer byteBuffer) {
        /**
         * 获取消息的长度，不包括消息长度本身
         * {@link NettyDecoder#NettyDecoder()} 在构造方法中已经通过strip指定了要丢弃的长度
         */
        int length = byteBuffer.limit();
        // 获取消息头的长度
        int oriHeaderLen = byteBuffer.getInt();
        // 执行length & 0xFFFFFF，将int的后24位取出
        int headerLength = getHeaderLength(oriHeaderLen);

        byte[] headerData = new byte[headerLength];
        byteBuffer.get(headerData);

        // 将消息头进行解码
        RemotingCommand cmd = headerDecode(headerData, getProtocolType(oriHeaderLen));

        // 减去消息头4个字节和消息头长度剩下的为消息体
        int bodyLength = length - 4 - headerLength;
        byte[] bodyData = null;
        if (bodyLength > 0) {
            bodyData = new byte[bodyLength];
            byteBuffer.get(bodyData);
        }
        cmd.body = bodyData;

        return cmd;
    }

    /**
     * 获取int的后24位
     * @param length
     * @return
     */
    public static int getHeaderLength(int length) {
        return length & 0xFFFFFF;
    }

    private static RemotingCommand headerDecode(byte[] headerData, SerializeType type) {
        switch (type) {
            case JSON:
                // 调用RemotingSerializable中的decode解码json返回RemotingCommand
                RemotingCommand resultJson = RemotingSerializable.decode(headerData, RemotingCommand.class);
                resultJson.setSerializeTypeCurrentRPC(type);
                return resultJson;
            case ROCKETMQ:
                RemotingCommand resultRMQ = RocketMQSerializable.rocketMQProtocolDecode(headerData);
                resultRMQ.setSerializeTypeCurrentRPC(type);
                return resultRMQ;
            default:
                break;
        }

        return null;
    }

    /**
     * 获取序列化类型
     * @param source
     * @return
     */
    public static SerializeType getProtocolType(int source) {
        // 第一个字节为协议类型 右移24位， 前8位
        return SerializeType.valueOf((byte) ((source >> 24) & 0xFF));
    }

    /**
     * 创建消息id
     * @return
     */
    public static int createNewRequestId() {
        return requestId.incrementAndGet();
    }

    /**
     * 获取序列化类型
     * @return
     */
    public static SerializeType getSerializeTypeConfigInThisServer() {
        return serializeTypeConfigInThisServer;
    }

    // 是否是空字符串
    private static boolean isBlank(String str) {
        int strLen;
        if (str == null || (strLen = str.length()) == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if (!Character.isWhitespace(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * 设置
     * @param source
     * @param type
     * @return
     */
    public static byte[] markProtocolType(int source, SerializeType type) {
        // 长度为4字节数组
        byte[] result = new byte[4];

        // 序列化类型编码
        result[0] = type.getCode();
        // int 4个字节长度数字写入长度3个字节
        result[1] = (byte) ((source >> 16) & 0xFF);
        result[2] = (byte) ((source >> 8) & 0xFF);
        result[3] = (byte) (source & 0xFF);
        return result;
    }

    public void markResponseType() {
        int bits = 1 << RPC_TYPE;
        this.flag |= bits;
    }

    public CommandCustomHeader readCustomHeader() {
        return customHeader;
    }

    public void writeCustomHeader(CommandCustomHeader customHeader) {
        this.customHeader = customHeader;
    }

    /**
     * 解码头信息
     * @param classHeader
     * @return
     * @throws RemotingCommandException
     */
    public CommandCustomHeader decodeCommandCustomHeader(
        Class<? extends CommandCustomHeader> classHeader) throws RemotingCommandException {
        CommandCustomHeader objectHeader;
        try {
            // 创建一个头信息实例
            objectHeader = classHeader.newInstance();
        } catch (InstantiationException e) {
            return null;
        } catch (IllegalAccessException e) {
            return null;
        }

        if (this.extFields != null) {

            // 获取到该头信息对象的所有字段
            Field[] fields = getClazzFields(classHeader);
            for (Field field : fields) {
                // 拿到非静态字段
                if (!Modifier.isStatic(field.getModifiers())) {
                    // 反射赋值
                    String fieldName = field.getName();
                    if (!fieldName.startsWith("this")) {
                        try {
                            String value = this.extFields.get(fieldName);
                            if (null == value) {
                                if (!isFieldNullable(field)) {
                                    throw new RemotingCommandException("the custom field <" + fieldName + "> is null");
                                }
                                continue;
                            }

                            field.setAccessible(true);
                            String type = getCanonicalName(field.getType());
                            Object valueParsed;

                            if (type.equals(STRING_CANONICAL_NAME)) {
                                valueParsed = value;
                            } else if (type.equals(INTEGER_CANONICAL_NAME_1) || type.equals(INTEGER_CANONICAL_NAME_2)) {
                                valueParsed = Integer.parseInt(value);
                            } else if (type.equals(LONG_CANONICAL_NAME_1) || type.equals(LONG_CANONICAL_NAME_2)) {
                                valueParsed = Long.parseLong(value);
                            } else if (type.equals(BOOLEAN_CANONICAL_NAME_1) || type.equals(BOOLEAN_CANONICAL_NAME_2)) {
                                valueParsed = Boolean.parseBoolean(value);
                            } else if (type.equals(DOUBLE_CANONICAL_NAME_1) || type.equals(DOUBLE_CANONICAL_NAME_2)) {
                                valueParsed = Double.parseDouble(value);
                            } else {
                                throw new RemotingCommandException("the custom field <" + fieldName + "> type is not supported");
                            }

                            // 反射赋值
                            field.set(objectHeader, valueParsed);

                        } catch (Throwable e) {
                            log.error("Failed field [{}] decoding", fieldName, e);
                        }
                    }
                }
            }

            objectHeader.checkFields();
        }

        return objectHeader;
    }

    private Field[] getClazzFields(Class<? extends CommandCustomHeader> classHeader) {
        Field[] field = CLASS_HASH_MAP.get(classHeader);

        if (field == null) {
            field = classHeader.getDeclaredFields();
            synchronized (CLASS_HASH_MAP) {
                CLASS_HASH_MAP.put(classHeader, field);
            }
        }
        return field;
    }

    private boolean isFieldNullable(Field field) {
        if (!NULLABLE_FIELD_CACHE.containsKey(field)) {
            Annotation annotation = field.getAnnotation(CFNotNull.class);
            synchronized (NULLABLE_FIELD_CACHE) {
                NULLABLE_FIELD_CACHE.put(field, annotation == null);
            }
        }
        return NULLABLE_FIELD_CACHE.get(field);
    }

    private String getCanonicalName(Class clazz) {
        String name = CANONICAL_NAME_CACHE.get(clazz);

        if (name == null) {
            name = clazz.getCanonicalName();
            synchronized (CANONICAL_NAME_CACHE) {
                CANONICAL_NAME_CACHE.put(clazz, name);
            }
        }
        return name;
    }

    public ByteBuffer encode() {
        // 1> header length size 协议组成第二部分序列化类型和头长度，占用4个字节
        int length = 4;

        // 2> header data length 协议组成第三部分，首先得到消息头，再加上该消息头长度
        byte[] headerData = this.headerEncode();
        length += headerData.length;

        // 3> body data length 协议组成第四部分，消息体长度
        if (this.body != null) {
            length += body.length;
        }

        // 协议第一部分，加上记录整个整个消息长度的4个字节，申请缓冲区
        ByteBuffer result = ByteBuffer.allocate(4 + length);

        // length
        result.putInt(length);

        // header length
        result.put(markProtocolType(headerData.length, serializeTypeCurrentRPC));

        // header data
        result.put(headerData);

        // body data;
        if (this.body != null) {
            result.put(this.body);
        }

        // 写完后切换可读
        result.flip();

        return result;
    }

    /**
     * 头数据进行编码
     * @return
     */
    private byte[] headerEncode() {
        /**
         * 将消息头中自定义的字段写入extFields的hashMap
         */
        this.makeCustomHeaderToNet();
        /**
         * 判断目前采用的序列化方式 将RemotingCommand进行序列化，返回字节数组
         */
        if (SerializeType.ROCKETMQ == serializeTypeCurrentRPC) {
            return RocketMQSerializable.rocketMQProtocolEncode(this);
        } else {
            // this RemotingCommand [code=0, language=JAVA, version=0, opaque=0, flag(B)=0, remark=null, extFields={count=1, messageTitle=Welcome}, serializeTypeCurrentRPC=JSON]
            return RemotingSerializable.encode(this);
        }
    }

    /**
     * 将customHeader中定义的键值对参数写入extFields中，
     */
    public void makeCustomHeaderToNet() {
        if (this.customHeader != null) {
            // 获取所有声明的成员变量
            Field[] fields = getClazzFields(customHeader.getClass());
            if (null == this.extFields) {
                this.extFields = new HashMap<String, String>();
            }

            for (Field field : fields) {
                // 获取非静态变量
                if (!Modifier.isStatic(field.getModifiers())) {
                    // 成员变量名
                    String name = field.getName();
                    if (!name.startsWith("this")) {
                        Object value = null;
                        try {
                            field.setAccessible(true);
                            // 变量值
                            value = field.get(this.customHeader);
                        } catch (Exception e) {
                            log.error("Failed to access field [{}]", name, e);
                        }

                        if (value != null) {
                            // 写入自定义字段到hashMap中
                            this.extFields.put(name, value.toString());
                        }
                    }
                }
            }
        }
    }

    public ByteBuffer encodeHeader() {
        return encodeHeader(this.body != null ? this.body.length : 0);
    }

    /**
     * 完成了出body之外的消息转为bytebuffer的过程
     * 前四个字节存放的是整个消息的长度(但是这个长度不包括前四个字节)
     * @param bodyLength
     * @return
     */
    public ByteBuffer encodeHeader(final int bodyLength) {
        // 1> header length size
        int length = 4;

        // 2> header data length
        byte[] headerData;
        headerData = this.headerEncode();

        length += headerData.length;

        // 3> body data length
        length += bodyLength;

        ByteBuffer result = ByteBuffer.allocate(4 + length - bodyLength);

        // length
        result.putInt(length);

        // header length
        result.put(markProtocolType(headerData.length, serializeTypeCurrentRPC));

        // header data
        result.put(headerData);

        result.flip();

        return result;
    }

    public void markOnewayRPC() {
        int bits = 1 << RPC_ONEWAY;
        this.flag |= bits;
    }

    @JSONField(serialize = false)
    public boolean isOnewayRPC() {
        int bits = 1 << RPC_ONEWAY;
        return (this.flag & bits) == bits;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    @JSONField(serialize = false)
    public RemotingCommandType getType() {
        if (this.isResponseType()) {
            return RemotingCommandType.RESPONSE_COMMAND;
        }

        return RemotingCommandType.REQUEST_COMMAND;
    }

    @JSONField(serialize = false)
    public boolean isResponseType() {
        int bits = 1 << RPC_TYPE;
        return (this.flag & bits) == bits;
    }

    public LanguageCode getLanguage() {
        return language;
    }

    public void setLanguage(LanguageCode language) {
        this.language = language;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getOpaque() {
        return opaque;
    }

    public void setOpaque(int opaque) {
        this.opaque = opaque;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public HashMap<String, String> getExtFields() {
        return extFields;
    }

    public void setExtFields(HashMap<String, String> extFields) {
        this.extFields = extFields;
    }

    public void addExtField(String key, String value) {
        if (null == extFields) {
            extFields = new HashMap<String, String>();
        }
        extFields.put(key, value);
    }

    @Override
    public String toString() {
        return "RemotingCommand [code=" + code + ", language=" + language + ", version=" + version + ", opaque=" + opaque + ", flag(B)="
            + Integer.toBinaryString(flag) + ", remark=" + remark + ", extFields=" + extFields + ", serializeTypeCurrentRPC="
            + serializeTypeCurrentRPC + "]";
    }

    public SerializeType getSerializeTypeCurrentRPC() {
        return serializeTypeCurrentRPC;
    }

    public void setSerializeTypeCurrentRPC(SerializeType serializeTypeCurrentRPC) {
        this.serializeTypeCurrentRPC = serializeTypeCurrentRPC;
    }
}