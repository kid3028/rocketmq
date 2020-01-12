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
package org.apache.rocketmq.common.sysflag;

/**
 * 消息类型
 */
public class MessageSysFlag {
    //
    // 1   0000_0001
    public final static int COMPRESSED_FLAG = 0x1;
    // 2  0000_0010
    public final static int MULTI_TAGS_FLAG = 0x1 << 1;
    // 0 0000_0000
    public final static int TRANSACTION_NOT_TYPE = 0;
    // 4 0000_0100
    public final static int TRANSACTION_PREPARED_TYPE = 0x1 << 2;
    // 8 0000_0010 -> 0000_1000
    public final static int TRANSACTION_COMMIT_TYPE = 0x2 << 2;
    // 12 0000_0011 ->  0000_1100
    public final static int TRANSACTION_ROLLBACK_TYPE = 0x3 << 2;

    /**
     * 如果 & 之后的结果不为0，那么flag为 TRANSACTION_ROLLBACK_TYPE、 TRANSACTION_COMMIT_TYPE、 TRANSACTION_PREPARED_TYPE ，即消息类型为事务类消息
     * @param flag
     * @return
     */
    public static int getTransactionValue(final int flag) {
        return flag & TRANSACTION_ROLLBACK_TYPE;
    }

    public static int resetTransactionValue(final int flag, final int type) {
        return (flag & (~TRANSACTION_ROLLBACK_TYPE)) | type;
    }

    public static int clearCompressedFlag(final int flag) {
        return flag & (~COMPRESSED_FLAG);
    }
}
