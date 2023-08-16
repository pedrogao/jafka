/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.jafka.producer.serializer;

import java.nio.ByteBuffer;

import io.jafka.message.Message;

/**
 * a bytes en/decoder
 *
 * @author adyliu (imxylz@gmail.com)
 * @since 1.1
 */
public class ByteArrayEncoders implements Encoder<byte[]>, Decoder<byte[]> {

    @Override
    public Message toMessage(byte[] event) {
        return new Message(event);
    }

    @Override
    public byte[] toEvent(Message message) {
        ByteBuffer buf = message.payload();
        byte[] b = new byte[buf.remaining()];
        buf.get(b);
        return b;
    }
}
