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

package io.jafka.api;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import io.jafka.network.Request;

/**
 * Multi fetch request
 *
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public record MultiFetchRequest(List<FetchRequest> fetches) implements Request {

    public RequestKeys getRequestKey() {
        return RequestKeys.MULTIFETCH;
    }

    /**
     * @return the fetches
     */
    @Override
    public List<FetchRequest> fetches() {
        return fetches;
    }

    public int getSizeInBytes() {
        int size = 2;
        for (FetchRequest fetch : fetches) {
            size += fetch.getSizeInBytes();
        }
        return size;
    }

    public void writeTo(ByteBuffer buffer) {
        if (fetches.size() > Short.MAX_VALUE) { //max 32767
            throw new IllegalArgumentException("Number of requests in MultiFetchRequest exceeds " + Short.MAX_VALUE + ".");
        }
        buffer.putShort((short) fetches.size());
        for (FetchRequest fetch : fetches) {
            fetch.writeTo(buffer);
        }
    }

    public static MultiFetchRequest readFrom(ByteBuffer buffer) {
        int count = buffer.getShort();
        List<FetchRequest> fetches = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            fetches.add(FetchRequest.readFrom(buffer));
        }
        return new MultiFetchRequest(fetches);
    }
}
