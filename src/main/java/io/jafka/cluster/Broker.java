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

package io.jafka.cluster;

import io.jafka.server.ServerRegister;

/**
 * messages store broker
 *
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class Broker {

    /**
     * the global unique broker id
     */
    public final int id;

    /**
     * the broker creator (hostname with created time)
     *
     * @see ServerRegister#registerBrokerInZk()
     */
    public final String creatorId;

    /**
     * broker hostname
     */
    public final String host;

    /**
     * broker port
     */
    public final int port;

    public final boolean autoCreated;

    /**
     * create a broker
     *
     * @param id        broker id
     * @param creatorId the creator id
     * @param host      broker hostname
     * @param port      broker port
     * @param autoCreated auto-create new topics
     */
    public Broker(int id, String creatorId, String host, int port, boolean autoCreated) {
        super();
        this.id = id;
        this.creatorId = creatorId;
        this.host = host;
        this.port = port;
        this.autoCreated = autoCreated;
    }

    /**
     * the broker info saved in zookeeper
     * <p>
     * format: <b>creatorId:host:port</b>
     *
     * @return broker info saved in zookeeper
     */
    public String getZKString() {
        return String.format("%s:%s:%s:%s", creatorId.replace(':', '#'), host.replace(':', '#'), port, autoCreated);
    }

    @Override
    public String toString() {
        return getZKString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Broker broker = (Broker) o;

        if (autoCreated != broker.autoCreated) return false;
        if (id != broker.id) return false;
        if (port != broker.port) return false;
        if (host != null ? !host.equals(broker.host) : broker.host != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + (host != null ? host.hashCode() : 0);
        result = 31 * result + port;
        result = 31 * result + (autoCreated ? 1 : 0);
        return result;
    }

    /**
     * create a broker with given broker info
     *
     * @param id               broker id
     * @param brokerInfoString broker info format: <b>creatorId:host:port:autocreated</b>
     * @return broker instance with connection config
     * @see #getZKString()
     */
    public static Broker createBroker(int id, String brokerInfoString) {
        String[] brokerInfo = brokerInfoString.split(":");
        String creator = brokerInfo[0].replace('#', ':');
        String hostname = brokerInfo[1].replace('#', ':');
        String port = brokerInfo[2];
        boolean autoCreated = Boolean.valueOf(brokerInfo.length > 3 ? brokerInfo[3] : "true");
        return new Broker(id, creator, hostname, Integer.parseInt(port), autoCreated);
    }

}
