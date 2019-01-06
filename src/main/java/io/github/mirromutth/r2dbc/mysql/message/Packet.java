/*
 * Copyright 2018-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.mirromutth.r2dbc.mysql.message;

/**
 * All packet interfaces, whether from server to client or from client to server.
 */
public interface Packet {

    PacketHeader getPacketHeader();

    /**
     * If it is the part of a huge packet, and it is not last part,
     * server should send next part of this huge packet.
     *
     * @return {@code true} if this message is not last part of a huge packet
     */
    boolean hasNext();
}
