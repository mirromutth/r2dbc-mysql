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

import io.github.mirromutth.r2dbc.mysql.constant.Packets;

/**
 * All packet have packet header, and have a unified "has a next part" logic.
 */
public abstract class AbstractPacket implements Packet {

    private final PacketHeader packetHeader;

    protected AbstractPacket(PacketHeader packetHeader) {
        this.packetHeader = packetHeader;
    }

    @Override
    public final PacketHeader getPacketHeader() {
        return packetHeader;
    }

    @Override
    public final boolean hasNext() {
        return packetHeader.getByteSize() == Packets.MAX_PART_SIZE;
    }
}
