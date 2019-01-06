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
 * The header of all packets.
 */
public final class PacketHeader {

    private final int byteSize;

    private final short sequenceId;

    public PacketHeader(int byteSize, short sequenceId) {
        this.byteSize = byteSize;
        this.sequenceId = sequenceId;
    }

    public int getByteSize() {
        return byteSize;
    }

    public short getSequenceId() {
        return sequenceId;
    }

    @Override
    public String toString() {
        return "PacketHeader{" +
            "byteSize=" + byteSize +
            ", sequenceId=" + sequenceId +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PacketHeader that = (PacketHeader) o;

        if (byteSize != that.byteSize) {
            return false;
        }
        return sequenceId == that.sequenceId;
    }

    @Override
    public int hashCode() {
        int result = byteSize;
        result = 31 * result + (int) sequenceId;
        return result;
    }
}
