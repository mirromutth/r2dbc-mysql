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

package dev.miku.r2dbc.mysql.message.client;

import dev.miku.r2dbc.mysql.constant.Capabilities;
import dev.miku.r2dbc.mysql.constant.Envelopes;
import dev.miku.r2dbc.mysql.internal.AssertUtils;
import io.netty.buffer.ByteBuf;

/**
 * The ssl request message on protocol 3.20. It is also first part of {@link HandshakeResponse320}.
 */
final class SslRequest320 extends FixedSizeClientMessage implements SslRequest {

    private static final int SIZE = Short.BYTES + Envelopes.SIZE_FIELD_SIZE;

    private final int capabilities;

    SslRequest320(int capabilities) {
        AssertUtils.require((capabilities & Capabilities.PROTOCOL_41) == 0, "protocol 4.1 capability should never be set");

        this.capabilities = capabilities;
    }

    @Override
    public int getCapabilities() {
        return capabilities;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SslRequest320)) {
            return false;
        }

        SslRequest320 that = (SslRequest320) o;

        return capabilities == that.capabilities;
    }

    @Override
    public int hashCode() {
        return capabilities;
    }

    @Override
    public String toString() {
        return String.format("SslRequest320{capabilities=%x}", capabilities);
    }

    @Override
    protected int size() {
        return SIZE;
    }

    @Override
    protected void writeTo(ByteBuf buf) {
        buf.writeShortLE(capabilities & 0xFFFF) // only low 16-bits
            .writeMediumLE(Envelopes.MAX_ENVELOPE_SIZE);
    }
}
