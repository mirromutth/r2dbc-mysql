/*
 * Copyright 2018-2021 the original author or authors.
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

import dev.miku.r2dbc.mysql.Capability;
import dev.miku.r2dbc.mysql.constant.Envelopes;
import io.netty.buffer.ByteBuf;

import static dev.miku.r2dbc.mysql.util.AssertUtils.require;

/**
 * The ssl request message on protocol 3.20. It is also first part of {@link HandshakeResponse320}.
 */
final class SslRequest320 extends SizedClientMessage implements SslRequest {

    private static final int SIZE = Short.BYTES + Envelopes.SIZE_FIELD_SIZE;

    private final int envelopeId;

    private final Capability capability;

    SslRequest320(int envelopeId, Capability capability) {
        require(!capability.isProtocol41(), "protocol 4.1 capability should never be set");

        this.envelopeId = envelopeId;
        this.capability = capability;
    }

    @Override
    public int getEnvelopeId() {
        return envelopeId;
    }

    @Override
    public Capability getCapability() {
        return capability;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SslRequest320 that = (SslRequest320) o;

        return envelopeId == that.envelopeId && capability.equals(that.capability);
    }

    @Override
    public int hashCode() {
        return 31 * envelopeId + capability.hashCode();
    }

    @Override
    public String toString() {
        return "SslRequest320{envelopeId=" + envelopeId +
            ", capability=" + capability + '}';
    }

    @Override
    protected int size() {
        return SIZE;
    }

    @Override
    protected void writeTo(ByteBuf buf) {
        // Protocol 3.20 only allows low 16-bits capabilities.
        buf.writeShortLE(capability.getBitmap() & 0xFFFF)
            .writeMediumLE(Envelopes.MAX_ENVELOPE_SIZE);
    }
}
