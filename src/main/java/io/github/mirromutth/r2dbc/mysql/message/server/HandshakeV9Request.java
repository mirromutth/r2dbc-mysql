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

package io.github.mirromutth.r2dbc.mysql.message.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

import java.util.Arrays;

import static io.github.mirromutth.r2dbc.mysql.constant.EmptyArrays.EMPTY_BYTES;
import static io.github.mirromutth.r2dbc.mysql.internal.AssertUtils.requireNonNull;

/**
 * TODO: write comment for this class or object
 */
final class HandshakeV9Request implements HandshakeRequest {

    private final HandshakeHeader header;

    private final byte[] salt;

    private HandshakeV9Request(HandshakeHeader header, byte[] salt) {
        this.header = requireNonNull(header, "header must not be null");
        this.salt = requireNonNull(salt, "salt must not be null");
    }

    @Override
    public HandshakeHeader getHeader() {
        return header;
    }

    @Override
    public int getServerCapabilities() {
        // Unsupported anything.
        return 0;
    }

    @Override
    public String getAuthType() {
        // TODO: old_password
        return "";
    }

    @Override
    public byte[] getSalt() {
        return salt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HandshakeV9Request)) {
            return false;
        }

        HandshakeV9Request that = (HandshakeV9Request) o;

        if (!header.equals(that.header)) {
            return false;
        }
        return Arrays.equals(salt, that.salt);
    }

    @Override
    public int hashCode() {
        int result = header.hashCode();
        result = 31 * result + Arrays.hashCode(salt);
        return result;
    }

    @Override
    public String toString() {
        return String.format("HandshakeV9Request{header=%s, salt=REDACTED}", header);
    }

    static HandshakeV9Request decodeV9(ByteBuf buf, HandshakeHeader header) {
        int bytes = buf.readableBytes();

        if (bytes <= 0) {
            return new HandshakeV9Request(header, EMPTY_BYTES);
        }

        byte[] salt;

        if (buf.getByte(buf.writerIndex() - 1) == 0) {
            salt = ByteBufUtil.getBytes(buf, buf.readerIndex(),  bytes - 1);
        } else {
            salt = ByteBufUtil.getBytes(buf);
        }

        return new HandshakeV9Request(header, salt);
    }
}
