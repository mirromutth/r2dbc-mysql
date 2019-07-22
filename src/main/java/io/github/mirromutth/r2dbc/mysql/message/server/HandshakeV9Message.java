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

import static io.github.mirromutth.r2dbc.mysql.constant.EmptyArrays.EMPTY_BYTES;
import static io.github.mirromutth.r2dbc.mysql.internal.AssertUtils.requireNonNull;

/**
 * TODO: write comment for this class or object
 */
final class HandshakeV9Message extends AbstractHandshakeMessage {

    private final byte[] salt;

    private HandshakeV9Message(HandshakeHeader header, byte[] salt) {
        super(header);
        this.salt = requireNonNull(salt, "salt must not be null");
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

    static HandshakeV9Message decodeV9(ByteBuf buf, HandshakeHeader header) {
        int bytes = buf.readableBytes();

        if (bytes <= 0) {
            return new HandshakeV9Message(header, EMPTY_BYTES);
        }

        byte[] salt;

        if (buf.getByte(buf.writerIndex() - 1) == 0) {
            salt = ByteBufUtil.getBytes(buf, buf.readerIndex(),  bytes - 1);
        } else {
            salt = ByteBufUtil.getBytes(buf);
        }

        return new HandshakeV9Message(header, salt);
    }
}
