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

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * Authentication more data request, means continue send auth change response message if is exists.
 * <p>
 * Note: should implement {@link CompleteMessage} if need support full authentication
 * for "caching_sha2_password" or "sha256_password". (and connection should support SSL)
 */
public final class AuthMoreDataMessage implements ServerMessage {

    private final byte[] authMethodData;

    private AuthMoreDataMessage(byte[] authMethodData) {
        this.authMethodData = requireNonNull(authMethodData, "authMethodData must not be null");
    }

    public byte[] getAuthMethodData() {
        return authMethodData;
    }

    static AuthMoreDataMessage decode(ByteBuf buf) {
        buf.skipBytes(1); // auth more data message header, 0x01
        return new AuthMoreDataMessage(ByteBufUtil.getBytes(buf));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AuthMoreDataMessage)) {
            return false;
        }

        AuthMoreDataMessage that = (AuthMoreDataMessage) o;

        return Arrays.equals(authMethodData, that.authMethodData);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(authMethodData);
    }

    @Override
    public String toString() {
        return "AuthMoreDataMessage{" +
            "authMethodData=" + Arrays.toString(authMethodData) +
            '}';
    }
}
