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

package io.github.mirromutth.r2dbc.mysql.message.backend;

import io.github.mirromutth.r2dbc.mysql.constant.AuthType;
import io.github.mirromutth.r2dbc.mysql.util.CodecUtils;
import io.github.mirromutth.r2dbc.mysql.util.EnumUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * Change authentication plugin and scramble message.
 */
final class AuthChangeMessage implements BackendMessage {

    private final AuthType authType;

    private final byte[] scramble;

    private AuthChangeMessage(AuthType authType, byte[] scramble) {
        this.authType = requireNonNull(authType, "authType must not be null");
        this.scramble = requireNonNull(scramble, "providedData must not be null");
    }

    public AuthType getAuthType() {
        return authType;
    }

    public byte[] getScramble() {
        return scramble;
    }

    static AuthChangeMessage decode(ByteBuf buf) {
        AuthType authType = EnumUtils.authType(CodecUtils.readCString(buf, StandardCharsets.US_ASCII));
        return new AuthChangeMessage(authType, ByteBufUtil.getBytes(buf));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AuthChangeMessage)) {
            return false;
        }

        AuthChangeMessage that = (AuthChangeMessage) o;

        if (authType != that.authType) {
            return false;
        }
        return Arrays.equals(scramble, that.scramble);
    }

    @Override
    public int hashCode() {
        int result = authType.hashCode();
        result = 31 * result + Arrays.hashCode(scramble);
        return result;
    }

    @Override
    public String toString() {
        return "AuthChangeMessage{" +
            "authType=" + authType +
            ", scramble=" + Arrays.toString(scramble) +
            '}';
    }
}
