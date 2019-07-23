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

package io.github.mirromutth.r2dbc.mysql.message.client;

import io.github.mirromutth.r2dbc.mysql.internal.MySqlSession;
import io.netty.buffer.ByteBuf;

import java.util.Arrays;

import static io.github.mirromutth.r2dbc.mysql.internal.AssertUtils.requireNonNull;

/**
 * Full authentication response, i.e. authentication change response.
 */
public final class FullAuthResponse extends EnvelopeClientMessage implements ExchangeableMessage {

    private final byte[] authentication;

    public FullAuthResponse(byte[] authentication) {
        this.authentication = requireNonNull(authentication, "authentication must not be null");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FullAuthResponse)) {
            return false;
        }

        FullAuthResponse that = (FullAuthResponse) o;

        return Arrays.equals(authentication, that.authentication);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(authentication);
    }

    @Override
    public String toString() {
        return "FullAuthResponse{authentication=REDACTED}";
    }

    @Override
    protected void writeTo(ByteBuf buf, MySqlSession session) {
        buf.writeBytes(authentication);
    }
}
