/*
 * Copyright 2018-2020 the original author or authors.
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

import io.netty.buffer.ByteBuf;

import java.util.Arrays;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * A message that contains only an authentication, used by full authentication or change authentication
 * response.
 */
public final class AuthResponse extends SizedClientMessage implements LoginClientMessage {

    private final int envelopeId;

    private final byte[] authentication;

    public AuthResponse(int envelopeId, byte[] authentication) {
        this.envelopeId = envelopeId;
        this.authentication = requireNonNull(authentication, "authentication must not be null");
    }

    @Override
    public int getEnvelopeId() {
        return envelopeId;
    }

    @Override
    protected int size() {
        return authentication.length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AuthResponse that = (AuthResponse) o;

        return envelopeId == that.envelopeId && Arrays.equals(authentication, that.authentication);
    }

    @Override
    public int hashCode() {
        return 31 * envelopeId + Arrays.hashCode(authentication);
    }

    @Override
    public String toString() {
        return "AuthResponse{envelopeId=" + envelopeId + ", authentication=REDACTED}";
    }

    @Override
    protected void writeTo(ByteBuf buf) {
        buf.writeBytes(authentication);
    }
}
