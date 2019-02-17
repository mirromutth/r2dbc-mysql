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

import io.github.mirromutth.r2dbc.mysql.core.ServerSession;
import io.netty.buffer.ByteBuf;

/**
 * All response messages generic decode logic with type router.
 */
abstract class AbstractResponse implements BackendMessage {

    static AbstractResponse decode(ByteBuf buf, ServerSession session) {
        switch (ResponseType.valueOfCode(buf.readByte())) {
            case OK:
                // TODO: OK packet
                throw new IllegalStateException();
            case ERROR:
                throw new IllegalStateException();
            case EOF:
                if (session.getServerVersion().compareTo(5, 7, 5) >= 0) {
                    // TODO: OK packet
                    throw new IllegalStateException();
                } else {
                    // TODO: EOF packet
                    throw new IllegalStateException();
                }
        }
        return null;
    }

    private enum ResponseType {
        OK((byte) 0),
        ERROR((byte) 0xFF),
        EOF((byte) 0xFE);

        private static final ResponseType[] VALUES = values();

        private final byte code;

        ResponseType(byte code) {
            this.code = code;
        }

        static ResponseType valueOfCode(byte code) {
            for (ResponseType type : VALUES) {
                if (type.code == code) {
                    return type;
                }
            }

            // TODO: use custom exception
            throw new IllegalStateException();
        }
    }
}
