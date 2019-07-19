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

package io.github.mirromutth.r2dbc.mysql.codec;

import io.github.mirromutth.r2dbc.mysql.collation.CharCollation;
import io.github.mirromutth.r2dbc.mysql.constant.DataType;
import io.github.mirromutth.r2dbc.mysql.internal.MySqlSession;
import io.github.mirromutth.r2dbc.mysql.message.NormalFieldValue;
import io.github.mirromutth.r2dbc.mysql.message.ParameterValue;
import io.github.mirromutth.r2dbc.mysql.message.client.ParameterWriter;
import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Mono;

import java.nio.charset.Charset;
import java.util.Arrays;

import static io.github.mirromutth.r2dbc.mysql.constant.EmptyArrays.EMPTY_STRINGS;

/**
 * Codec for {@link String[]}.
 */
final class StringArrayCodec extends AbstractClassedCodec<String[]> {

    static final StringArrayCodec INSTANCE = new StringArrayCodec();

    private StringArrayCodec() {
        super(String[].class);
    }

    @Override
    public String[] decode(NormalFieldValue value, FieldInformation info, Class<? super String[]> target, boolean binary, MySqlSession session) {
        ByteBuf buf = value.getBuffer();

        if (!buf.isReadable()) {
            return EMPTY_STRINGS;
        }

        int firstComma = buf.indexOf(buf.readerIndex(), buf.writerIndex(), (byte) ',');
        Charset charset = CharCollation.fromId(info.getCollationId(), session.getServerVersion()).getCharset();

        if (firstComma < 0) {
            return new String[] { buf.toString(charset) };
        }

        return buf.toString(charset).split(",");
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof CharSequence[];
    }

    @Override
    public ParameterValue encode(Object value, MySqlSession session) {
        return new StringArrayValue((CharSequence[]) value, session);
    }

    @Override
    protected boolean doCanDecode(FieldInformation info) {
        return DataType.SET == info.getType();
    }

    private static final class StringArrayValue extends AbstractParameterValue {

        private final CharSequence[] value;

        private final MySqlSession session;

        private StringArrayValue(CharSequence[] value, MySqlSession session) {
            this.value = value;
            this.session = session;
        }

        @Override
        public Mono<Void> writeTo(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.writeSet(Arrays.asList(value), session.getCollation()));
        }

        @Override
        public int getNativeType() {
            return DataType.VARCHAR.getType();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof StringArrayValue)) {
                return false;
            }

            StringArrayValue that = (StringArrayValue) o;

            return Arrays.equals(this.value, that.value);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(value);
        }
    }
}
