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

/**
 * Codec for {@link String}.
 */
final class StringCodec extends AbstractClassedCodec<String> {

    static final StringCodec INSTANCE = new StringCodec();

    private StringCodec() {
        super(String.class);
    }

    @Override
    public String decodeText(NormalFieldValue value, FieldInformation info, Class<? super String> target, MySqlSession session) {
        return decode(value.getBuffer(), info, session);
    }

    @Override
    public String decodeBinary(NormalFieldValue value, FieldInformation info, Class<? super String> target, MySqlSession session) {
        return decode(value.getBuffer(), info, session);
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof CharSequence;
    }

    @Override
    public ParameterValue encode(Object value, MySqlSession session) {
        return new StringValue((CharSequence) value, session);
    }

    @Override
    protected boolean doCanDecode(FieldInformation info) {
        DataType type = info.getType();
        // Note: TEXT is also BLOB with char collation in MySQL.
        return (TypeConditions.isString(type) || TypeConditions.isLob(type)) && info.getCollationId() != CharCollation.BINARY_ID;
    }

    private static String decode(ByteBuf buf, FieldInformation info, MySqlSession session) {
        if (!buf.isReadable()) {
            return "";
        }

        return buf.toString(CharCollation.fromId(info.getCollationId(), session.getServerVersion()).getCharset());
    }

    private static class StringValue extends AbstractParameterValue {

        private final CharSequence value;

        private final MySqlSession session;

        private StringValue(CharSequence value, MySqlSession session) {
            this.value = value;
            this.session = session;
        }

        @Override
        public Mono<Void> writeTo(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.writeCharSequence(value, session.getCollation()));
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
            if (!(o instanceof StringValue)) {
                return false;
            }

            StringValue that = (StringValue) o;

            return value.equals(that.value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }
    }
}
