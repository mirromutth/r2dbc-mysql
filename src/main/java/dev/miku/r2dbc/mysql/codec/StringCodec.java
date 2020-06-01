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

package dev.miku.r2dbc.mysql.codec;

import dev.miku.r2dbc.mysql.ParameterOutputStream;
import dev.miku.r2dbc.mysql.ParameterWriter;
import dev.miku.r2dbc.mysql.collation.CharCollation;
import dev.miku.r2dbc.mysql.constant.DataTypes;
import dev.miku.r2dbc.mysql.Parameter;
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
    public String decode(ByteBuf value, FieldInformation info, Class<?> target, boolean binary, CodecContext context) {
        if (!value.isReadable()) {
            return "";
        }

        return value.toString(CharCollation.fromId(info.getCollationId(), context.getServerVersion()).getCharset());
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof CharSequence;
    }

    @Override
    public Parameter encode(Object value, CodecContext context) {
        return new StringParameter((CharSequence) value, context);
    }

    @Override
    protected boolean doCanDecode(FieldInformation info) {
        short type = info.getType();
        // Note: TEXT is also BLOB with char collation in MySQL.
        return (TypePredicates.isString(type) || TypePredicates.isLob(type)) && info.getCollationId() != CharCollation.BINARY_ID;
    }

    private static class StringParameter extends AbstractParameter {

        private final CharSequence value;

        private final CodecContext context;

        private StringParameter(CharSequence value, CodecContext context) {
            this.value = value;
            this.context = context;
        }

        @Override
        public Mono<Void> binary(ParameterOutputStream output) {
            return Mono.fromRunnable(() -> output.writeCharSequence(value, context.getClientCollation()));
        }

        @Override
        public Mono<Void> text(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.append(value));
        }

        @Override
        public short getType() {
            return DataTypes.VARCHAR;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof StringParameter)) {
                return false;
            }

            StringParameter that = (StringParameter) o;

            return value.equals(that.value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }
    }
}
