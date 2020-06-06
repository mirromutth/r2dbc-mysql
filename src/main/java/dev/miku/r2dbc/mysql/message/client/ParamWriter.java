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

import dev.miku.r2dbc.mysql.Parameter;
import dev.miku.r2dbc.mysql.ParameterWriter;
import dev.miku.r2dbc.mysql.util.OperatorUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * A default implementation of {@link ParameterWriter}.
 * <p>
 * WARNING: It is not safe for multithreaded access.
 */
final class ParamWriter extends ParameterWriter {

    private static final char[] HEX = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };

    private static final Consumer<Parameter> DISPOSE = Parameter::dispose;

    private final StringBuilder builder;

    private final Iterator<String> sql;

    private Mode mode;

    private ParamWriter(StringBuilder builder, Iterator<String> sql) {
        this.builder = builder;
        this.sql = sql;
        this.mode = sql.hasNext() ? Mode.AVAILABLE : Mode.FULL;
    }

    @Override
    public void writeNull() {
        startAvailable(Mode.NULL);

        builder.append("NULL");
    }

    @Override
    public void writeInt(int value) {
        startAvailable(Mode.NUMERIC);

        builder.append(value);
    }

    @Override
    public void writeLong(long value) {
        startAvailable(Mode.NUMERIC);

        builder.append(value);
    }

    @Override
    public void writeBigInteger(BigInteger value) {
        requireNonNull(value, "value must not be null");

        startAvailable(Mode.NUMERIC);
        builder.append(value.toString());
    }

    @Override
    public void writeFloat(float value) {
        startAvailable(Mode.NUMERIC);

        builder.append(value);
    }

    @Override
    public void writeDouble(double value) {
        startAvailable(Mode.NUMERIC);

        builder.append(value);
    }

    @Override
    public void writeBigDecimal(BigDecimal value) {
        requireNonNull(value, "value must not be null");

        startAvailable(Mode.NUMERIC);
        builder.append(value.toString());
    }

    @Override
    public void writeBinary(boolean bit) {
        startAvailable(Mode.BINARY);

        builder.append(bit ? '1' : '0');
    }

    @Override
    public void startHex() {
        startAvailable(Mode.HEX);
    }

    @Override
    public void writeHex(ByteBuffer buffer) {
        requireNonNull(buffer, "buffer must not be null");

        startAvailable(Mode.HEX);

        int limit = buffer.limit();
        for (int i = buffer.position(); i < limit; ++i) {
            byte b = buffer.get(i);
            builder.append(HEX[(b & 0xF0) >>> 4])
                .append(HEX[b & 0xF]);
        }
    }

    @Override
    public void writeHex(byte[] bytes) {
        requireNonNull(bytes, "bytes must not be null");

        startAvailable(Mode.HEX);

        for (byte b : bytes) {
            builder.append(HEX[(b & 0xF0) >>> 4])
                .append(HEX[b & 0xF]);
        }
    }

    @Override
    public void startString() {
        startAvailable(Mode.STRING);
    }

    @Override
    public void write(int c) {
        startAvailable(Mode.STRING);

        escape((char) c);
    }

    @Override
    public ParameterWriter append(char c) {
        startAvailable(Mode.STRING);

        escape(c);
        return this;
    }

    @Override
    public ParameterWriter append(@Nullable CharSequence csq) {
        CharSequence s = csq == null ? "null" : csq;

        return append0(s, 0, s.length());
    }

    @Override
    public ParameterWriter append(@Nullable CharSequence csq, int start, int end) {
        CharSequence s = csq == null ? "null" : csq;

        if (start < 0 || start > s.length() || end < start || end > s.length()) {
            throw new IndexOutOfBoundsException("start: " + start + ", end: " + end + ", str length: " + s.length());
        }

        return append0(s, start, end);
    }

    @Override
    public void write(@Nullable String str) {
        String s = str == null ? "null" : str;

        write0(s, 0, s.length());
    }

    @Override
    public void write(@Nullable String str, int off, int len) {
        String s = str == null ? "null" : str;

        if (off < 0 || off > s.length() || len < 0 || off + len > s.length() || off + len < 0) {
            throw new IndexOutOfBoundsException("off: " + off + ", len: " + len + ", str length: " + s.length());
        }

        write0(s, off, len);
    }

    @Override
    public void write(@Nullable char[] c) {
        if (c == null) {
            write((String) null);
            return;
        }

        write0(c, 0, c.length);
    }

    @Override
    public void write(@Nullable char[] c, int off, int len) {
        if (c == null) {
            write((String) null, off, len);
            return;
        }

        if (off < 0 || off > c.length || len < 0 || off + len > c.length || off + len < 0) {
            throw new IndexOutOfBoundsException("off: " + off + ", len: " + len + ", chars length: " + c.length);
        }

        write0(c, off, len);
    }

    private String toSql() {
        if (this.mode != Mode.FULL) {
            throw new IllegalStateException("Unexpected completion, parameters are not filled");
        }

        return this.builder.toString();
    }

    private void startAvailable(Mode mode) {
        Mode current = this.mode;

        if (current == Mode.AVAILABLE) {
            this.mode = mode;
            mode.start(this.builder);
            return;
        } else if (current.canFollow(mode)) {
            return;
        }

        if (current == Mode.FULL) {
            throw new IllegalStateException("Unexpected write, parameters are filled-up");
        } else {
            throw new IllegalStateException("Unexpected write, current mode is " + current + ", but write with " + mode);
        }
    }

    private void flushParameter(Void ignored) {
        Mode current = this.mode;
        if (current == Mode.FULL) {
            return;
        }

        if (current == Mode.AVAILABLE) {
            // This parameter never be filled, filling with STRING mode by default.
            this.builder.append('\'').append('\'');
        }

        current.end(this.builder);

        this.builder.append(sql.next());
        this.mode = sql.hasNext() ? Mode.AVAILABLE : Mode.FULL;
    }

    private ParamWriter append0(CharSequence csq, int start, int end) {
        startAvailable(Mode.STRING);

        for (int i = start; i < end; ++i) {
            escape(csq.charAt(i));
        }

        return this;
    }

    private void write0(String s, int off, int len) {
        startAvailable(Mode.STRING);

        int end = len + off;
        for (int i = off; i < end; ++i) {
            escape(s.charAt(i));
        }
    }

    private void write0(char[] s, int off, int len) {
        startAvailable(Mode.STRING);

        int end = len + off;
        for (int i = off; i < end; ++i) {
            escape(s[i]);
        }
    }

    private void escape(char c) {
        switch (c) {
            case '\\':
                builder.append('\\').append('\\');
                break;
            case '\'':
                // MySQL will auto-combine consecutive strings, like '1''2' -> '12'.
                // Sure, there can use "\\'", but this will be better. (For some logging systems)
                builder.append('\'').append('\'');
                break;
            // Maybe useful in the future, keep '"' here.
            // case '"': buf.append('\\').append('"'); break;
            // SHIFT-JIS, WINDOWS-932, EUC-JP and eucJP-OPEN will encode '\u00a5' (the sign of Japanese Yen
            // or Chinese Yuan) to '\' (ASCII 92). X-IBM949, X-IBM949C will encode '\u20a9' (the sign of
            // Korean Won) to '\'. It is nothing because the driver is using UTF-8. See also CharCollation.
            // case '\u00a5': do something; break;
            // case '\u20a9': do something; break;
            case 0:
                // MySQL is based on C/C++, must escape '\0' which is an end flag in C style string.
                builder.append('\\').append('0');
                break;
            case '\032':
                // It seems like a problem on Windows 32, maybe check current OS here?
                builder.append('\\').append('Z');
                break;
            case '\n':
                // Should escape it for some logging such as Relational Database Service (RDS) Logging System, etc.
                // Sure, it is not necessary, but this will be better.
                builder.append('\\').append('n');
                break;
            case '\r':
                // Should escape it for some logging such as RDS Logging System, etc.
                builder.append('\\').append('r');
                break;
            default:
                builder.append(c);
                break;
        }
    }

    static Mono<String> publish(List<String> sqlParts, Parameter[] values) {
        Iterator<String> iter = sqlParts.iterator();
        ParamWriter writer = new ParamWriter(new StringBuilder().append(iter.next()), iter);

        return OperatorUtils.discardOnCancel(Flux.fromArray(values))
            .doOnDiscard(Parameter.class, DISPOSE)
            .concatMap(it -> it.text(writer).doOnSuccess(writer::flushParameter))
            .then(Mono.fromCallable(writer::toSql));
    }

    private enum Mode {

        AVAILABLE,
        FULL,

        NULL {

            @Override
            boolean canFollow(Mode mode) {
                return false;
            }
        },

        NUMERIC {

            @Override
            boolean canFollow(Mode mode) {
                return false;
            }
        },

        BINARY {

            @Override
            void start(StringBuilder builder) {
                builder.append('b').append('\'');
            }

            @Override
            void end(StringBuilder builder) {
                builder.append('\'');
            }
        },

        HEX {

            @Override
            void start(StringBuilder builder) {
                builder.append('x').append('\'');
            }

            @Override
            void end(StringBuilder builder) {
                builder.append('\'');
            }
        },

        STRING {

            @Override
            boolean canFollow(Mode mode) {
                return this == mode || mode == Mode.NUMERIC;
            }

            @Override
            void start(StringBuilder builder) {
                builder.append('\'');
            }

            @Override
            void end(StringBuilder builder) {
                builder.append('\'');
            }
        };


        void start(StringBuilder builder) {
            // Do nothing
        }

        void end(StringBuilder builder) {
            // Do nothing
        }

        boolean canFollow(Mode mode) {
            return this == mode;
        }
    }
}
