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

package dev.miku.r2dbc.mysql;

import dev.miku.r2dbc.mysql.collation.CharCollation;

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;

/**
 * TODO: remove it! see Parameter.
 */
public abstract class ParameterOutputStream extends OutputStream {

    @Override
    public abstract void write(byte[] b);

    @Override
    public abstract void write(byte[] b, int off, int len);

    @Override
    public abstract void write(int b);

    public abstract void writeBoolean(boolean value);

    public abstract void writeByte(byte value);

    public abstract void writeShort(short value);

    public abstract void writeInt(int value);

    public abstract void writeLong(long value);

    public abstract void writeFloat(float value);

    public abstract void writeDouble(double value);

    public abstract void writeDate(LocalDate date);

    public abstract void writeDateTime(LocalDateTime dateTime);

    public abstract void writeDuration(Duration duration);

    public abstract void writeTime(LocalTime time);

    public abstract void writeAsciiString(CharSequence sequence);

    public abstract void writeCharSequence(CharSequence sequence, CharCollation collation);

    public abstract void writeCharSequences(List<CharSequence> sequences, CharCollation collation);

    public abstract void writeSet(List<CharSequence> elements, CharCollation collation);

    public abstract void writeByteArray(byte[] bytes);

    public abstract void writeByteBuffer(ByteBuffer buffer);

    public abstract void writeByteBuffers(List<ByteBuffer> buffers);

    @Override
    public final void flush() {
    }

    @Override
    public final void close() {
    }
}
