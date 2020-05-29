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

import dev.miku.r2dbc.mysql.ParameterOutputStream;
import dev.miku.r2dbc.mysql.collation.CharCollation;
import dev.miku.r2dbc.mysql.constant.BinaryDateTimes;
import dev.miku.r2dbc.mysql.Parameter;
import dev.miku.r2dbc.mysql.util.CodecUtils;
import dev.miku.r2dbc.mysql.util.OperatorUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * TODO: remove it! see Parameter.
 */
final class ParamOutputStream extends ParameterOutputStream {

    private static final String COMMA = ",";

    private static final int SECONDS_OF_MINUTE = 60;

    private static final int SECONDS_OF_HOUR = SECONDS_OF_MINUTE * 60;

    private static final int SECONDS_OF_DAY = SECONDS_OF_HOUR * 24;

    private static final int NANOS_OF_SECOND = 1000_000_000;

    private static final int NANOS_OF_MICRO = 1000;

    private static final int MIN_CAPACITY = 256;

    private final ByteBufAllocator allocator;

    private final List<ByteBuf> buffers;

    private ParamOutputStream(ByteBuf buf) {
        requireNonNull(buf, "buf must not be null");

        this.buffers = new ArrayList<>();
        this.buffers.add(buf);
        this.allocator = buf.alloc();
    }

    @Override
    public void write(int b) {
        writableBuffer(Byte.BYTES).writeByte(b);
    }

    @Override
    public void write(byte[] b) {
        Objects.requireNonNull(b);

        if (b.length == 0) {
            return;
        }

        writableBuffer(b.length).writeBytes(b);
    }

    @Override
    public void write(byte[] b, int off, int len) {
        Objects.requireNonNull(b);

        if (off < 0 || off > b.length || len < 0 || off + len > b.length || off + len < 0) {
            throw new IndexOutOfBoundsException("off: " + off + ", len: " + len + ", bytes length: " + b.length);
        } else if (len == 0) {
            return;
        }

        writableBuffer(len - off).writeBytes(b, off, len);
    }

    @Override
    public void writeBoolean(boolean value) {
        writableBuffer(Byte.BYTES).writeBoolean(value);
    }

    @Override
    public void writeByte(byte value) {
        writableBuffer(Byte.BYTES).writeByte(value);
    }

    @Override
    public void writeShort(short value) {
        writableBuffer(Short.BYTES).writeShortLE(value);
    }

    @Override
    public void writeInt(int value) {
        writableBuffer(Integer.BYTES).writeIntLE(value);
    }

    @Override
    public void writeLong(long value) {
        writableBuffer(Long.BYTES).writeLongLE(value);
    }

    @Override
    public void writeFloat(float value) {
        writableBuffer(Float.BYTES).writeFloatLE(value);
    }

    @Override
    public void writeDouble(double value) {
        writableBuffer(Double.BYTES).writeDoubleLE(value);
    }

    @Override
    public void writeDate(LocalDate date) {
        writableBuffer(Byte.BYTES + BinaryDateTimes.DATE_SIZE)
            .writeByte(BinaryDateTimes.DATE_SIZE)
            .writeShortLE(date.getYear())
            .writeByte(date.getMonthValue())
            .writeByte(date.getDayOfMonth());
    }

    @Override
    public void writeDateTime(LocalDateTime dateTime) {
        LocalTime time = dateTime.toLocalTime();

        if (LocalTime.MIDNIGHT.equals(time)) {
            writeDate(dateTime.toLocalDate());
        } else {
            int nano = time.getNano();
            int bytes = nano > 0 ? BinaryDateTimes.MICRO_DATETIME_SIZE : BinaryDateTimes.DATETIME_SIZE;

            ByteBuf buf = writableBuffer(Byte.BYTES + bytes).writeByte(bytes)
                .writeShortLE(dateTime.getYear())
                .writeByte(dateTime.getMonthValue())
                .writeByte(dateTime.getDayOfMonth())
                .writeByte(time.getHour())
                .writeByte(time.getMinute())
                .writeByte(time.getSecond());

            if (nano > 0) {
                buf.writeIntLE(nano / NANOS_OF_MICRO);
            }
        }
    }

    @Override
    public void writeDuration(Duration duration) {
        long seconds = duration.getSeconds();
        int nanos = duration.getNano();

        if (seconds == 0 && nanos == 0) {
            // It is zero of var int, not terminal.
            writableBuffer(Byte.BYTES).writeByte(0);
            return;
        }

        boolean isNegative = duration.isNegative();
        if (isNegative) {
            if (nanos > 0) {
                // Note: nanos should always be a positive integer or 0, see Duration.getNano().
                // So if duration is negative, seconds should be humanity seconds - 1, so +1 then negate.
                seconds = -(seconds + 1);
                nanos = NANOS_OF_SECOND - nanos;
            } else {
                seconds = -seconds;
            }
        }

        int size = nanos > 0 ? BinaryDateTimes.MICRO_TIME_SIZE : BinaryDateTimes.TIME_SIZE;

        ByteBuf buf = writableBuffer(Byte.BYTES + size).writeByte(size)
            .writeBoolean(isNegative)
            .writeIntLE((int) (seconds / SECONDS_OF_DAY))
            .writeByte((int) ((seconds % SECONDS_OF_DAY) / SECONDS_OF_HOUR))
            .writeByte((int) ((seconds % SECONDS_OF_HOUR) / SECONDS_OF_MINUTE))
            .writeByte((int) (seconds % SECONDS_OF_MINUTE));

        if (nanos > 0) {
            buf.writeIntLE(nanos / NANOS_OF_MICRO);
        }
    }

    @Override
    public void writeTime(LocalTime time) {
        if (LocalTime.MIDNIGHT.equals(time)) {
            writableBuffer(Byte.BYTES).writeByte(0);
            return;
        }

        int nanos = time.getNano();
        int size = nanos == 0 ? BinaryDateTimes.TIME_SIZE : BinaryDateTimes.MICRO_TIME_SIZE;

        ByteBuf buf = writableBuffer(Byte.BYTES + size)
            .writeByte(size)
            .writeBoolean(false)
            .writeIntLE(0)
            .writeByte(time.getHour())
            .writeByte(time.getMinute())
            .writeByte(time.getSecond());

        if (nanos != 0) {
            buf.writeIntLE(nanos / NANOS_OF_MICRO);
        }
    }

    @Override
    public void writeAsciiString(CharSequence sequence) {
        int bytes = sequence.length();
        ByteBuf buf = writableBuffer(CodecUtils.varIntBytes(bytes) + bytes);

        CodecUtils.writeVarInt(buf, bytes);
        buf.writeCharSequence(sequence, StandardCharsets.US_ASCII);
    }

    @Override
    public void writeCharSequence(CharSequence sequence, CharCollation collation) {
        int minBytes = sequence.length();

        if (minBytes <= 0) {
            // Zero of var int, not terminal.
            writableBuffer(Byte.BYTES).writeByte(0);
            return;
        }

        long maxBytes = ((long) sequence.length()) * collation.getByteSize();
        int minVarIntBytes = CodecUtils.varIntBytes(minBytes);
        int maxVarIntBytes = CodecUtils.varIntBytes(maxBytes);

        if (minVarIntBytes == maxVarIntBytes) {
            // Can use place holder for least copy.
            ByteBuf varIntBuf = writableBuffer(maxVarIntBytes);
            int varIntWriter = varIntBuf.writerIndex();

            // Use all zero for placeholder.
            varIntBuf.writeZero(maxVarIntBytes);

            if (maxBytes > Integer.MAX_VALUE) {
                List<CharSequence> sliced = slicedSequence(sequence, Integer.MAX_VALUE / collation.getByteSize());
                long writtenBytes = writeOnlySliced(sliced, collation);

                CodecUtils.setVarInt(varIntBuf, varIntWriter, writtenBytes);
            } else {
                Charset charset = collation.getCharset();
                int writtenBytes = writableBuffer((int) maxBytes).writeCharSequence(sequence, charset);

                CodecUtils.setVarInt(varIntBuf, varIntWriter, writtenBytes);
            }
        } else {
            // Just copied.
            if (maxBytes > Integer.MAX_VALUE) {
                List<CharSequence> sliced = slicedSequence(sequence, Integer.MAX_VALUE / collation.getByteSize());
                writeCopySlicedWithSize(sliced, collation);
            } else {
                Charset charset = collation.getCharset();
                ByteBuf strBuf = allocator.buffer(minBytes);

                try {
                    int writtenBytes = strBuf.writeCharSequence(sequence, charset);
                    ByteBuf varIntBuf = writableBuffer(CodecUtils.varIntBytes(writtenBytes));

                    CodecUtils.writeVarInt(varIntBuf, writtenBytes);
                    writableBuffer(writtenBytes).writeBytes(strBuf);
                } finally {
                    strBuf.release();
                }
            }
        }
    }

    @Override
    public void writeCharSequences(List<CharSequence> sequences, CharCollation collation) {
        long minBytes = 0;

        for (CharSequence sequence : sequences) {
            minBytes += sequence.length();
        }

        if (minBytes <= 0) {
            // Zero of var int, not terminal.
            writableBuffer(Byte.BYTES).writeByte(0);
            return;
        }

        long maxBytes = Math.multiplyExact(minBytes, collation.getByteSize());
        int minVarIntBytes = CodecUtils.varIntBytes(minBytes);
        int maxVarIntBytes = CodecUtils.varIntBytes(maxBytes);

        if (minVarIntBytes == maxVarIntBytes) {
            // Can use place holder for least copy.
            ByteBuf varIntBuf = writableBuffer(maxVarIntBytes);
            int varIntWriter = varIntBuf.writerIndex();

            // Use all zero for placeholder.
            varIntBuf.writeZero(maxVarIntBytes);

            if (maxBytes > Integer.MAX_VALUE) {
                int eachSize = Integer.MAX_VALUE / collation.getByteSize();
                List<CharSequence> sliced = new ArrayList<>(sequences.size());

                for (CharSequence sequence : sequences) {
                    slicedSequenceTo(sliced, sequence, eachSize);
                }

                long writtenBytes = writeOnlySliced(sliced, collation);
                CodecUtils.setVarInt(varIntBuf, varIntWriter, writtenBytes);
            } else {
                Charset charset = collation.getCharset();
                ByteBuf buffer = writableBuffer((int) maxBytes);
                int writtenBytes = 0; // max bytes <= Integer.MAX_VALUE, must not be overflow.

                for (CharSequence sequence : sequences) {
                    writtenBytes += buffer.writeCharSequence(sequence, charset);
                }

                CodecUtils.setVarInt(varIntBuf, varIntWriter, writtenBytes);
            }
        } else {
            // Just copied.
            if (maxBytes > Integer.MAX_VALUE) {
                int eachSize = Integer.MAX_VALUE / collation.getByteSize();
                List<CharSequence> sliced = new ArrayList<>(sequences.size());

                for (CharSequence sequence : sequences) {
                    slicedSequenceTo(sliced, sequence, eachSize);
                }

                writeCopySlicedWithSize(sliced, collation);
            } else {
                // max bytes <= Integer.MAX_VALUE, must not be overflow.
                int writtenBytes = 0;
                Charset charset = collation.getCharset();
                ByteBuf strBuf = allocator.buffer((int) minBytes);

                try {
                    for (CharSequence sequence : sequences) {
                        writtenBytes += strBuf.writeCharSequence(sequence, charset);
                    }

                    ByteBuf varIntBuf = writableBuffer(CodecUtils.varIntBytes(writtenBytes));

                    CodecUtils.writeVarInt(varIntBuf, writtenBytes);
                    writableBuffer(writtenBytes).writeBytes(strBuf);
                } finally {
                    strBuf.release();
                }
            }
        }
    }

    @Override
    public void writeSet(List<CharSequence> elements, CharCollation collation) {
        if (elements.isEmpty()) {
            // Zero of var int, not terminal.
            writableBuffer(Byte.BYTES).writeByte(0);
            return;
        }

        int size = elements.size();
        int byteSize = collation.getByteSize();
        long minBytes = elements.get(0).length();

        for (int i = 1; i < size; ++i) {
            minBytes += 1 + (long) elements.get(i).length();
        }

        long maxBytes = minBytes * byteSize;
        int minVarIntBytes = CodecUtils.varIntBytes(minBytes);
        int maxVarIntBytes = CodecUtils.varIntBytes(maxBytes);

        if (minVarIntBytes == maxVarIntBytes) {
            // Can use place holder for least copy.
            ByteBuf varIntBuf = writableBuffer(maxVarIntBytes);
            int varIntWriter = varIntBuf.writerIndex();

            // Use all zero for placeholder.
            varIntBuf.writeZero(maxVarIntBytes);

            if (maxBytes > Integer.MAX_VALUE) {
                List<CharSequence> sliced = slicedSet(elements, collation);
                long writtenBytes = writeOnlySliced(sliced, collation);
                CodecUtils.setVarInt(varIntBuf, varIntWriter, writtenBytes);
            } else {
                Charset charset = collation.getCharset();
                ByteBuf buffer = writableBuffer((int) maxBytes);
                // max bytes <= Integer.MAX_VALUE, must not be overflow.
                int writtenBytes = buffer.writeCharSequence(elements.get(0), charset);

                for (int i = 1; i < size; ++i) {
                    writtenBytes += buffer.writeCharSequence(COMMA, charset);
                    writtenBytes += buffer.writeCharSequence(elements.get(i), charset);
                }

                CodecUtils.setVarInt(varIntBuf, varIntWriter, writtenBytes);
            }
        } else {
            // Just copied.
            if (maxBytes > Integer.MAX_VALUE) {
                writeCopySlicedWithSize(slicedSet(elements, collation), collation);
            } else {
                Charset charset = collation.getCharset();
                ByteBuf strBuf = allocator.buffer((int) minBytes);

                try {
                    // max bytes <= Integer.MAX_VALUE, must not be overflow.
                    int writtenBytes = strBuf.writeCharSequence(elements.get(0), charset);

                    for (int i = 1; i < size; ++i) {
                        writtenBytes += strBuf.writeCharSequence(COMMA, charset);
                        writtenBytes += strBuf.writeCharSequence(elements.get(i), charset);
                    }

                    ByteBuf varIntBuf = writableBuffer(CodecUtils.varIntBytes(writtenBytes));
                    CodecUtils.writeVarInt(varIntBuf, writtenBytes);
                    writableBuffer(writtenBytes).writeBytes(strBuf);
                } finally {
                    strBuf.release();
                }
            }
        }
    }

    @Override
    public void writeByteArray(byte[] bytes) {
        int bufferBytes = bytes.length;

        if (bufferBytes <= 0) {
            // Zero of var int, not terminal.
            writableBuffer(Byte.BYTES).writeByte(0);
        } else {
            int varIntBytes = CodecUtils.varIntBytes(bufferBytes);

            // varIntBytes + bufferBytes > Integer.MAX_VALUE
            if (bufferBytes > Integer.MAX_VALUE - varIntBytes) {
                CodecUtils.writeVarInt(writableBuffer(varIntBytes), bufferBytes);

                writableBuffer(bufferBytes).writeBytes(bytes);
            } else {
                // varIntBytes + bufferBytes must <= Integer.MAX_VALUE
                ByteBuf buf = writableBuffer(varIntBytes + bufferBytes);

                CodecUtils.writeVarInt(buf, bufferBytes);
                buf.writeBytes(bytes);
            }
        }
    }

    @Override
    public void writeByteBuffer(ByteBuffer buffer) {
        int bufferBytes = buffer.remaining();

        if (bufferBytes <= 0) {
            // Zero of var int, not terminal.
            writableBuffer(Byte.BYTES).writeByte(0);
            return;
        }

        int varIntBytes = CodecUtils.varIntBytes(bufferBytes);

        // (without overflow) bufferBytes + varIntBytes > Integer.MAX_VALUE
        if (bufferBytes > Integer.MAX_VALUE - varIntBytes) {
            CodecUtils.writeVarInt(writableBuffer(varIntBytes), bufferBytes);
            writableBuffer(buffer.remaining()).writeBytes(buffer);
        } else {
            ByteBuf buf = writableBuffer(bufferBytes + varIntBytes);
            CodecUtils.writeVarInt(buf, bufferBytes);
            buf.writeBytes(buffer);
        }
    }

    @Override
    public void writeByteBuffers(List<ByteBuffer> buffers) {
        long bufferBytes = 0;

        for (ByteBuffer buffer : buffers) {
            if (buffer.hasRemaining()) {
                bufferBytes += buffer.remaining();
            }
        }

        if (bufferBytes <= 0) {
            // Zero of var int, not terminal.
            writableBuffer(Byte.BYTES).writeByte(0);
            return;
        }

        int varIntBytes = CodecUtils.varIntBytes(bufferBytes);
        long totalBytes = varIntBytes + bufferBytes;

        if (totalBytes > Integer.MAX_VALUE) {
            CodecUtils.writeVarInt(writableBuffer(varIntBytes), bufferBytes);

            for (ByteBuffer buffer : buffers) {
                if (buffer.hasRemaining()) {
                    writableBuffer(buffer.remaining()).writeBytes(buffer);
                }
            }
        } else {
            ByteBuf buf = writableBuffer((int) totalBytes);

            CodecUtils.writeVarInt(buf, bufferBytes);
            for (ByteBuffer buffer : buffers) {
                if (buffer.hasRemaining()) {
                    buf.writeBytes(buffer);
                }
            }
        }
    }

    private void dispose() {
        for (ByteBuf buffer : this.buffers) {
            ReferenceCountUtil.safeRelease(buffer);
        }
    }

    private Publisher<ByteBuf> allBuffers() {
        return Flux.defer(() -> {
            if (this.buffers.size() == 1) {
                return Flux.just(this.buffers.get(0));
            }
            return Flux.fromIterable(this.buffers);
        });
    }

    private ByteBuf writableBuffer(int bytes) {
        ByteBuf buf = this.buffers.get(this.buffers.size() - 1);

        if (buf.maxWritableBytes() < bytes) {
            buf = this.allocator.buffer(Math.max(bytes, MIN_CAPACITY));
            this.buffers.add(buf);
        }

        return buf;
    }

    /**
     * Write "sliced" {@link CharSequence}s to writer, without vat integer size, without copy.
     */
    private long writeOnlySliced(List<CharSequence> sliced, CharCollation collation) {
        Charset charset = collation.getCharset();
        int byteSize = collation.getByteSize();
        long writtenBytes = 0;

        for (CharSequence slice : sliced) {
            writtenBytes += writableBuffer(slice.length() * byteSize).writeCharSequence(slice, charset);
        }

        return writtenBytes;
    }

    /**
     * Write "sliced" {@link CharSequence}s to writer with vat integer size, but use copy write.
     */
    private void writeCopySlicedWithSize(List<CharSequence> sliced, CharCollation collation) {
        EncodedBuffers encoded = encodeSliced(sliced, collation);

        try {
            long writtenBytes = encoded.totalBytes;
            ByteBuf varIntBuf = writableBuffer(CodecUtils.varIntBytes(writtenBytes));

            CodecUtils.writeVarInt(varIntBuf, writtenBytes);
            for (ByteBuf strBuf : encoded.buffers) {
                writableBuffer(strBuf.readableBytes()).writeBytes(strBuf);
            }
        } finally {
            for (ByteBuf strBuf : encoded.buffers) {
                if (strBuf != null) {
                    ReferenceCountUtil.safeRelease(strBuf);
                }
            }
        }
    }

    private EncodedBuffers encodeSliced(List<CharSequence> sliced, CharCollation collation) {
        Charset charset = collation.getCharset();
        int size = sliced.size();
        ByteBuf[] buffers = new ByteBuf[size];
        long written = 0;

        try {
            for (int i = 0; i < size; ++i) {
                CharSequence slice = sliced.get(i);
                buffers[i] = allocator.buffer(slice.length());
                written += buffers[i].writeCharSequence(slice, charset);
            }

            return new EncodedBuffers(written, buffers);
        } catch (Throwable e) {
            for (int i = 0; i < size; ++i) {
                if (buffers[i] != null) {
                    ReferenceCountUtil.safeRelease(buffers[i]);
                }
            }
            throw e;
        }
    }

    static Publisher<ByteBuf> publish(ByteBuf prefix, Parameter[] values) {
        ParamOutputStream writer = new ParamOutputStream(prefix);
        return OperatorUtils.discardOnCancel(Flux.fromArray(values))
            .doOnDiscard(Parameter.class, Parameter.DISPOSE)
            .concatMap(param -> param.binary(writer))
            .doOnError(ignored -> writer.dispose())
            .thenMany(writer.allBuffers());
    }

    private static List<CharSequence> slicedSequence(CharSequence sequence, int eachSize) {
        int length = sequence.length();

        if (length <= 0) {
            return Collections.emptyList();
        } else if (length <= eachSize) {
            return Collections.singletonList(sequence);
        }

        // ceil(length / eachSize) = floor((length + eachSize - 1) / eachSize), but we
        // cannot use (length + eachSize - 1) / eachSize, because it may overflow.
        int r = length / eachSize;
        List<CharSequence> result = new ArrayList<>(r * eachSize == length ? r : r + 1);
        slicedSequenceTo0(result, sequence, eachSize, length);

        return result;
    }

    private static List<CharSequence> slicedSet(List<CharSequence> elements, CharCollation collation) {
        int size = elements.size();

        if (size <= 0) {
            return Collections.emptyList();
        } else if (size == 1) {
            return Collections.singletonList(elements.get(0));
        }

        int eachLength = Integer.MAX_VALUE / collation.getByteSize();
        List<CharSequence> sliced = new ArrayList<>((size << 1) - 1);

        slicedSequenceTo(sliced, elements.get(0), eachLength);

        for (int i = 1; i < size; ++i) {
            sliced.add(COMMA);
            slicedSequenceTo(sliced, elements.get(i), eachLength);
        }

        return sliced;
    }

    private static void slicedSequenceTo(List<CharSequence> result, CharSequence sequence, int eachSize) {
        int length = sequence.length();

        if (length <= 0) {
            return;
        } else if (length <= eachSize) {
            result.add(sequence);
            return;
        }

        slicedSequenceTo0(result, sequence, eachSize, length);
    }

    private static void slicedSequenceTo0(List<CharSequence> result, CharSequence sequence, int eachSize, int length) {
        int read = 0;

        while (read < length) {
            int endIndex = read + eachSize;

            if (endIndex > length || endIndex <= read) {
                // range overflow or int32 overflow.
                endIndex = length;
            }

            result.add(sequence.subSequence(read, endIndex));
            read = endIndex;
        }
    }

    private static final class EncodedBuffers {

        private final long totalBytes;

        private final ByteBuf[] buffers;

        private EncodedBuffers(long totalBytes, ByteBuf[] buffers) {
            this.totalBytes = totalBytes;
            this.buffers = buffers;
        }
    }
}
