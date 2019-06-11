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

import io.github.mirromutth.r2dbc.mysql.collation.CharCollation;
import io.github.mirromutth.r2dbc.mysql.constant.BinaryDateTimes;
import io.github.mirromutth.r2dbc.mysql.util.CodecUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
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
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 * Parameter writer for {@link ByteBuf}(s).
 */
public final class ParameterWriter implements Disposable {

    private static final String COMMA = ",";

    private static final int SECONDS_OF_MINUTE = 60;

    private static final int SECONDS_OF_HOUR = SECONDS_OF_MINUTE * 60;

    private static final int SECONDS_OF_DAY = SECONDS_OF_HOUR * 24;

    private static final int MIN_CAPACITY = 256;

    private final ByteBufAllocator allocator;

    private ByteBuf buf;

    private List<ByteBuf> buffers;

    private int lastIndex = -1;

    ParameterWriter(ByteBuf buf) {
        this.buf = requireNonNull(buf, "buf must not be null");
        this.allocator = buf.alloc();
    }

    public void writeBoolean(boolean value) {
        writableBuffer(Byte.BYTES).writeBoolean(value);
    }

    public void writeByte(byte value) {
        writableBuffer(Byte.BYTES).writeByte(value);
    }

    public void writeShort(short value) {
        writableBuffer(Short.BYTES).writeShortLE(value);
    }

    public void writeInt(int value) {
        writableBuffer(Integer.BYTES).writeIntLE(value);
    }

    public void writeLong(long value) {
        writableBuffer(Long.BYTES).writeLongLE(value);
    }

    public void writeFloat(float value) {
        writableBuffer(Float.BYTES).writeFloatLE(value);
    }

    public void writeDouble(double value) {
        writableBuffer(Double.BYTES).writeDoubleLE(value);
    }

    public void writeStringifyNumber(Number value) {
        String valueStr = value.toString();
        int size = valueStr.length();
        ByteBuf buf = writableBuffer(CodecUtils.varIntBytes(size) + size);

        CodecUtils.writeVarInt(buf, size);
        buf.writeCharSequence(valueStr, StandardCharsets.US_ASCII);
    }

    public void writeDate(LocalDate date) {
        writableBuffer(Byte.BYTES + BinaryDateTimes.DATE_SIZE)
            .writeByte(BinaryDateTimes.DATE_SIZE)
            .writeShortLE(date.getYear())
            .writeByte(date.getMonthValue())
            .writeByte(date.getDayOfMonth());
    }

    public void writeDateTime(LocalDateTime dateTime) {
        LocalTime time = dateTime.toLocalTime();

        if (LocalTime.MIDNIGHT.equals(time)) {
            writeDate(dateTime.toLocalDate());
        } else {
            int nano = time.getNano();
            int bytes;

            if (nano > 0) {
                bytes = BinaryDateTimes.MICRO_DATETIME_SIZE;
            } else {
                bytes = BinaryDateTimes.DATETIME_SIZE;
            }

            ByteBuf buf = writableBuffer(Byte.BYTES + bytes);

            buf.writeByte(bytes) // var int
                .writeShortLE(dateTime.getYear())
                .writeByte(dateTime.getMonthValue())
                .writeByte(dateTime.getDayOfMonth())
                .writeByte(time.getHour())
                .writeByte(time.getMinute())
                .writeByte(time.getSecond());

            if (nano > 0) {
                buf.writeIntLE((int) TimeUnit.NANOSECONDS.toMicros(nano));
            }
        }
    }

    public void writeDuration(Duration duration) {
        long seconds = duration.getSeconds();
        long nanos = duration.getNano();

        if (nanos <= 0) {
            // Nano muse not be negative.
            writeSeconds(seconds);
        } else {
            writeSecondNanos(seconds, nanos);
        }
    }

    public void writeTime(LocalTime time) {
        long hour = time.getHour();
        long minute = time.getMinute();
        long second = time.getSecond();
        long nanos = time.getNano();
        long totalSeconds = TimeUnit.HOURS.toSeconds(hour) + TimeUnit.MINUTES.toSeconds(minute) + second;

        if (nanos <= 0) {
            writeSeconds(totalSeconds);
        } else {
            writeSecondNanos(totalSeconds, nanos);
        }
    }

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

    Publisher<ByteBuf> publish() {
        return Flux.defer(() -> {
            ByteBuf buf = this.buf;

            if (buf == null) {
                return Flux.fromIterable(this.buffers);
            } else {
                return Flux.just(buf);
            }
        });
    }

    @Override
    public void dispose() {
        ReferenceCountUtil.safeRelease(this.buf);

        if (this.buffers != null) {
            for (ByteBuf buffer : this.buffers) {
                ReferenceCountUtil.safeRelease(buffer);
            }
        }
    }

    private ByteBuf writableBuffer(int bytes) {
        if (this.buf == null) {
            ByteBuf buf = this.buffers.get(this.lastIndex);

            if (buf.maxWritableBytes() < bytes) {
                buf = this.allocator.buffer(Math.max(bytes, MIN_CAPACITY));
                this.buffers.add(buf);
                ++this.lastIndex;
            }

            return buf;
        } else {
            ByteBuf buf = this.buf;

            if (buf.maxWritableBytes() < bytes) {
                this.buffers = new ArrayList<>();
                this.buffers.add(buf);
                this.buf = null;

                buf = allocator.buffer(Math.max(bytes, MIN_CAPACITY));
                this.buffers.add(buf);
                this.lastIndex = 1;
            }

            return buf;
        }
    }

    private void writeSecondNanos(long seconds, long nanos) {
        boolean isNegative;

        if (seconds < 0) {
            isNegative = true;
            seconds = -(seconds + 1);
            nanos = TimeUnit.SECONDS.toNanos(1) - nanos;
        } else {
            isNegative = false;
        }

        ByteBuf buf = writableBuffer(Byte.BYTES + BinaryDateTimes.MICRO_TIME_SIZE)
            .writeByte(BinaryDateTimes.MICRO_TIME_SIZE);

        writeSeconds0(buf, isNegative, seconds).writeIntLE((int) TimeUnit.NANOSECONDS.toMicros(nanos));
    }

    private void writeSeconds(long seconds) {
        if (seconds == 0) {
            // Zero of var int, not terminal.
            writableBuffer(Byte.BYTES).writeByte(0);
            return;
        }

        boolean isNegative;

        if (seconds < 0) {
            seconds = -seconds;
            isNegative = true;
        } else {
            isNegative = false;
        }

        ByteBuf buf = writableBuffer(Byte.BYTES + BinaryDateTimes.TIME_SIZE)
            .writeByte(BinaryDateTimes.TIME_SIZE);

        writeSeconds0(buf, isNegative, seconds);
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

    private static ByteBuf writeSeconds0(ByteBuf buf, boolean isNegative, long seconds) {
        return buf.writeBoolean(isNegative)
            .writeIntLE((int) (seconds / SECONDS_OF_DAY))
            .writeByte((int) ((seconds % SECONDS_OF_DAY) / SECONDS_OF_HOUR))
            .writeByte((int) ((seconds % SECONDS_OF_HOUR) / SECONDS_OF_MINUTE))
            .writeByte((int) (seconds % SECONDS_OF_MINUTE));
    }

    private static List<CharSequence> slicedSequence(CharSequence sequence, int eachSize) {
        int length = sequence.length();

        if (length <= 0) {
            return Collections.emptyList();
        } else if (length <= eachSize) {
            return Collections.singletonList(sequence);
        }

        List<CharSequence> result = new ArrayList<>(ceilDiv(length, eachSize));
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

    private static int ceilDiv(int a, int b) {
        // Can not use (a + b - 1) / b, because it may overflow.
        int r = a / b;

        if (r * b == a) {
            return r;
        }

        return r + 1;
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
