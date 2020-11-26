package dev.miku.r2dbc.mysql.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;

/**
 * Unit tests for {@link TimestampCodec}.
 */
public class TimestampCodecTest extends LegacyDateCodecTestSupport<Timestamp> {

    private final Timestamp[] timestamps = {
            Timestamp.from(Instant.EPOCH),
            Timestamp.from(Instant.ofEpochMilli(-1000)),
            Timestamp.from(Instant.ofEpochMilli(1000)),
            Timestamp.from(Instant.ofEpochMilli(-1577777771671L)),
            Timestamp.from(Instant.ofEpochMilli(1577777771671L)),
            Timestamp.from(Instant.ofEpochSecond(-30557014167219200L)),
            Timestamp.from(Instant.ofEpochSecond(30557014167219200L)),
    };

    @Override
    public TimestampCodec getCodec(ByteBufAllocator allocator) {
        return new TimestampCodec(allocator);
    }

    @Override
    public Timestamp[] originParameters() {
        return timestamps;
    }

    @Override
    public Object[] stringifyParameters() {
        return Arrays.stream(timestamps).map(this::convert).map(this::toText).toArray();
    }

    @Override
    public ByteBuf[] binaryParameters(Charset charset) {
        return Arrays.stream(timestamps).map(this::convert).map(this::toBinary).toArray(ByteBuf[]::new);
    }
}
