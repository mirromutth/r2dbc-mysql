package dev.miku.r2dbc.mysql.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.nio.charset.Charset;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;

/**
 * Unit tests for {@link DateCodec}.
 */
public class DateCodecTest extends LegacyDateCodecTestSupport<Date> {

    private final Date[] dates = {
            Date.from(Instant.EPOCH),
            Date.from(Instant.ofEpochMilli(-1000)),
            Date.from(Instant.ofEpochMilli(1000)),
            Date.from(Instant.ofEpochMilli(-1577777771671L)),
            Date.from(Instant.ofEpochMilli(1577777771671L)),
    };

    @Override
    public DateCodec getCodec(ByteBufAllocator allocator) {
        return new DateCodec(allocator);
    }

    @Override
    public Date[] originParameters() {
        return dates;
    }

    @Override
    public Object[] stringifyParameters() {
        return Arrays.stream(dates).map(this::convert).map(this::toText).toArray();
    }

    @Override
    public ByteBuf[] binaryParameters(Charset charset) {
        return Arrays.stream(dates).map(this::convert).map(this::toBinary).toArray(ByteBuf[]::new);
    }
}
