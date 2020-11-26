package dev.miku.r2dbc.mysql.codec;

import dev.miku.r2dbc.mysql.ConnectionContextTest;
import io.netty.buffer.ByteBuf;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.temporal.Temporal;
import java.util.Date;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MICRO_OF_SECOND;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.time.temporal.ChronoField.YEAR;

/**
 * Base class considers codecs unit tests of legacy date.
 */
abstract class LegacyDateCodecTestSupport<T extends Date> implements CodecTestSupport<T> {

    protected static final ZoneId ENCODE_SERVER_ZONE = ZoneId.of("+6");

    private static final DateTimeFormatter FORMATTER = new DateTimeFormatterBuilder()
            .appendLiteral('\'')
            .appendValue(YEAR, 4, 19, SignStyle.NORMAL)
            .appendLiteral('-')
            .appendValue(MONTH_OF_YEAR, 2)
            .appendLiteral('-')
            .appendValue(DAY_OF_MONTH, 2)
            .appendLiteral(' ')
            .appendValue(HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 2)
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 2)
            .optionalStart()
            .appendFraction(MICRO_OF_SECOND, 0, 6, true)
            .optionalEnd()
            .appendLiteral('\'')
            .toFormatter(Locale.ENGLISH);

    static {
        System.setProperty("user.timezone", "GMT+2");
    }

    @Override
    public CodecContext context() {
        return ConnectionContextTest.mock(ENCODE_SERVER_ZONE);
    }

    protected final String toText(Temporal dateTime) {
        return FORMATTER.format(dateTime);
    }

    protected final ByteBuf toBinary(LocalDateTime dateTime) {
        ByteBuf buf = LocalDateCodecTest.encode(dateTime.toLocalDate());
        LocalTime time = dateTime.toLocalTime();

        if (LocalTime.MIDNIGHT.equals(time)) {
            return buf;
        }

        buf.writeByte(time.getHour())
                .writeByte(time.getMinute())
                .writeByte(time.getSecond());

        if (time.getNano() != 0) {
            buf.writeIntLE((int) TimeUnit.NANOSECONDS.toMicros(time.getNano()));
        }

        return buf;
    }

    protected final LocalDateTime convert(T value) {
        return LocalDateTime.ofInstant(value.toInstant(), ENCODE_SERVER_ZONE);
    }

}
