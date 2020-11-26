package dev.miku.r2dbc.mysql.codec;

import dev.miku.r2dbc.mysql.Parameter;
import dev.miku.r2dbc.mysql.ParameterWriter;
import dev.miku.r2dbc.mysql.constant.DataTypes;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Date;

/**
 * Codec for {@link Date}.
 */
public class DateCodec implements Codec<Date> {

    private final ByteBufAllocator allocator;

    public DateCodec(ByteBufAllocator allocator) {
        this.allocator = allocator;
    }

    @Override
    public Date decode(ByteBuf value, FieldInformation info, Class<?> target, boolean binary, CodecContext context) {
        LocalDateTime origin = LocalDateTimeCodec.decodeOrigin(value, binary, context);

        if (origin == null) {
            return null;
        }

        Instant instant = origin.toInstant(context.getServerZoneId().getRules().getOffset(origin));
        return Date.from(instant);
    }

    @Override
    public Parameter encode(Object value, CodecContext context) {
        return new DateParameter(allocator, (Date) value, context);
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof Date;
    }

    @Override
    public boolean canDecode(FieldInformation info, Class<?> target) {
        return DateTimes.canDecodeLegacyDate(info.getType(), target, Date.class);
    }

    private static final class DateParameter extends AbstractParameter {

        private final ByteBufAllocator allocator;

        private final Date value;

        private final CodecContext context;

        private DateParameter(ByteBufAllocator allocator, Date value, CodecContext context) {
            this.allocator = allocator;
            this.value = value;
            this.context = context;
        }

        @Override
        public Publisher<ByteBuf> publishBinary() {
            return Mono.fromSupplier(() -> LocalDateTimeCodec.encodeBinary(allocator, serverValue()));
        }

        @Override
        public Mono<Void> publishText(ParameterWriter writer) {
            return Mono.fromRunnable(() -> LocalDateTimeCodec.encodeText(writer, serverValue()));
        }

        @Override
        public short getType() {
            return DataTypes.TIMESTAMP;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DateParameter that = (DateParameter) o;
            return value.equals(that.value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }

        private LocalDateTime serverValue() {
            return LocalDateTime.ofInstant(value.toInstant(), context.getServerZoneId());
        }
    }
}
