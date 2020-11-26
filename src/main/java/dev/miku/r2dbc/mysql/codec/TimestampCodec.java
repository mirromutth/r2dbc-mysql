package dev.miku.r2dbc.mysql.codec;

import dev.miku.r2dbc.mysql.Parameter;
import dev.miku.r2dbc.mysql.ParameterWriter;
import dev.miku.r2dbc.mysql.constant.DataTypes;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;

/**
 * Codec for {@link java.sql.Timestamp}.
 */
final class TimestampCodec implements Codec<Timestamp> {

    private final ByteBufAllocator allocator;

    TimestampCodec(ByteBufAllocator allocator) {
        this.allocator = allocator;
    }

    @Override
    public Timestamp decode(ByteBuf value, FieldInformation info, Class<?> target, boolean binary, CodecContext context) {
        LocalDateTime origin = LocalDateTimeCodec.decodeOrigin(value, binary, context);

        if (origin == null) {
            return null;
        }

        Instant instant = origin.toInstant(context.getServerZoneId().getRules().getOffset(origin));
        return Timestamp.from(instant);
    }

    @Override
    public Parameter encode(Object value, CodecContext context) {
        return new TimestampParameter(allocator, (Timestamp) value, context);
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof Timestamp;
    }

    @Override
    public boolean canDecode(FieldInformation info, Class<?> target) {
        return DateTimes.canDecodeLegacyDate(info.getType(), target, Timestamp.class);
    }

    private static final class TimestampParameter extends AbstractParameter {

        private final ByteBufAllocator allocator;

        private final Timestamp value;

        private final CodecContext context;

        private TimestampParameter(ByteBufAllocator allocator, Timestamp value, CodecContext context) {
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
            TimestampParameter that = (TimestampParameter) o;
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
