/*
 * Copyright 2018-2021 the original author or authors.
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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;

import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests for {@link DurationCodec}.
 */
class DurationCodecTest implements CodecTestSupport<Duration> {

    private static final long SECONDS_OF_DAY = TimeUnit.DAYS.toSeconds(1);

    private static final long SECONDS_OF_HOUR = TimeUnit.HOURS.toSeconds(1);

    private static final long SECONDS_OF_MINUTE = TimeUnit.MINUTES.toSeconds(1);

    private static final long MAX_SECONDS = TimeUnit.HOURS.toSeconds(838) +
        TimeUnit.MINUTES.toSeconds(59) + 59;

    private static final long MAX_NANOS = TimeUnit.MICROSECONDS.toNanos(999999);

    private final Duration[] durations = {
        Duration.ZERO,
        Duration.ofSeconds(1),
        Duration.ofSeconds(-1),
        Duration.ofSeconds(10, 1000L),
        Duration.ofSeconds(-10, -1000L),
        Duration.ofSeconds(30, 200000L),
        Duration.ofSeconds(-30, -200000L),
        Duration.ofSeconds(1234567, 89105612L),
        Duration.ofSeconds(-1234567, -89105612L),
        Duration.ofSeconds(1234567, 800000000L),
        Duration.ofSeconds(-1234567, -800000000L),
        Duration.ofSeconds(MAX_SECONDS, MAX_NANOS),
        Duration.ofSeconds(-MAX_SECONDS, -MAX_NANOS),
        // Following should not be permitted by MySQL server, but also test.
        Duration.ofSeconds(Integer.MAX_VALUE, -1),
        Duration.ofSeconds(Integer.MAX_VALUE, 999_999_999),
        Duration.ofSeconds(Integer.MAX_VALUE, Long.MAX_VALUE),
        Duration.ofSeconds(Integer.MIN_VALUE, -1),
        Duration.ofSeconds(Integer.MIN_VALUE, 999_999_999),
        Duration.ofSeconds(Integer.MIN_VALUE, Long.MIN_VALUE),
    };

    @Override
    public DurationCodec getCodec(ByteBufAllocator allocator) {
        return new DurationCodec(allocator);
    }

    @Override
    public Duration[] originParameters() {
        return durations;
    }

    @Override
    public Object[] stringifyParameters() {
        return Arrays.stream(durations)
            .map(duration -> duration.isNegative() ?
                String.format("'-%s'", format(duration.abs())) :
                String.format("'%s'", format(duration)))
            .toArray();
    }

    @Override
    public ByteBuf[] binaryParameters(Charset charset) {
        return Arrays.stream(durations)
            .map(it -> {
                if (it.isZero()) {
                    return Unpooled.buffer(0, 0);
                }

                ByteBuf buf = Unpooled.buffer();
                if (it.isNegative()) {
                    buf.writeBoolean(true);
                    it = it.abs();
                } else {
                    buf.writeBoolean(false);
                }
                long seconds = it.getSeconds();
                int nanos = it.getNano();

                buf.writeIntLE((int) (seconds / SECONDS_OF_DAY))
                    .writeByte((int) ((seconds % SECONDS_OF_DAY) / SECONDS_OF_HOUR))
                    .writeByte((int) ((seconds % SECONDS_OF_HOUR) / SECONDS_OF_MINUTE))
                    .writeByte((int) (seconds % SECONDS_OF_MINUTE));

                if (nanos != 0) {
                    buf.writeIntLE((int) TimeUnit.NANOSECONDS.toMicros(nanos));
                }

                return buf;
            })
            .toArray(ByteBuf[]::new);
    }

    private static String format(Duration d) {
        long hours = d.toHours();
        long minutes = d.minusHours(hours).toMinutes();
        long seconds = d.minusHours(hours).minusMinutes(minutes).getSeconds();
        long nanos = d.minusHours(hours).minusMinutes(minutes).minusSeconds(seconds).toNanos();
        long micros = TimeUnit.NANOSECONDS.toMicros(nanos);

        if (micros != 0) {
            String origin = String.format("%02d:%02d:%02d.%06d", hours, minutes, seconds, micros);
            int length = origin.length();

            while (origin.charAt(length - 1) == '0') {
                --length;
            }

            return origin.substring(0, length);
        } else {
            return String.format("%02d:%02d:%02d", hours, minutes, seconds);
        }
    }
}
