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

package dev.miku.r2dbc.mysql.codec;

import dev.miku.r2dbc.mysql.message.NormalFieldValue;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests for {@link DurationCodec}.
 */
class DurationCodecTest implements CodecTestSupport<Duration, NormalFieldValue, Class<? super Duration>> {

    private static final long MAX_SECONDS = TimeUnit.HOURS.toSeconds(838) + TimeUnit.MINUTES.toSeconds(59) + 59;

    private static final long MAX_NANOS = TimeUnit.MICROSECONDS.toNanos(999999);

    private final Duration[] durations = {
        Duration.ZERO,
        Duration.ofSeconds(1),
        Duration.ofSeconds(-1),
        Duration.ofSeconds(10, 1000L),
        Duration.ofSeconds(-10, -1000L),
        Duration.ofSeconds(30, 200000L),
        Duration.ofSeconds(-30, -200000L),
        Duration.ofSeconds(1234567, 89101112L),
        Duration.ofSeconds(-1234567, -89101112L),
        Duration.ofSeconds(1234567, 800000000L),
        Duration.ofSeconds(-1234567, -800000000L),
        Duration.ofSeconds(MAX_SECONDS, MAX_NANOS),
        Duration.ofSeconds(-MAX_SECONDS, -MAX_NANOS),
        // Following should not be permitted by MySQL server, but also test.
        Duration.ofSeconds(Integer.MAX_VALUE, Long.MAX_VALUE),
        Duration.ofSeconds(Integer.MIN_VALUE, Long.MIN_VALUE),
    };

    @Override
    public DurationCodec getCodec() {
        return DurationCodec.INSTANCE;
    }

    @Override
    public Duration[] originParameters() {
        return durations;
    }

    @Override
    public Object[] stringifyParameters() {
        String[] results = new String[durations.length];
        for (int i = 0; i < results.length; ++i) {
            if (durations[i].isNegative()) {
                results[i] = String.format("'-%s'", format(durations[i].abs()));
            } else {
                results[i] = String.format("'%s'", format(durations[i]));
            }
        }
        return results;
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
