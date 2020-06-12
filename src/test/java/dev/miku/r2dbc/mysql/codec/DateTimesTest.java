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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link DateTimes}.
 */
class DateTimesTest {

    @Test
    void readMicroInDigits() {
        assertThat(DateTimes.readMicroInDigits(Unpooled.wrappedBuffer("".getBytes())))
            .isEqualTo(0);
        assertThat(DateTimes.readMicroInDigits(Unpooled.wrappedBuffer("1".getBytes())))
            .isEqualTo(100_000);
        assertThat(DateTimes.readMicroInDigits(Unpooled.wrappedBuffer("9".getBytes())))
            .isEqualTo(900_000);
        assertThat(DateTimes.readMicroInDigits(Unpooled.wrappedBuffer("05".getBytes())))
            .isEqualTo(50_000);
        assertThat(DateTimes.readMicroInDigits(Unpooled.wrappedBuffer("0012".getBytes())))
            .isEqualTo(1200);
        assertThat(DateTimes.readMicroInDigits(Unpooled.wrappedBuffer("00004897".getBytes())))
            .isEqualTo(48);
        assertThat(DateTimes.readMicroInDigits(Unpooled.wrappedBuffer("1321a469789".getBytes())))
            .isEqualTo(132100);
        assertThat(DateTimes.readMicroInDigits(Unpooled.wrappedBuffer("001321a469789".getBytes())))
            .isEqualTo(1321);
    }

    @Test
    void readIntInDigits() {
        assertThat(DateTimes.readIntInDigits(Unpooled.wrappedBuffer("".getBytes())))
            .isEqualTo(0);
        assertThat(DateTimes.readIntInDigits(Unpooled.wrappedBuffer("1".getBytes())))
            .isEqualTo(1);
        assertThat(DateTimes.readIntInDigits(Unpooled.wrappedBuffer("123456".getBytes())))
            .isEqualTo(123456);
        assertThat(DateTimes.readIntInDigits(Unpooled.wrappedBuffer("04897".getBytes())))
            .isEqualTo(4897);
        assertThat(DateTimes.readIntInDigits(Unpooled.wrappedBuffer("00004897".getBytes())))
            .isEqualTo(4897);
        assertThat(DateTimes.readIntInDigits(Unpooled.wrappedBuffer("1321a469789".getBytes())))
            .isEqualTo(1321);
        ByteBuf buf = Unpooled.wrappedBuffer("10:59:32.4213".getBytes());
        assertThat(DateTimes.readIntInDigits(buf)).isEqualTo(10);
        assertThat(DateTimes.readIntInDigits(buf)).isEqualTo(59);
        assertThat(DateTimes.readIntInDigits(buf)).isEqualTo(32);
        buf = Unpooled.wrappedBuffer("::.4213".getBytes());
        assertThat(DateTimes.readIntInDigits(buf)).isEqualTo(0);
        assertThat(DateTimes.readIntInDigits(buf)).isEqualTo(0);
        assertThat(DateTimes.readIntInDigits(buf)).isEqualTo(0);
    }
}
