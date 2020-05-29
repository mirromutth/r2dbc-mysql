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

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

/**
 * Unit tests for {@link ParamWriter}.
 */
class ParamWriterTest {

    private static final int SIZE = 10;

    @Test
    void badFollowString() {
        assertThatIllegalStateException().isThrownBy(() -> stringWriter().writeNull());
        assertThatIllegalStateException().isThrownBy(() -> stringWriter().writeBinary(true));
        assertThatIllegalStateException().isThrownBy(() -> stringWriter().writeBinary(false));
        assertThatIllegalStateException().isThrownBy(() -> stringWriter().writeHex(new byte[0]));
        assertThatIllegalStateException().isThrownBy(() -> stringWriter().writeHex(ByteBuffer.allocate(0)));
    }

    @Test
    void stringFollowString() {
        ParamWriter writer = stringWriter();
        writer.write("abc");
        writer.write('1');
        writer.write("define", 2, 3);
        assertThat(ParameterWriterHelper.toSql(writer)).isEqualTo("'0abc1fin'");
    }

    @Test
    void numericFollowString() {
        ParamWriter writer = stringWriter();
        writer.writeInt(1);
        writer.writeLong(2);
        writer.writeFloat(3.4f);
        writer.writeDouble(5.6);
        writer.writeBigInteger(BigInteger.valueOf(7));
        writer.writeBigDecimal(BigDecimal.valueOf(8.9));
        assertThat(ParameterWriterHelper.toSql(writer)).isEqualTo("'0123.45.678.9'");
    }

    @Test
    void badFollowNull() {
        assertThatIllegalStateException().isThrownBy(() -> nullWriter().writeNull());
        assertThatIllegalStateException().isThrownBy(() -> nullWriter().writeBinary(true));
        assertThatIllegalStateException().isThrownBy(() -> nullWriter().writeBinary(false));
        assertThatIllegalStateException().isThrownBy(() -> nullWriter().writeHex(new byte[0]));
        assertThatIllegalStateException().isThrownBy(() -> nullWriter().writeHex(ByteBuffer.allocate(0)));
        assertThatIllegalStateException().isThrownBy(() -> nullWriter().append(' '));
        assertThatIllegalStateException().isThrownBy(() -> nullWriter().append("123"));
        assertThatIllegalStateException().isThrownBy(() -> nullWriter().append("123", 1, 2));
        assertThatIllegalStateException().isThrownBy(() -> nullWriter().write(' '));
        assertThatIllegalStateException().isThrownBy(() -> nullWriter().write("123"));
        assertThatIllegalStateException().isThrownBy(() -> nullWriter().write("123", 1, 2));
        assertThatIllegalStateException().isThrownBy(() -> nullWriter().writeDouble(0.1));
    }

    @Test
    void appendPart() {
        ParamWriter writer = (ParamWriter) ParameterWriterHelper.get(1);
        writer.append("define", 2, 5);
        assertThat(ParameterWriterHelper.toSql(writer)).isEqualTo("'fin'");
    }

    @Test
    void writePart() {
        ParamWriter writer = (ParamWriter) ParameterWriterHelper.get(1);
        writer.write("define", 2, 3);
        assertThat(ParameterWriterHelper.toSql(writer)).isEqualTo("'fin'");
    }

    @Test
    void appendNull() {
        assertThat(ParameterWriterHelper.toSql(ParameterWriterHelper.get(1).append(null)))
            .isEqualTo("'null'");
        assertThat(ParameterWriterHelper.toSql(ParameterWriterHelper.get(1).append(null, 1, 3)))
            .isEqualTo("'ul'");
    }

    @Test
    void writeNull() {
        ParamWriter writer = (ParamWriter) ParameterWriterHelper.get(1);
        writer.write((String) null);
        assertThat(ParameterWriterHelper.toSql(writer)).isEqualTo("'null'");

        writer = (ParamWriter) ParameterWriterHelper.get(1);
        writer.write((String) null, 1, 2);
        assertThat(ParameterWriterHelper.toSql(writer)).isEqualTo("'ul'");

        writer = (ParamWriter) ParameterWriterHelper.get(1);
        writer.write((char[]) null);
        assertThat(ParameterWriterHelper.toSql(writer)).isEqualTo("'null'");

        writer = (ParamWriter) ParameterWriterHelper.get(1);
        writer.write((char[]) null, 1, 2);
        assertThat(ParameterWriterHelper.toSql(writer)).isEqualTo("'ul'");
    }

    @Test
    void publishSuccess() {
        MockParameter[] values = new MockParameter[SIZE];

        for (int i = 0; i < SIZE; ++i) {
            values[i] = new MockParameter(true);
        }

        Flux.from(ParamWriter.publish(emptyStrings(), values))
            .as(StepVerifier::create)
            .expectNext(new String(new char[SIZE]).replace("\0", "''"))
            .verifyComplete();

        assertThat(values).extracting(MockParameter::refCnt).containsOnly(0);
    }

    @Test
    void publishPartially() {
        MockParameter[] values = new MockParameter[SIZE];

        int i = 0;

        for (; i < SIZE >>> 1; ++i) {
            values[i] = new MockParameter(true);
        }

        for (; i < SIZE; ++i) {
            values[i] = new MockParameter(false);
        }

        Flux.from(ParamWriter.publish(emptyStrings(), values))
            .as(StepVerifier::create)
            .verifyError(MockException.class);

        assertThat(values).extracting(MockParameter::refCnt).containsOnly(0);
    }

    @Test
    void publishNothing() {
        MockParameter[] values = new MockParameter[SIZE];

        for (int i = 0; i < SIZE; ++i) {
            values[i] = new MockParameter(false);
        }

        Flux.from(ParamWriter.publish(emptyStrings(), values))
            .as(StepVerifier::create)
            .verifyError(MockException.class);

        assertThat(values).extracting(MockParameter::refCnt).containsOnly(0);
    }

    private static List<String> emptyStrings() {
        List<String> l = new ArrayList<>(SIZE + 1);

        for (int i = 0; i < SIZE + 1; ++i) {
            l.add("");
        }

        return l;
    }

    private static List<String> spaceStrings() {
        List<String> l = new ArrayList<>(SIZE + 1);
        l.add("");

        for (int i = 1; i < SIZE; ++i) {
            l.add(" ");
        }

        l.add("");
        return l;
    }

    private static ParamWriter stringWriter() {
        ParamWriter writer = (ParamWriter) ParameterWriterHelper.get(1);
        writer.write('0');
        return writer;
    }

    private static ParamWriter nullWriter() {
        ParamWriter writer = (ParamWriter) ParameterWriterHelper.get(1);
        writer.writeNull();
        return writer;
    }
}
