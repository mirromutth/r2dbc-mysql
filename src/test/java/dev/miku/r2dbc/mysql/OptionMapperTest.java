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

package dev.miku.r2dbc.mysql;

import dev.miku.r2dbc.mysql.constant.SslMode;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.NoSuchOptionException;
import io.r2dbc.spi.Option;
import org.junit.jupiter.api.Test;
import reactor.util.annotation.Nullable;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.SSL;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link OptionMapper}.
 */
class OptionMapperTest {

    @Test
    void to() {
        AtomicReference<Object> ref = new AtomicReference<>();

        new OptionMapper(ConnectionFactoryOptions.builder()
            .option(USER, "no-root")
            .build())
            .requires(USER)
            .to(ref::set);

        assertThat(ref.get()).isEqualTo("no-root");

        new OptionMapper(ConnectionFactoryOptions.builder()
            .option(HOST, "my-localhost")
            .build())
            .requires(HOST)
            .to(ref::set);

        assertThat(ref.get()).isEqualTo("my-localhost");

        new OptionMapper(ConnectionFactoryOptions.builder()
            .option(SSL, true)
            .build())
            .requires(SSL)
            .to(ref::set);

        assertThat(ref.get()).isEqualTo(true);

        new OptionMapper(ConnectionFactoryOptions.builder()
            .option(Option.valueOf("sslMode"), SslMode.VERIFY_IDENTITY)
            .build())
            .requires(Option.valueOf("sslMode"))
            .to(ref::set);

        assertThat(ref.get()).isEqualTo(SslMode.VERIFY_IDENTITY);

        Predicate<String> predicate = sql -> sql.startsWith("SELECT");

        new OptionMapper(ConnectionFactoryOptions.builder()
            .option(Option.valueOf("useServerPrepareStatement"), predicate)
            .build())
            .requires(Option.valueOf("useServerPrepareStatement"))
            .to(ref::set);

        assertThat(ref.get()).isSameAs(predicate);
    }

    @Test
    void otherwiseNoop() {
        Object fill = new Object();
        AtomicReference<Object> ref = new AtomicReference<>(fill);
        AtomicReference<Object> other = new AtomicReference<>(fill);

        new OptionMapper(ConnectionFactoryOptions.builder()
            .option(USER, "no-root")
            .build())
            .requires(USER)
            .to(ref::set)
            .otherwise(() -> other.set(8));

        assertThat(ref.get()).isEqualTo("no-root");
        assertThat(other.get()).isSameAs(fill);
    }

    @Test
    void otherwiseFall() {
        Object fill = new Object();
        AtomicReference<Object> ref = new AtomicReference<>(fill);
        AtomicReference<Object> other = new AtomicReference<>(fill);

        new OptionMapper(ConnectionFactoryOptions.builder()
            .build())
            .optional(USER)
            .to(ref::set)
            .otherwise(() -> other.set(8));

        assertThat(ref.get()).isSameAs(fill);
        assertThat(other.get()).isEqualTo(8);
    }

    @Test
    void requires() {
        assertThat(new OptionMapper(ConnectionFactoryOptions.builder()
            .option(USER, "no-root")
            .build())
            .requires(USER)).isNotNull();
    }

    @Test
    void optional() {
        assertThat(new OptionMapper(ConnectionFactoryOptions.builder()
            .option(USER, "no-root")
            .build())
            .optional(USER)).isNotNull();
    }

    @Test
    void requiresNoSuch() {
        assertThatExceptionOfType(NoSuchOptionException.class)
            .isThrownBy(() -> new OptionMapper(ConnectionFactoryOptions.builder().build()).requires(HOST));
        assertThatExceptionOfType(NoSuchOptionException.class)
            .isThrownBy(() -> new OptionMapper(ConnectionFactoryOptions.builder()
                .option(USER, "no-root")
                .build())
                .requires(HOST));
        assertThatExceptionOfType(NoSuchOptionException.class)
            .isThrownBy(() -> new OptionMapper(ConnectionFactoryOptions.builder()
                .option(HOST, "my-localhost")
                .build())
                .requires(USER));
    }

    @Test
    void optionalNoSuch() {
        Object fill = new Object();
        AtomicReference<Object> ref = new AtomicReference<>(fill);

        new OptionMapper(ConnectionFactoryOptions.builder().build()).optional(HOST).to(ref::set);

        assertThat(ref.get()).isSameAs(fill);

        new OptionMapper(ConnectionFactoryOptions.builder()
            .option(USER, "no-root")
            .build())
            .optional(HOST)
            .to(ref::set);

        assertThat(ref.get()).isSameAs(fill);

        new OptionMapper(ConnectionFactoryOptions.builder()
            .option(HOST, "my-localhost")
            .build())
            .optional(USER)
            .to(ref::set);

        assertThat(ref.get()).isSameAs(fill);
    }

    @Test
    void as() {
        AtomicReference<Mock> ref = new AtomicReference<>();

        new OptionMapper(ConnectionFactoryOptions.builder()
            .option(Option.valueOf("mock"), new MockPrivateImpl())
            .build())
            .requires(Option.valueOf("mock"))
            .as(Mock.class)
            .to(ref::set);

        assertThat(ref.get()).isExactlyInstanceOf(MockPrivateImpl.class)
            .extracting(Mock::get)
            .isEqualTo(MockPrivateImpl.class.getName());

        new OptionMapper(ConnectionFactoryOptions.builder()
            .option(Option.valueOf("mock"), MockImpl.class.getName())
            .build())
            .requires(Option.valueOf("mock"))
            .as(Mock.class)
            .to(ref::set);

        assertThat(ref.get()).isExactlyInstanceOf(MockImpl.class)
            .extracting(Mock::get)
            .isEqualTo(MockImpl.class.getName());

        new OptionMapper(ConnectionFactoryOptions.builder()
            .option(Option.valueOf("mock"), new AbstractMock() { })
            .build())
            .requires(Option.valueOf("mock"))
            .as(Mock.class)
            .to(ref::set);

        assertThat(ref.get()).isInstanceOf(AbstractMock.class)
            .extracting(Mock::get)
            .isEqualTo(AbstractMock.class.getName());
    }

    @Test
    void asMapping() {
        AtomicReference<Mock> ref = new AtomicReference<>();

        new OptionMapper(ConnectionFactoryOptions.builder()
            .option(Option.valueOf("mock"), new MockImpl())
            .build())
            .requires(Option.valueOf("mock"))
            .as(Mock.class, it -> {
                throw new IllegalStateException(it);
            })
            .to(ref::set);

        assertThat(ref.get()).isExactlyInstanceOf(MockImpl.class)
            .extracting(Mock::get)
            .isEqualTo(MockImpl.class.getName());

        new OptionMapper(ConnectionFactoryOptions.builder()
            .option(Option.valueOf("mock"), "options-mock-name")
            .build())
            .requires(Option.valueOf("mock"))
            .as(Mock.class, MockImpl::new)
            .to(ref::set);

        assertThat(ref.get()).isExactlyInstanceOf(MockImpl.class)
            .extracting(Mock::get)
            .isEqualTo("options-mock-name");

        new OptionMapper(ConnectionFactoryOptions.builder()
            .option(Option.valueOf("mock"), "interface-mock-name")
            .build())
            .requires(Option.valueOf("mock"))
            .as(Mock.class, it -> () -> it)
            .to(ref::set);

        assertThat(ref.get()).isInstanceOf(Mock.class)
            .extracting(Mock::get)
            .isEqualTo("interface-mock-name");
    }

    @Test
    void prepare() {
        Runnable thrown = () -> {
            throw new IllegalStateException("Should not be here");
        };
        Predicate<String> selectPreferred = sql -> sql.startsWith("SELECT");
        AtomicInteger counter = new AtomicInteger();
        AtomicReference<Predicate<String>> ref = new AtomicReference<>();

        preparingOptions(null).optional(Option.valueOf("useServerPrepareStatement"))
            .prepare(thrown, thrown, ignored -> thrown.run());

        preparingOptions(false).optional(Option.valueOf("useServerPrepareStatement"))
            .prepare(counter::getAndIncrement, thrown, ignored -> thrown.run());
        assertThat(counter.get()).isEqualTo(1);

        preparingOptions("false").optional(Option.valueOf("useServerPrepareStatement"))
            .prepare(counter::getAndIncrement, thrown, ignored -> thrown.run());
        assertThat(counter.get()).isEqualTo(2);

        preparingOptions("False").optional(Option.valueOf("useServerPrepareStatement"))
            .prepare(counter::getAndIncrement, thrown, ignored -> thrown.run());
        assertThat(counter.get()).isEqualTo(3);

        preparingOptions("FALSE").optional(Option.valueOf("useServerPrepareStatement"))
            .prepare(counter::getAndIncrement, thrown, ignored -> thrown.run());
        assertThat(counter.get()).isEqualTo(4);

        preparingOptions(true).optional(Option.valueOf("useServerPrepareStatement"))
            .prepare(thrown, counter::getAndIncrement, ignored -> thrown.run());
        assertThat(counter.get()).isEqualTo(5);

        preparingOptions("true").optional(Option.valueOf("useServerPrepareStatement"))
            .prepare(thrown, counter::getAndIncrement, ignored -> thrown.run());
        assertThat(counter.get()).isEqualTo(6);

        preparingOptions("True").optional(Option.valueOf("useServerPrepareStatement"))
            .prepare(thrown, counter::getAndIncrement, ignored -> thrown.run());
        assertThat(counter.get()).isEqualTo(7);

        preparingOptions("TRUE").optional(Option.valueOf("useServerPrepareStatement"))
            .prepare(thrown, counter::getAndIncrement, ignored -> thrown.run());
        assertThat(counter.get()).isEqualTo(8);

        preparingOptions(selectPreferred).optional(Option.valueOf("useServerPrepareStatement"))
            .prepare(thrown, thrown, ref::set);
        assertThat(ref.get()).isSameAs(selectPreferred);

        preparingOptions(MockPreparePreferred.class.getName())
            .optional(Option.valueOf("useServerPrepareStatement"))
            .prepare(thrown, thrown, ref::set);

        assertThat(ref.get()).isExactlyInstanceOf(MockPreparePreferred.class);
    }

    @Test
    void asFails() {
        assertThatIllegalArgumentException().isThrownBy(() -> new OptionMapper(ConnectionFactoryOptions.builder()
                .option(Option.valueOf("mock"), Mock.class.getName())
                .build())
                .requires(Option.valueOf("mock"))
                .as(Mock.class))
            .withCauseExactlyInstanceOf(NoSuchMethodException.class);

        assertThatIllegalArgumentException().isThrownBy(() -> new OptionMapper(ConnectionFactoryOptions.builder()
                .option(Option.valueOf("mock"), MockPrivateImpl.class.getName())
                .build())
                .requires(Option.valueOf("mock"))
                .as(Mock.class))
            .withCauseExactlyInstanceOf(IllegalAccessException.class);

        assertThatIllegalArgumentException().isThrownBy(() -> new OptionMapper(ConnectionFactoryOptions.builder()
                .option(Option.valueOf("mock"), AbstractMock.class.getName())
                .build())
                .requires(Option.valueOf("mock"))
                .as(Mock.class))
            .withCauseExactlyInstanceOf(InstantiationException.class);
    }

    private static OptionMapper preparingOptions(@Nullable Object value) {
        if (value == null) {
            return new OptionMapper(ConnectionFactoryOptions.builder().build());
        }

        return new OptionMapper(ConnectionFactoryOptions.builder()
            .option(Option.valueOf("useServerPrepareStatement"), value)
            .build());
    }

    @FunctionalInterface
    interface Mock {

        String get();
    }

    static abstract class AbstractMock implements Mock {

        private final String name;

        AbstractMock() {
            this(AbstractMock.class.getName());
        }

        AbstractMock(String name) {
            this.name = name;
        }

        @Override
        public String get() {
            return name;
        }
    }

    static final class MockImpl extends AbstractMock implements Mock {

        MockImpl() {
            super(MockImpl.class.getName());
        }

        MockImpl(String name) {
            super(name);
        }
    }

    static final class MockPrivateImpl extends AbstractMock implements Mock {

        private MockPrivateImpl() {
            super(MockPrivateImpl.class.getName());
        }
    }

    static final class MockPreparePreferred implements Predicate<String> {

        @Override
        public boolean test(String s) {
            return s.startsWith("INSERT");
        }
    }
}
