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

import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;

import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * An utility data parser for {@link Option}.
 *
 * @see MySqlConnectionFactoryProvider using this utility.
 * @since 0.8.2
 */
final class OptionMapper {

    private final ConnectionFactoryOptions options;

    OptionMapper(ConnectionFactoryOptions options) {
        this.options = options;
    }

    SourceSpec from(Option<?> option) {
        return new SourceSpec(options, option);
    }

    <T> void consume(Option<T> option, Consumer<T> consumer, Class<T> clazz) {
        T t = clazz.cast(options.getValue(option));

        if (t != null) {
            consumer.accept(t);
        }
    }

    <T> void requiredConsume(Option<T> option, Consumer<T> consumer, Class<T> clazz) {
        consumer.accept(clazz.cast(options.getRequiredValue(option)));
    }
}

final class SourceSpec {

    private final ConnectionFactoryOptions options;

    private final Option<?> option;

    SourceSpec(ConnectionFactoryOptions options, Option<?> option) {
        this.options = options;
        this.option = option;
    }

    <T> Source<T> asInstance(Class<T> type) {
        Object value = options.getValue(option);

        if (value == null) {
            return Source.nilSource();
        }

        if (type.isInstance(value)) {
            return new Source.Impl<>(type.cast(value));
        } else if (value instanceof String) {
            try {
                Class<?> impl = Class.forName((String) value);

                if (type.isAssignableFrom(impl)) {
                    return new Source.Impl<>(type.cast(impl.getDeclaredConstructor().newInstance()));
                }
                // Otherwise not an implementation, convert failed.
            } catch (ReflectiveOperationException e) {
                throw new IllegalArgumentException("Cannot instantiate '" + value + "'", e);
            }
        }

        throw new IllegalArgumentException(toMessage(option, value, type.getName()));
    }

    <T> Source<T> asInstance(Class<T> type, Function<String, T> mapping) {
        Object value = options.getValue(option);

        if (value == null) {
            return Source.nilSource();
        }

        if (type.isInstance(value)) {
            return new Source.Impl<>(type.cast(value));
        } else if (value instanceof String) {
            // Type cast for check mapping result.
            return new Source.Impl<>(type.cast(mapping.apply((String) value)));
        }

        throw new IllegalArgumentException(toMessage(option, value, type.getTypeName()));
    }

    Source<String[]> asStrings() {
        Object value = options.getValue(option);

        if (value == null) {
            return Source.nilSource();
        }

        if (value instanceof String[]) {
            return new Source.Impl<>((String[]) value);
        } else if (value instanceof String) {
            return new Source.Impl<>(((String) value).split(","));
        } else if (value instanceof Collection<?>) {
            return new Source.Impl<>(((Collection<?>) value).stream()
                .map(String.class::cast).toArray(String[]::new));
        }

        throw new IllegalArgumentException(toMessage(option, value, "String[]"));
    }

    Source<Boolean> asBoolean() {
        Object value = options.getValue(option);

        if (value == null) {
            return Source.nilSource();
        }

        if (value instanceof Boolean) {
            return new Source.Impl<>((Boolean) value);
        } else if (value instanceof String) {
            return new Source.Impl<>(Boolean.parseBoolean((String) value));
        }

        throw new IllegalArgumentException(toMessage(option, value, "Boolean"));
    }

    Source<Integer> asInt() {
        Object value = options.getValue(option);

        if (value == null) {
            return Source.nilSource();
        }

        if (value instanceof Integer) {
            // Reduce the cost of re-boxed.
            return new Source.Impl<>((Integer) value);
        } else if (value instanceof Number) {
            return new Source.Impl<>(((Number) value).intValue());
        } else if (value instanceof String) {
            return new Source.Impl<>(Integer.parseInt((String) value));
        }

        throw new IllegalArgumentException(toMessage(option, value, "Integer"));
    }

    Source<String> asString() {
        Object value = options.getValue(option);

        if (value == null) {
            return Source.nilSource();
        }

        if (value instanceof String) {
            return new Source.Impl<>((String) value);
        }

        throw new IllegalArgumentException(toMessage(option, value, "String"));
    }

    @SuppressWarnings("unchecked")
    void servePrepare(Consumer<Boolean> enables, Consumer<Predicate<String>> preferred) {
        Object value = options.getValue(option);

        if (value == null) {
            return;
        }

        if (value instanceof Boolean) {
            enables.accept((Boolean) value);
            return;
        } else if (value instanceof Predicate<?>) {
            preferred.accept((Predicate<String>) value);
            return;
        } else if (value instanceof String) {
            String serverPreparing = (String) value;

            if ("true".equalsIgnoreCase(serverPreparing) || "false".equalsIgnoreCase(serverPreparing)) {
                enables.accept(Boolean.parseBoolean(serverPreparing));
                return;
            }

            try {
                Class<?> impl = Class.forName(serverPreparing);

                if (Predicate.class.isAssignableFrom(impl)) {
                    preferred.accept((Predicate<String>) impl.getDeclaredConstructor().newInstance());
                    return;
                }
                // Otherwise not an implementation, convert failed.
            } catch (ReflectiveOperationException e) {
                throw new IllegalArgumentException("Cannot instantiate '" + value + "'", e);
            }
        }

        throw new IllegalArgumentException(toMessage(option, value, "Boolean or Predicate<String>"));
    }

    private static String toMessage(Option<?> option, Object value, String type) {
        return "Cannot convert value " + value + " of " + value.getClass() + " as " + type + " for option " +
            option.name();
    }
}

@FunctionalInterface
interface Source<T> {

    Otherwise into(Consumer<T> consumer);

    @SuppressWarnings("unchecked")
    static <T> Source<T> nilSource() {
        return (Source<T>) Nil.INSTANCE;
    }

    final class Impl<T> implements Source<T> {

        private final T value;

        Impl(T value) {
            this.value = value;
        }

        @Override
        public Otherwise into(Consumer<T> consumer) {
            consumer.accept(value);
            return Otherwise.NOOP;
        }
    }

    enum Nil implements Source<Object> {

        INSTANCE;

        @Override
        public Otherwise into(Consumer<Object> consumer) {
            return Otherwise.FALL;
        }
    }
}

@FunctionalInterface
interface Otherwise {

    Otherwise NOOP = ignored -> { };

    Otherwise FALL = Runnable::run;

    /**
     * Invoked if the previous {@link Source} outcome did not match.
     *
     * @param runnable the {@link Runnable} that should be invoked.
     */
    void otherwise(Runnable runnable);
}
