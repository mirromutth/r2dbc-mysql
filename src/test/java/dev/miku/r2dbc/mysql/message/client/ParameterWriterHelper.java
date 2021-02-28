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

package dev.miku.r2dbc.mysql.message.client;

import dev.miku.r2dbc.mysql.ParameterWriter;
import dev.miku.r2dbc.mysql.Query;
import org.junit.platform.commons.util.ReflectionUtils;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A helper of {@link ParameterWriter} for unit tests.
 * <p>
 * Use reflection to access private methods/fields.
 */
public final class ParameterWriterHelper {

    private static final Constructor<ParamWriter> CONSTRUCTOR =
        ReflectionUtils.getDeclaredConstructor(ParamWriter.class);

    private static final Method FLUSH_PARAMETER =
        ReflectionUtils.findMethod(ParamWriter.class, "flushParameter", Void.class)
            .orElseThrow(RuntimeException::new);

    private static final Method TO_SQL =
        ReflectionUtils.findMethod(ParamWriter.class, "toSql")
            .orElseThrow(RuntimeException::new);

    public static ParameterWriter get(Query query) {
        assertThat(query.getPartSize()).isGreaterThan(1);

        return ReflectionUtils.newInstance(CONSTRUCTOR, query);
    }

    public static String toSql(ParameterWriter writer) {
        ParamWriter w = (ParamWriter) writer;

        ReflectionUtils.invokeMethod(FLUSH_PARAMETER, w, (Void) null);
        return (String) ReflectionUtils.invokeMethod(TO_SQL, w);
    }

    private static final class Iter implements Iterator<String> {

        private int leftover;

        private Iter(int leftover) {
            this.leftover = leftover;
        }

        @Override
        public boolean hasNext() {
            return leftover > 0;
        }

        @Override
        public String next() {
            if (leftover-- > 0) {
                return "";
            }

            throw new NoSuchElementException();
        }
    }

    private ParameterWriterHelper() { }
}
