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

package dev.miku.r2dbc.mysql;

import dev.miku.r2dbc.mysql.util.OperatorUtils;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static dev.miku.r2dbc.mysql.util.AssertUtils.require;
import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link Query} considers client-preparing statements.
 */
final class TextQuery extends Query {

    private final Map<String, ParameterIndex> nameKeyedIndex;

    private final List<String> sqlParts;

    TextQuery(Map<String, ParameterIndex> nameKeyedIndex, List<String> sqlParts) {
        requireNonNull(nameKeyedIndex, "named parameter map must not be null");
        requireNonNull(sqlParts, "sql parts must not be null");
        require(sqlParts.size() > 1, "sql parts need least 2 parts");

        this.nameKeyedIndex = nameKeyedIndex;
        this.sqlParts = sqlParts;
    }

    @Override
    int getParameters() {
        return sqlParts.size() - 1;
    }

    @Override
    ParameterIndex getIndexes(String identifier) {
        ParameterIndex index = nameKeyedIndex.get(identifier);

        if (index == null) {
            throw new IllegalArgumentException(String.format("No such parameter with identifier '%s'", identifier));
        }

        return index;
    }

    Flux<String> formatSql(List<Binding> bindings) {
        requireNonNull(bindings, "bindings must not be null");

        // No need defer because it must be called by inner of Flux.defer.
        if (bindings.isEmpty()) {
            return Flux.empty();
        }

        Iterator<Binding> iter = bindings.iterator();
        EmitterProcessor<Binding> processor = EmitterProcessor.create(1, true);
        Consumer<String> succeed = ignored -> OperatorUtils.emitIterator(processor, iter);

        processor.onNext(iter.next());

        return processor.concatMap(it -> it.toSql(sqlParts).doOnSuccess(succeed))
            .doOnCancel(() -> Binding.clearSubsequent(iter))
            .doOnError(ignored -> Binding.clearSubsequent(iter));
    }

    /**
     * Visible for unit tests.
     *
     * @return parameter name set
     */
    Set<String> getParameterNames() {
        return nameKeyedIndex.keySet();
    }

    /**
     * Visible for unit tests.
     *
     * @return split SQL parts
     */
    List<String> getSqlParts() {
        return sqlParts;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TextQuery)) {
            return false;
        }

        TextQuery textQuery = (TextQuery) o;

        if (!nameKeyedIndex.equals(textQuery.nameKeyedIndex)) {
            return false;
        }
        return sqlParts.equals(textQuery.sqlParts);
    }

    @Override
    public int hashCode() {
        return 31 * nameKeyedIndex.hashCode() + sqlParts.hashCode();
    }

    @Override
    public String toString() {
        return String.format("TextQuery{nameKeyedIndex=%s, sqlParts=%s}", nameKeyedIndex, sqlParts);
    }
}
