/*
 * Copyright 2018-2019 the original author or authors.
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

/**
 * An implementation of {@link Query} considers simple statement which does not contains any parameter.
 * No need contains origin SQL here, so it is a singleton class.
 */
final class SimpleQuery extends Query {

    static final SimpleQuery INSTANCE = new SimpleQuery();

    @Override
    int getParameters() {
        return 0;
    }

    @Override
    ParameterIndex getIndexes(String identifier) {
        throw new IllegalArgumentException(String.format("No such parameter with identifier '%s'", identifier));
    }

    @Override
    public String toString() {
        return "SimpleQuery";
    }

    private SimpleQuery() {
    }
}
