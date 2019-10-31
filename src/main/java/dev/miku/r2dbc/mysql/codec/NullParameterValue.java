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

package dev.miku.r2dbc.mysql.codec;

import dev.miku.r2dbc.mysql.constant.DataTypes;
import dev.miku.r2dbc.mysql.message.ParameterValue;
import dev.miku.r2dbc.mysql.message.client.ParameterWriter;
import reactor.core.publisher.Mono;

/**
 * An implementation of {@link ParameterValue} which considers value is {@code null}.
 * <p>
 * Note: the parameter is marked with a bitmap of {@code null}, so {@link #writeTo}
 * will not do anything.
 */
final class NullParameterValue implements ParameterValue {

    static final NullParameterValue INSTANCE = new NullParameterValue();

    private NullParameterValue() {
    }

    @Override
    public boolean isNull() {
        return true;
    }

    @Override
    public Mono<Void> writeTo(ParameterWriter writer) {
        return Mono.empty();
    }

    @Override
    public short getType() {
        return DataTypes.NULL;
    }

    @Override
    public void dispose() {
        // No resource to release.
    }

    @Override
    public String toString() {
        return "NullParameterValue{ null }";
    }
}
