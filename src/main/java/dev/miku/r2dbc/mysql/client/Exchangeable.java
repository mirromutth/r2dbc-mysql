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

package dev.miku.r2dbc.mysql.client;

import dev.miku.r2dbc.mysql.message.client.ExchangeableMessage;
import dev.miku.r2dbc.mysql.message.server.ServerMessage;
import reactor.core.Disposable;

import java.util.function.Predicate;

/**
 * An entity for exchange messages with {@link Client}.
 */
public final class Exchangeable implements Disposable {

    final ExchangeableMessage request;

    final Predicate<ServerMessage> complete;

    final boolean takeComplete;

    public Exchangeable(ExchangeableMessage request, Predicate<ServerMessage> complete, boolean takeComplete) {
        this.request = request;
        this.complete = complete;
        this.takeComplete = takeComplete;
    }

    @Override
    public void dispose() {
        if (request instanceof Disposable) {
            ((Disposable) request).dispose();
        }
    }

    @Override
    public boolean isDisposed() {
        if (request instanceof Disposable) {
            return ((Disposable) request).isDisposed();
        }

        return true;
    }
}
