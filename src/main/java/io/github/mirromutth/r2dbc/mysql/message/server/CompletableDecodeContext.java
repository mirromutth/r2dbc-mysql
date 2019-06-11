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

package io.github.mirromutth.r2dbc.mysql.message.server;

/**
 * Base class considers {@link #isCompleted()} for {@link DecodeContext},
 * if it completed, should change context to {@link #nextContext()}, and
 * read a server-side message faked by {@link CompletableDecodeContext}.
 */
public interface CompletableDecodeContext extends DecodeContext {

    boolean isCompleted();

    DecodeContext nextContext();

    ServerMessage fakeMessage();
}
