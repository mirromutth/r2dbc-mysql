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

package dev.miku.r2dbc.mysql.collation;

import java.nio.charset.Charset;

/**
 * Character collation those use already cached {@link CharsetTarget} of MySQL
 */
final class CachedCharCollation extends AbstractCharCollation {

    CachedCharCollation(int id, String name, CharsetTarget target) {
        super(id, name, target);
    }

    @Override
    public Charset getCharset() {
        return target.getCharset();
    }

    @Override
    public String toString() {
        return String.format("CachedCharCollation{id=%d, name='%s', target=%s}", id, name, target);
    }
}
