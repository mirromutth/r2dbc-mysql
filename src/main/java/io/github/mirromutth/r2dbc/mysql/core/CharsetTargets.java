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

package io.github.mirromutth.r2dbc.mysql.core;

import java.nio.charset.StandardCharsets;

/**
 * A constant utility that is contains all {@link CharsetTarget}s.
 */
final class CharsetTargets {

    private CharsetTargets() {
    }

    static final CharsetTarget GBK = new NamedCharsetTarget(2, "GBK");

    static final CharsetTarget US_ASCII = new CachedCharsetTarget(1, StandardCharsets.US_ASCII);

    // Windows-1252 is a superset of ISO 8859-1 (also real Latin 1), which by standards should be considered the same encoding
    static final CharsetTarget LATIN_1 = new MixCharsetTarget(
        1,
        StandardCharsets.ISO_8859_1,
        new NamedCharsetTarget(1, "Cp1252")
    );

    static final CharsetTarget UTF_8 = new CachedCharsetTarget(3, StandardCharsets.UTF_8);
    static final CharsetTarget UTF_8_MB4 = new CachedCharsetTarget(4, StandardCharsets.UTF_8);
    static final CharsetTarget UTF_16 = new CachedCharsetTarget(4, StandardCharsets.UTF_16);
    static final CharsetTarget UTF_16LE = new CachedCharsetTarget(4, StandardCharsets.UTF_16LE);
    static final CharsetTarget UTF_32 = new NamedCharsetTarget(4, "UTF-32");
}
