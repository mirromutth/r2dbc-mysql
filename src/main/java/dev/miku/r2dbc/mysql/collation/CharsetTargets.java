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

package dev.miku.r2dbc.mysql.collation;

import java.nio.charset.StandardCharsets;

/**
 * A constant utility that is contains all {@link CharsetTarget}s.
 */
final class CharsetTargets {

    static final CharsetTarget ASCII = new CachedCharsetTarget(1, StandardCharsets.US_ASCII);

    // Windows-1252 is a superset of ISO 8859-1 (also real Latin 1), which by standards should be
    // considered the same encoding
    static final CharsetTarget LATIN1 = new MixCharsetTarget(1, StandardCharsets.ISO_8859_1,
        new NamedCharsetTarget(1, "Cp1252"));

    static final CharsetTarget UTF8 = new CachedCharsetTarget(3, StandardCharsets.UTF_8);

    static final CharsetTarget UTF8MB4 = new CachedCharsetTarget(4, StandardCharsets.UTF_8);

    static final CharsetTarget UTF16 = new CachedCharsetTarget(4, StandardCharsets.UTF_16);

    // UCS2 is 2-bytes encoded by MySQL, but it is also `UTF-16` in JVM, 4-bytes each character.
    static final CharsetTarget UCS2 = UTF16;

    static final CharsetTarget UTF16LE = new CachedCharsetTarget(4, StandardCharsets.UTF_16LE);

    static final CharsetTarget UTF32 = new NamedCharsetTarget(4, "UTF-32");

    static final CharsetTarget SWE7 = LATIN1;

    static final CharsetTarget HP8 = LATIN1;

    static final CharsetTarget DEC8 = LATIN1;

    static final CharsetTarget ARMSCII8 = LATIN1;

    static final CharsetTarget GEOSTD8 = LATIN1;

    static final CharsetTarget BIG5 = new NamedCharsetTarget(2, "Big5");

    static final CharsetTarget EUC_KR = new NamedCharsetTarget(2, "EUC-KR");

    static final CharsetTarget EUC_JIS = new NamedCharsetTarget(3, "EUC-JP");

    static final CharsetTarget EUC_JPMS = new NamedCharsetTarget(3, "eucJP-OPEN");

    static final CharsetTarget GB2312 = new NamedCharsetTarget(2, "GB2312");

    static final CharsetTarget GB18030 = new NamedCharsetTarget(4, "GB18030");

    static final CharsetTarget GBK = new NamedCharsetTarget(2, "GBK");

    static final CharsetTarget CP850 = new MixCharsetTarget(1, new NamedCharsetTarget(1, "Cp850"),
        new NamedCharsetTarget(1, "Cp437"));

    static final CharsetTarget CP852 = new NamedCharsetTarget(1, "Cp852");

    static final CharsetTarget CP866 = new NamedCharsetTarget(1, "Cp866");

    /**
     * Looks like JVM not support "Cp895" (also KAMENICKY, KEYBCS2), but it has some close to "Cp852".
     * <p>
     * See also:
     * <ul><li>https://en.wikipedia.org/wiki/Kamenick%C3%BD_encoding</li><li>
     * https://en.wikipedia.org/wiki/Code_page_852</li></ul>
     */
    static final CharsetTarget KEYBCS2 = CP852;

    // MySQL called "Cp932" for "Windows-932", but JVM not support "Cp932" aliases. It is also "Windows-31J".
    static final CharsetTarget CP932 = new NamedCharsetTarget(2, "WINDOWS-932");

    static final CharsetTarget SHIFT_JIS = new MixCharsetTarget(2, new NamedCharsetTarget(2, "SHIFT-JIS"),
        new NamedCharsetTarget(2, "Cp943"), CP932);

    // Note: also "Cp5346"
    static final CharsetTarget CP1250 = new NamedCharsetTarget(1, "Cp1250");

    // Note: also "Cp5347"
    static final CharsetTarget CP1251 = new NamedCharsetTarget(1, "Cp1251");

    static final CharsetTarget CP1256 = new NamedCharsetTarget(1, "Cp1256");

    // Note: also "Cp5353"
    static final CharsetTarget CP1257 = new NamedCharsetTarget(1, "Cp1257");

    static final CharsetTarget TIS620 = new NamedCharsetTarget(1, "TIS620");

    static final CharsetTarget KOI8_R = new NamedCharsetTarget(1, "KOI8-R");

    /**
     * Note: MySQL maybe refer to KOI8-R as koi8u, but they are different charsets. But yet, they are very
     * close, should keep looking. Truly name is "KOI8-U".
     */
    static final CharsetTarget KOI8_U = KOI8_R;

    static final CharsetTarget LATIN2 = new NamedCharsetTarget(1, "ISO-8859-2");

    // "ISO-8859-7" is "greek" aliases.
    static final CharsetTarget GREEK = new NamedCharsetTarget(1, "ISO-8859-7");

    // "ISO-8859-8" is "hebrew" aliases.
    static final CharsetTarget HEBREW = new NamedCharsetTarget(1, "ISO-8859-8");

    // "ISO-8859-9" is "latin5" aliases.
    static final CharsetTarget LATIN5 = new NamedCharsetTarget(1, "ISO-8859-9");

    // "ISO-8859-13" is "latin7" aliases.
    static final CharsetTarget LATIN7 = new NamedCharsetTarget(1, "ISO-8859-13");

    static final CharsetTarget MAC_ROMAN = new NamedCharsetTarget(1, "MacRoman");

    static final CharsetTarget MAC_CE = new NamedCharsetTarget(1, "MacCentralEurope");

    private CharsetTargets() { }
}
