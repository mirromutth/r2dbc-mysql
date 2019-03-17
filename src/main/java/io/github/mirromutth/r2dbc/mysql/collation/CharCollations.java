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

package io.github.mirromutth.r2dbc.mysql.collation;

import io.github.mirromutth.r2dbc.mysql.core.ServerVersion;
import reactor.util.annotation.Nullable;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * A constant utility that is contains all {@link CharCollation}s.
 */
final class CharCollations {

    private static final CharCollation UTF8MB4_GENERAL_CI = createCollation(45, "utf8mb4_general_ci", CharsetTargets.UTF_8_MB4);

    private static final CharCollation UTF8MB4_0900_AI_CI = createCollation(255, "utf8mb4_0900_ai_ci", CharsetTargets.UTF_8_MB4);

    private static final CharCollations INSTANCE = new CharCollations();

    static CharCollations getInstance() {
        return INSTANCE;
    }

    private final CharCollation[] universe;

    private CharCollations() {
        this(
            createCollation(11, "ascii_general_ci", CharsetTargets.US_ASCII),
            createCollation(65, "ascii_bin", CharsetTargets.US_ASCII),

            createCollation(5, "latin1_german1_ci", CharsetTargets.LATIN_1),
            createCollation(8, "latin1_swedish_ci", CharsetTargets.LATIN_1),
            createCollation(15, "latin1_danish_ci", CharsetTargets.LATIN_1),
            createCollation(31, "latin1_german2_ci", CharsetTargets.LATIN_1),
            createCollation(47, "latin1_bin", CharsetTargets.LATIN_1),
            createCollation(48, "latin1_general_ci", CharsetTargets.LATIN_1),
            createCollation(49, "latin1_general_cs", CharsetTargets.LATIN_1),
            createCollation(94, "latin1_spanish_ci", CharsetTargets.LATIN_1),

            // Latin 1 clusters? maybe wrong, should keep looking.
            createCollation(10, "swe7_swedish_ci", CharsetTargets.LATIN_1),
            createCollation(82, "swe7_bin", CharsetTargets.LATIN_1),
            createCollation(6, "hp8_english_ci", CharsetTargets.LATIN_1),
            createCollation(72, "hp8_bin", CharsetTargets.LATIN_1),
            createCollation(3, "dec8_swedish_ci", CharsetTargets.LATIN_1),
            createCollation(69, "dec8_bin", CharsetTargets.LATIN_1),
            createCollation(32, "armscii8_general_ci", CharsetTargets.LATIN_1),
            createCollation(64, "armscii8_bin", CharsetTargets.LATIN_1),
            createCollation(92, "geostd8_general_ci", CharsetTargets.LATIN_1),
            createCollation(93, "geostd8_bin", CharsetTargets.LATIN_1),

            createCollation(33, "utf8_general_ci", CharsetTargets.UTF_8),
            createCollation(76, "utf8_tolower_ci", CharsetTargets.UTF_8),
            createCollation(83, "utf8_bin", CharsetTargets.UTF_8),
            createCollation(192, "utf8_unicode_ci", CharsetTargets.UTF_8),
            createCollation(193, "utf8_icelandic_ci", CharsetTargets.UTF_8),
            createCollation(194, "utf8_latvian_ci", CharsetTargets.UTF_8),
            createCollation(195, "utf8_romanian_ci", CharsetTargets.UTF_8),
            createCollation(196, "utf8_slovenian_ci", CharsetTargets.UTF_8),
            createCollation(197, "utf8_polish_ci", CharsetTargets.UTF_8),
            createCollation(198, "utf8_estonian_ci", CharsetTargets.UTF_8),
            createCollation(199, "utf8_spanish_ci", CharsetTargets.UTF_8),
            createCollation(200, "utf8_swedish_ci", CharsetTargets.UTF_8),
            createCollation(201, "utf8_turkish_ci", CharsetTargets.UTF_8),
            createCollation(202, "utf8_czech_ci", CharsetTargets.UTF_8),
            createCollation(203, "utf8_danish_ci", CharsetTargets.UTF_8),
            createCollation(204, "utf8_lithuanian_ci", CharsetTargets.UTF_8),
            createCollation(205, "utf8_slovak_ci", CharsetTargets.UTF_8),
            createCollation(206, "utf8_spanish2_ci", CharsetTargets.UTF_8),
            createCollation(207, "utf8_roman_ci", CharsetTargets.UTF_8),
            createCollation(208, "utf8_persian_ci", CharsetTargets.UTF_8),
            createCollation(209, "utf8_esperanto_ci", CharsetTargets.UTF_8),
            createCollation(210, "utf8_hungarian_ci", CharsetTargets.UTF_8),
            createCollation(211, "utf8_sinhala_ci", CharsetTargets.UTF_8),
            createCollation(212, "utf8_german2_ci", CharsetTargets.UTF_8),
            createCollation(213, "utf8_croatian_ci", CharsetTargets.UTF_8),
            createCollation(214, "utf8_unicode_520_ci", CharsetTargets.UTF_8),
            createCollation(215, "utf8_vietnamese_ci", CharsetTargets.UTF_8),
            createCollation(223, "utf8_general_mysql500_ci", CharsetTargets.UTF_8),

            UTF8MB4_GENERAL_CI,
            createCollation(46, "utf8mb4_bin", CharsetTargets.UTF_8_MB4),
            createCollation(224, "utf8mb4_unicode_ci", CharsetTargets.UTF_8_MB4),
            createCollation(225, "utf8mb4_icelandic_ci", CharsetTargets.UTF_8_MB4),
            createCollation(226, "utf8mb4_latvian_ci", CharsetTargets.UTF_8_MB4),
            createCollation(227, "utf8mb4_romanian_ci", CharsetTargets.UTF_8_MB4),
            createCollation(228, "utf8mb4_slovenian_ci", CharsetTargets.UTF_8_MB4),
            createCollation(229, "utf8mb4_polish_ci", CharsetTargets.UTF_8_MB4),
            createCollation(230, "utf8mb4_estonian_ci", CharsetTargets.UTF_8_MB4),
            createCollation(231, "utf8mb4_spanish_ci", CharsetTargets.UTF_8_MB4),
            createCollation(232, "utf8mb4_swedish_ci", CharsetTargets.UTF_8_MB4),
            createCollation(233, "utf8mb4_turkish_ci", CharsetTargets.UTF_8_MB4),
            createCollation(234, "utf8mb4_czech_ci", CharsetTargets.UTF_8_MB4),
            createCollation(235, "utf8mb4_danish_ci", CharsetTargets.UTF_8_MB4),
            createCollation(236, "utf8mb4_lithuanian_ci", CharsetTargets.UTF_8_MB4),
            createCollation(237, "utf8mb4_slovak_ci", CharsetTargets.UTF_8_MB4),
            createCollation(238, "utf8mb4_spanish2_ci", CharsetTargets.UTF_8_MB4),
            createCollation(239, "utf8mb4_roman_ci", CharsetTargets.UTF_8_MB4),
            createCollation(240, "utf8mb4_persian_ci", CharsetTargets.UTF_8_MB4),
            createCollation(241, "utf8mb4_esperanto_ci", CharsetTargets.UTF_8_MB4),
            createCollation(242, "utf8mb4_hungarian_ci", CharsetTargets.UTF_8_MB4),
            createCollation(243, "utf8mb4_sinhala_ci", CharsetTargets.UTF_8_MB4),
            createCollation(244, "utf8mb4_german2_ci", CharsetTargets.UTF_8_MB4),
            createCollation(245, "utf8mb4_croatian_ci", CharsetTargets.UTF_8_MB4),
            createCollation(246, "utf8mb4_unicode_520_ci", CharsetTargets.UTF_8_MB4),
            createCollation(247, "utf8mb4_vietnamese_ci", CharsetTargets.UTF_8_MB4),
            UTF8MB4_0900_AI_CI,

            createCollation(54, "utf16_general_ci", CharsetTargets.UTF_16),
            createCollation(55, "utf16_bin", CharsetTargets.UTF_16),
            createCollation(101, "utf16_unicode_ci", CharsetTargets.UTF_16),
            createCollation(102, "utf16_icelandic_ci", CharsetTargets.UTF_16),
            createCollation(103, "utf16_latvian_ci", CharsetTargets.UTF_16),
            createCollation(104, "utf16_romanian_ci", CharsetTargets.UTF_16),
            createCollation(105, "utf16_slovenian_ci", CharsetTargets.UTF_16),
            createCollation(106, "utf16_polish_ci", CharsetTargets.UTF_16),
            createCollation(107, "utf16_estonian_ci", CharsetTargets.UTF_16),
            createCollation(108, "utf16_spanish_ci", CharsetTargets.UTF_16),
            createCollation(109, "utf16_swedish_ci", CharsetTargets.UTF_16),
            createCollation(110, "utf16_turkish_ci", CharsetTargets.UTF_16),
            createCollation(111, "utf16_czech_ci", CharsetTargets.UTF_16),
            createCollation(112, "utf16_danish_ci", CharsetTargets.UTF_16),
            createCollation(113, "utf16_lithuanian_ci", CharsetTargets.UTF_16),
            createCollation(114, "utf16_slovak_ci", CharsetTargets.UTF_16),
            createCollation(115, "utf16_spanish2_ci", CharsetTargets.UTF_16),
            createCollation(116, "utf16_roman_ci", CharsetTargets.UTF_16),
            createCollation(117, "utf16_persian_ci", CharsetTargets.UTF_16),
            createCollation(118, "utf16_esperanto_ci", CharsetTargets.UTF_16),
            createCollation(119, "utf16_hungarian_ci", CharsetTargets.UTF_16),
            createCollation(120, "utf16_sinhala_ci", CharsetTargets.UTF_16),
            createCollation(121, "utf16_german2_ci", CharsetTargets.UTF_16),
            createCollation(122, "utf16_croatian_ci", CharsetTargets.UTF_16),
            createCollation(123, "utf16_unicode_520_ci", CharsetTargets.UTF_16),
            createCollation(124, "utf16_vietnamese_ci", CharsetTargets.UTF_16),

            createCollation(56, "utf16le_general_ci", CharsetTargets.UTF_16LE),
            createCollation(62, "utf16le_bin", CharsetTargets.UTF_16LE),

            createCollation(60, "utf32_general_ci", CharsetTargets.UTF_32),
            createCollation(61, "utf32_bin", CharsetTargets.UTF_32),
            createCollation(160, "utf32_unicode_ci", CharsetTargets.UTF_32),
            createCollation(161, "utf32_icelandic_ci", CharsetTargets.UTF_32),
            createCollation(162, "utf32_latvian_ci", CharsetTargets.UTF_32),
            createCollation(163, "utf32_romanian_ci", CharsetTargets.UTF_32),
            createCollation(164, "utf32_slovenian_ci", CharsetTargets.UTF_32),
            createCollation(165, "utf32_polish_ci", CharsetTargets.UTF_32),
            createCollation(166, "utf32_estonian_ci", CharsetTargets.UTF_32),
            createCollation(167, "utf32_spanish_ci", CharsetTargets.UTF_32),
            createCollation(168, "utf32_swedish_ci", CharsetTargets.UTF_32),
            createCollation(169, "utf32_turkish_ci", CharsetTargets.UTF_32),
            createCollation(170, "utf32_czech_ci", CharsetTargets.UTF_32),
            createCollation(171, "utf32_danish_ci", CharsetTargets.UTF_32),
            createCollation(172, "utf32_lithuanian_ci", CharsetTargets.UTF_32),
            createCollation(173, "utf32_slovak_ci", CharsetTargets.UTF_32),
            createCollation(174, "utf32_spanish2_ci", CharsetTargets.UTF_32),
            createCollation(175, "utf32_roman_ci", CharsetTargets.UTF_32),
            createCollation(176, "utf32_persian_ci", CharsetTargets.UTF_32),
            createCollation(177, "utf32_esperanto_ci", CharsetTargets.UTF_32),
            createCollation(178, "utf32_hungarian_ci", CharsetTargets.UTF_32),
            createCollation(179, "utf32_sinhala_ci", CharsetTargets.UTF_32),
            createCollation(180, "utf32_german2_ci", CharsetTargets.UTF_32),
            createCollation(181, "utf32_croatian_ci", CharsetTargets.UTF_32),
            createCollation(182, "utf32_unicode_520_ci", CharsetTargets.UTF_32),
            createCollation(183, "utf32_vietnamese_ci", CharsetTargets.UTF_32),

            createCollation(28, "gbk_chinese_ci", CharsetTargets.GBK),
            createCollation(87, "gbk_bin", CharsetTargets.GBK)
        );
    }

    private CharCollations(CharCollation... universe) {
        this.universe = new CharCollation[universeSize(universe)];

        for (CharCollation collation : universe) {
            this.universe[collation.getId()] = collation;
        }
    }

    CharCollation defaultCollation(ServerVersion version) {
        requireNonNull(version, "version must not be null");

        if (version.compareTo(8, 0, 1) >= 0) {
            return UTF8MB4_0900_AI_CI;
        }

        return UTF8MB4_GENERAL_CI;
    }

    @Nullable
    CharCollation fromId(int id) {
        if (id < 0 || id >= this.universe.length) {
            return null;
        }

        return this.universe[id];
    }

    private static int universeSize(CharCollation[] universe) {
        int result = 0;

        for (CharCollation collation : universe) {
            int id = collation.getId();
            if (id > result) {
                result = id;
            }
        }

        if (result > 0) {
            // Ignore index 0 for ease of use, otherwise need to subtract 1 each time
            return result + 1;
        }

        return 0;
    }

    private static CharCollation createCollation(int id, String name, CharsetTarget target) {
        if (target.isCached()) {
            return new CachedCharCollation(id, name, target);
        } else {
            return new LazyInitCharCollation(id, name, target);
        }
    }
}
