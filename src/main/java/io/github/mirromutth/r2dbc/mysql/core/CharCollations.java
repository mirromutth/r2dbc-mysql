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

import java.util.Arrays;

/**
 * A constant utility that is contains all {@link CharCollation}s.
 */
final class CharCollations {

    private static final int MULTI_BYTES_FALLBACK = 33; // utf8_general_ci is fallback collation of multi-bytes charset

    private static final CharCollations INSTANCE = new CharCollations();

    static CharCollations getInstance() {
        return INSTANCE;
    }

    private final CharCollation[] universe;

    private CharCollations() {
        this(
            new CharCollation(11, "ascii_general_ci", CharsetTargets.US_ASCII),
            new CharCollation(65, "ascii_bin", CharsetTargets.US_ASCII),

            new CharCollation(5, "latin1_german1_ci", CharsetTargets.LATIN_1),
            new CharCollation(8, "latin1_swedish_ci", CharsetTargets.LATIN_1),
            new CharCollation(15, "latin1_danish_ci", CharsetTargets.LATIN_1),
            new CharCollation(31, "latin1_german2_ci", CharsetTargets.LATIN_1),
            new CharCollation(47, "latin1_bin", CharsetTargets.LATIN_1),
            new CharCollation(48, "latin1_general_ci", CharsetTargets.LATIN_1),
            new CharCollation(49, "latin1_general_cs", CharsetTargets.LATIN_1),
            new CharCollation(94, "latin1_spanish_ci", CharsetTargets.LATIN_1),

            // Latin 1 clusters? maybe wrong, should keep looking.
            new CharCollation(10, "swe7_swedish_ci", CharsetTargets.LATIN_1),
            new CharCollation(82, "swe7_bin", CharsetTargets.LATIN_1),
            new CharCollation(6, "hp8_english_ci", CharsetTargets.LATIN_1),
            new CharCollation(72, "hp8_bin", CharsetTargets.LATIN_1),
            new CharCollation(3, "dec8_swedish_ci", CharsetTargets.LATIN_1),
            new CharCollation(69, "dec8_bin", CharsetTargets.LATIN_1),
            new CharCollation(32, "armscii8_general_ci", CharsetTargets.LATIN_1),
            new CharCollation(64, "armscii8_bin", CharsetTargets.LATIN_1),
            new CharCollation(92, "geostd8_general_ci", CharsetTargets.LATIN_1),
            new CharCollation(93, "geostd8_bin", CharsetTargets.LATIN_1),

            new CharCollation(33, "utf8_general_ci", CharsetTargets.UTF_8),
            new CharCollation(76, "utf8_tolower_ci", CharsetTargets.UTF_8),
            new CharCollation(83, "utf8_bin", CharsetTargets.UTF_8),
            new CharCollation(192, "utf8_unicode_ci", CharsetTargets.UTF_8),
            new CharCollation(193, "utf8_icelandic_ci", CharsetTargets.UTF_8),
            new CharCollation(194, "utf8_latvian_ci", CharsetTargets.UTF_8),
            new CharCollation(195, "utf8_romanian_ci", CharsetTargets.UTF_8),
            new CharCollation(196, "utf8_slovenian_ci", CharsetTargets.UTF_8),
            new CharCollation(197, "utf8_polish_ci", CharsetTargets.UTF_8),
            new CharCollation(198, "utf8_estonian_ci", CharsetTargets.UTF_8),
            new CharCollation(199, "utf8_spanish_ci", CharsetTargets.UTF_8),
            new CharCollation(200, "utf8_swedish_ci", CharsetTargets.UTF_8),
            new CharCollation(201, "utf8_turkish_ci", CharsetTargets.UTF_8),
            new CharCollation(202, "utf8_czech_ci", CharsetTargets.UTF_8),
            new CharCollation(203, "utf8_danish_ci", CharsetTargets.UTF_8),
            new CharCollation(204, "utf8_lithuanian_ci", CharsetTargets.UTF_8),
            new CharCollation(205, "utf8_slovak_ci", CharsetTargets.UTF_8),
            new CharCollation(206, "utf8_spanish2_ci", CharsetTargets.UTF_8),
            new CharCollation(207, "utf8_roman_ci", CharsetTargets.UTF_8),
            new CharCollation(208, "utf8_persian_ci", CharsetTargets.UTF_8),
            new CharCollation(209, "utf8_esperanto_ci", CharsetTargets.UTF_8),
            new CharCollation(210, "utf8_hungarian_ci", CharsetTargets.UTF_8),
            new CharCollation(211, "utf8_sinhala_ci", CharsetTargets.UTF_8),
            new CharCollation(212, "utf8_german2_ci", CharsetTargets.UTF_8),
            new CharCollation(213, "utf8_croatian_ci", CharsetTargets.UTF_8),
            new CharCollation(214, "utf8_unicode_520_ci", CharsetTargets.UTF_8),
            new CharCollation(215, "utf8_vietnamese_ci", CharsetTargets.UTF_8),
            new CharCollation(223, "utf8_general_mysql500_ci", CharsetTargets.UTF_8),

            new CharCollation(45, "utf8mb4_general_ci", CharsetTargets.UTF_8_MB4),
            new CharCollation(46, "utf8mb4_bin", CharsetTargets.UTF_8_MB4),
            new CharCollation(224, "utf8mb4_unicode_ci", CharsetTargets.UTF_8_MB4),
            new CharCollation(225, "utf8mb4_icelandic_ci", CharsetTargets.UTF_8_MB4),
            new CharCollation(226, "utf8mb4_latvian_ci", CharsetTargets.UTF_8_MB4),
            new CharCollation(227, "utf8mb4_romanian_ci", CharsetTargets.UTF_8_MB4),
            new CharCollation(228, "utf8mb4_slovenian_ci", CharsetTargets.UTF_8_MB4),
            new CharCollation(229, "utf8mb4_polish_ci", CharsetTargets.UTF_8_MB4),
            new CharCollation(230, "utf8mb4_estonian_ci", CharsetTargets.UTF_8_MB4),
            new CharCollation(231, "utf8mb4_spanish_ci", CharsetTargets.UTF_8_MB4),
            new CharCollation(232, "utf8mb4_swedish_ci", CharsetTargets.UTF_8_MB4),
            new CharCollation(233, "utf8mb4_turkish_ci", CharsetTargets.UTF_8_MB4),
            new CharCollation(234, "utf8mb4_czech_ci", CharsetTargets.UTF_8_MB4),
            new CharCollation(235, "utf8mb4_danish_ci", CharsetTargets.UTF_8_MB4),
            new CharCollation(236, "utf8mb4_lithuanian_ci", CharsetTargets.UTF_8_MB4),
            new CharCollation(237, "utf8mb4_slovak_ci", CharsetTargets.UTF_8_MB4),
            new CharCollation(238, "utf8mb4_spanish2_ci", CharsetTargets.UTF_8_MB4),
            new CharCollation(239, "utf8mb4_roman_ci", CharsetTargets.UTF_8_MB4),
            new CharCollation(240, "utf8mb4_persian_ci", CharsetTargets.UTF_8_MB4),
            new CharCollation(241, "utf8mb4_esperanto_ci", CharsetTargets.UTF_8_MB4),
            new CharCollation(242, "utf8mb4_hungarian_ci", CharsetTargets.UTF_8_MB4),
            new CharCollation(243, "utf8mb4_sinhala_ci", CharsetTargets.UTF_8_MB4),
            new CharCollation(244, "utf8mb4_german2_ci", CharsetTargets.UTF_8_MB4),
            new CharCollation(245, "utf8mb4_croatian_ci", CharsetTargets.UTF_8_MB4),
            new CharCollation(246, "utf8mb4_unicode_520_ci", CharsetTargets.UTF_8_MB4),
            new CharCollation(247, "utf8mb4_vietnamese_ci", CharsetTargets.UTF_8_MB4),
            new CharCollation(255, "utf8mb4_0900_ai_ci", CharsetTargets.UTF_8_MB4),

            new CharCollation(54, "utf16_general_ci", CharsetTargets.UTF_16),
            new CharCollation(55, "utf16_bin", CharsetTargets.UTF_16),
            new CharCollation(101, "utf16_unicode_ci", CharsetTargets.UTF_16),
            new CharCollation(102, "utf16_icelandic_ci", CharsetTargets.UTF_16),
            new CharCollation(103, "utf16_latvian_ci", CharsetTargets.UTF_16),
            new CharCollation(104, "utf16_romanian_ci", CharsetTargets.UTF_16),
            new CharCollation(105, "utf16_slovenian_ci", CharsetTargets.UTF_16),
            new CharCollation(106, "utf16_polish_ci", CharsetTargets.UTF_16),
            new CharCollation(107, "utf16_estonian_ci", CharsetTargets.UTF_16),
            new CharCollation(108, "utf16_spanish_ci", CharsetTargets.UTF_16),
            new CharCollation(109, "utf16_swedish_ci", CharsetTargets.UTF_16),
            new CharCollation(110, "utf16_turkish_ci", CharsetTargets.UTF_16),
            new CharCollation(111, "utf16_czech_ci", CharsetTargets.UTF_16),
            new CharCollation(112, "utf16_danish_ci", CharsetTargets.UTF_16),
            new CharCollation(113, "utf16_lithuanian_ci", CharsetTargets.UTF_16),
            new CharCollation(114, "utf16_slovak_ci", CharsetTargets.UTF_16),
            new CharCollation(115, "utf16_spanish2_ci", CharsetTargets.UTF_16),
            new CharCollation(116, "utf16_roman_ci", CharsetTargets.UTF_16),
            new CharCollation(117, "utf16_persian_ci", CharsetTargets.UTF_16),
            new CharCollation(118, "utf16_esperanto_ci", CharsetTargets.UTF_16),
            new CharCollation(119, "utf16_hungarian_ci", CharsetTargets.UTF_16),
            new CharCollation(120, "utf16_sinhala_ci", CharsetTargets.UTF_16),
            new CharCollation(121, "utf16_german2_ci", CharsetTargets.UTF_16),
            new CharCollation(122, "utf16_croatian_ci", CharsetTargets.UTF_16),
            new CharCollation(123, "utf16_unicode_520_ci", CharsetTargets.UTF_16),
            new CharCollation(124, "utf16_vietnamese_ci", CharsetTargets.UTF_16),

            new CharCollation(56, "utf16le_general_ci", CharsetTargets.UTF_16LE),
            new CharCollation(62, "utf16le_bin", CharsetTargets.UTF_16LE),

            new CharCollation(60, "utf32_general_ci", CharsetTargets.UTF_32),
            new CharCollation(61, "utf32_bin", CharsetTargets.UTF_32),
            new CharCollation(160, "utf32_unicode_ci", CharsetTargets.UTF_32),
            new CharCollation(161, "utf32_icelandic_ci", CharsetTargets.UTF_32),
            new CharCollation(162, "utf32_latvian_ci", CharsetTargets.UTF_32),
            new CharCollation(163, "utf32_romanian_ci", CharsetTargets.UTF_32),
            new CharCollation(164, "utf32_slovenian_ci", CharsetTargets.UTF_32),
            new CharCollation(165, "utf32_polish_ci", CharsetTargets.UTF_32),
            new CharCollation(166, "utf32_estonian_ci", CharsetTargets.UTF_32),
            new CharCollation(167, "utf32_spanish_ci", CharsetTargets.UTF_32),
            new CharCollation(168, "utf32_swedish_ci", CharsetTargets.UTF_32),
            new CharCollation(169, "utf32_turkish_ci", CharsetTargets.UTF_32),
            new CharCollation(170, "utf32_czech_ci", CharsetTargets.UTF_32),
            new CharCollation(171, "utf32_danish_ci", CharsetTargets.UTF_32),
            new CharCollation(172, "utf32_lithuanian_ci", CharsetTargets.UTF_32),
            new CharCollation(173, "utf32_slovak_ci", CharsetTargets.UTF_32),
            new CharCollation(174, "utf32_spanish2_ci", CharsetTargets.UTF_32),
            new CharCollation(175, "utf32_roman_ci", CharsetTargets.UTF_32),
            new CharCollation(176, "utf32_persian_ci", CharsetTargets.UTF_32),
            new CharCollation(177, "utf32_esperanto_ci", CharsetTargets.UTF_32),
            new CharCollation(178, "utf32_hungarian_ci", CharsetTargets.UTF_32),
            new CharCollation(179, "utf32_sinhala_ci", CharsetTargets.UTF_32),
            new CharCollation(180, "utf32_german2_ci", CharsetTargets.UTF_32),
            new CharCollation(181, "utf32_croatian_ci", CharsetTargets.UTF_32),
            new CharCollation(182, "utf32_unicode_520_ci", CharsetTargets.UTF_32),
            new CharCollation(183, "utf32_vietnamese_ci", CharsetTargets.UTF_32),

            new CharCollation(28, "gbk_chinese_ci", CharsetTargets.GBK),
            new CharCollation(87, "gbk_bin", CharsetTargets.GBK)
        );
    }

    private CharCollations(CharCollation... universe) {
        this.universe = new CharCollation[universeSize(universe)];

        for (CharCollation collation : universe) {
            this.universe[collation.getId()] = collation;
        }

        CharCollation multiFallback = this.universe[MULTI_BYTES_FALLBACK];

        if (multiFallback == null || multiFallback.getByteSize() <= 1) {
            throw new IllegalStateException("have no fallback for multi-bytes encoding");
        }
    }

    CharCollation fromId(int id) {
        if (id < 0 || id >= this.universe.length) {
            return this.universe[MULTI_BYTES_FALLBACK];
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
}
