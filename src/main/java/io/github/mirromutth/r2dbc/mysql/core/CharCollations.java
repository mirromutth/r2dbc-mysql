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
import java.util.Arrays;

/**
 * Character collation helper utility.
 */
final class CharCollations {

    private static final int MULTI_BYTES_FALLBACK = 33;

    private static final CharCollations INSTANCE = new CharCollations();

    static CharCollations getInstance() {
        return INSTANCE;
    }

    private final CharCollation[] universe;

    private CharCollations() {
        this(
            new CharCollation(33, "utf8_general_ci", StandardCharsets.UTF_8),
            new CharCollation(45, "utf8mb4_general_ci", StandardCharsets.UTF_8),
            new CharCollation(46, "utf8mb4_bin", StandardCharsets.UTF_8),
            new CharCollation(76, "utf8_tolower_ci", StandardCharsets.UTF_8),
            new CharCollation(83, "utf8_bin", StandardCharsets.UTF_8),

            new CharCollation(192, "utf8_unicode_ci", StandardCharsets.UTF_8),
            new CharCollation(193, "utf8_icelandic_ci", StandardCharsets.UTF_8),
            new CharCollation(194, "utf8_latvian_ci", StandardCharsets.UTF_8),
            new CharCollation(195, "utf8_romanian_ci", StandardCharsets.UTF_8),
            new CharCollation(196, "utf8_slovenian_ci", StandardCharsets.UTF_8),
            new CharCollation(197, "utf8_polish_ci", StandardCharsets.UTF_8),
            new CharCollation(198, "utf8_estonian_ci", StandardCharsets.UTF_8),
            new CharCollation(199, "utf8_spanish_ci", StandardCharsets.UTF_8),
            new CharCollation(200, "utf8_swedish_ci", StandardCharsets.UTF_8),
            new CharCollation(201, "utf8_turkish_ci", StandardCharsets.UTF_8),
            new CharCollation(202, "utf8_czech_ci", StandardCharsets.UTF_8),
            new CharCollation(203, "utf8_danish_ci", StandardCharsets.UTF_8),
            new CharCollation(204, "utf8_lithuanian_ci", StandardCharsets.UTF_8),
            new CharCollation(205, "utf8_slovak_ci", StandardCharsets.UTF_8),
            new CharCollation(206, "utf8_spanish2_ci", StandardCharsets.UTF_8),
            new CharCollation(207, "utf8_roman_ci", StandardCharsets.UTF_8),
            new CharCollation(208, "utf8_persian_ci", StandardCharsets.UTF_8),
            new CharCollation(209, "utf8_esperanto_ci", StandardCharsets.UTF_8),
            new CharCollation(210, "utf8_hungarian_ci", StandardCharsets.UTF_8),
            new CharCollation(211, "utf8_sinhala_ci", StandardCharsets.UTF_8),
            new CharCollation(212, "utf8_german2_ci", StandardCharsets.UTF_8),
            new CharCollation(213, "utf8_croatian_ci", StandardCharsets.UTF_8),
            new CharCollation(214, "utf8_unicode_520_ci", StandardCharsets.UTF_8),
            new CharCollation(215, "utf8_vietnamese_ci", StandardCharsets.UTF_8),

            new CharCollation(223, "utf8_general_mysql500_ci", StandardCharsets.UTF_8),
            new CharCollation(224, "utf8mb4_unicode_ci", StandardCharsets.UTF_8),
            new CharCollation(225, "utf8mb4_icelandic_ci", StandardCharsets.UTF_8),
            new CharCollation(226, "utf8mb4_latvian_ci", StandardCharsets.UTF_8),
            new CharCollation(227, "utf8mb4_romanian_ci", StandardCharsets.UTF_8),
            new CharCollation(228, "utf8mb4_slovenian_ci", StandardCharsets.UTF_8),
            new CharCollation(229, "utf8mb4_polish_ci", StandardCharsets.UTF_8),
            new CharCollation(230, "utf8mb4_estonian_ci", StandardCharsets.UTF_8),
            new CharCollation(231, "utf8mb4_spanish_ci", StandardCharsets.UTF_8),
            new CharCollation(232, "utf8mb4_swedish_ci", StandardCharsets.UTF_8),
            new CharCollation(233, "utf8mb4_turkish_ci", StandardCharsets.UTF_8),
            new CharCollation(234, "utf8mb4_czech_ci", StandardCharsets.UTF_8),
            new CharCollation(235, "utf8mb4_danish_ci", StandardCharsets.UTF_8),
            new CharCollation(236, "utf8mb4_lithuanian_ci", StandardCharsets.UTF_8),
            new CharCollation(237, "utf8mb4_slovak_ci", StandardCharsets.UTF_8),
            new CharCollation(238, "utf8mb4_spanish2_ci", StandardCharsets.UTF_8),
            new CharCollation(239, "utf8mb4_roman_ci", StandardCharsets.UTF_8),
            new CharCollation(240, "utf8mb4_persian_ci", StandardCharsets.UTF_8),
            new CharCollation(241, "utf8mb4_esperanto_ci", StandardCharsets.UTF_8),
            new CharCollation(242, "utf8mb4_hungarian_ci", StandardCharsets.UTF_8),
            new CharCollation(243, "utf8mb4_sinhala_ci", StandardCharsets.UTF_8),
            new CharCollation(244, "utf8mb4_german2_ci", StandardCharsets.UTF_8),
            new CharCollation(245, "utf8mb4_croatian_ci", StandardCharsets.UTF_8),
            new CharCollation(246, "utf8mb4_unicode_520_ci", StandardCharsets.UTF_8),
            new CharCollation(247, "utf8mb4_vietnamese_ci", StandardCharsets.UTF_8),

            new CharCollation(255, "utf8mb4_0900_ai_ci", StandardCharsets.UTF_8)
        );
    }

    private CharCollations(CharCollation... universe) {
        int length = Arrays.stream(universe).mapToInt(CharCollation::getId).max().orElse(0);

        this.universe = new CharCollation[length];

        for (CharCollation collation : universe) {
            this.universe[collation.getId() - 1] = collation;
        }

        if (this.universe[MULTI_BYTES_FALLBACK] == null) {
            throw new IllegalStateException("have no fallback for multi-bytes encoding");
        }
    }

    CharCollation fromId(int id) {
        if (id < 0 || id >= this.universe.length) {
            return this.universe[MULTI_BYTES_FALLBACK];
        }

        return this.universe[id];
    }
}
