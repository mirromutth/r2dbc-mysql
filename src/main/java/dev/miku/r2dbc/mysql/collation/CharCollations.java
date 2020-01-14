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

import dev.miku.r2dbc.mysql.util.ServerVersion;

/**
 * A constant utility that is contains all {@link CharCollation}s.
 */
final class CharCollations {

    static final CharCollation BINARY;

    static final CharCollation UTF8MB4_GENERAL_CI;

    private static final CharCollation LATIN1_GENERAL_CI;

    private static final CharCollation UTF8MB4_0900_AI_CI;

    private static final CharCollation ASCII_GENERAL_CI;

    private static final CharCollation[] COSMOS;

    /**
     * The universe extra character collations, they have special IDs
     * which maybe causing unnecessary memory waste.
     * <p>
     * Note: all IDs must bigger than the max ID of universe.
     *
     * @see #fromId(int, ServerVersion)
     */
    private static final CharCollation[] EXTRA;

    private static final ServerVersion UTF8MB4_0900_VER = ServerVersion.create(8, 0, 1);

    static {
        // The initialization of character collation constants is the premise of the universe big bang.
        ASCII_GENERAL_CI = createCollation(11, "ascii_general_ci", CharsetTargets.ASCII);
        UTF8MB4_GENERAL_CI = createCollation(45, "utf8mb4_general_ci", CharsetTargets.UTF8MB4);
        LATIN1_GENERAL_CI = createCollation(48, "latin1_general_ci", CharsetTargets.LATIN1);
        BINARY = createCollation(63, "binary", BinaryTarget.INSTANCE);
        UTF8MB4_0900_AI_CI = createCollation(255, "utf8mb4_0900_ai_ci", CharsetTargets.UTF8MB4);

        CharCollation[] universe = universeBigBang();
        COSMOS = new CharCollation[cosmosSize(universe)];
        EXTRA = universeExtra();

        for (CharCollation collation : universe) {
            COSMOS[collation.getId()] = collation;
        }
    }

    private CharCollations() {
    }

    static CharCollation fromId(int id, ServerVersion version) {
        if (id < 0 || id >= COSMOS.length) {
            for (CharCollation collation : EXTRA) {
                if (collation.getId() == id) {
                    return collation;
                }
            }

            return defaultServerCollation(version);
        }

        CharCollation collation = COSMOS[id];

        if (collation == null) {
            return defaultServerCollation(version);
        }

        return collation;
    }

    private static CharCollation defaultServerCollation(ServerVersion version) {
        if (version.isGreaterThanOrEqualTo(UTF8MB4_0900_VER)) {
            return UTF8MB4_0900_AI_CI;
        }

        return UTF8MB4_GENERAL_CI;
    }

    private static int cosmosSize(CharCollation[] universe) {
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

    private static CharCollation[] universeBigBang() {
        return new CharCollation[]{
            createCollation(1, "big5_chinese_ci", CharsetTargets.BIG5),
            createCollation(2, "latin2_czech_cs", CharsetTargets.LATIN2),
            createCollation(3, "dec8_swedish_ci", CharsetTargets.DEC8),
            createCollation(4, "cp850_general_ci", CharsetTargets.CP850),
            createCollation(5, "latin1_german1_ci", CharsetTargets.LATIN1),
            createCollation(6, "hp8_english_ci", CharsetTargets.HP8),
            createCollation(7, "koi8r_general_ci", CharsetTargets.KOI8_R),
            createCollation(8, "latin1_swedish_ci", CharsetTargets.LATIN1),
            createCollation(9, "latin2_general_ci", CharsetTargets.LATIN2),
            createCollation(10, "swe7_swedish_ci", CharsetTargets.SWE7),
            ASCII_GENERAL_CI,
            createCollation(12, "ujis_japanese_ci", CharsetTargets.EUC_JIS),
            createCollation(13, "sjis_japanese_ci", CharsetTargets.SHIFT_JIS),
            createCollation(14, "cp1251_bulgarian_ci", CharsetTargets.CP1251),
            createCollation(15, "latin1_danish_ci", CharsetTargets.LATIN1),
            createCollation(16, "hebrew_general_ci", CharsetTargets.HEBREW),
            createCollation(18, "tis620_thai_ci", CharsetTargets.TIS620),
            createCollation(19, "euckr_korean_ci", CharsetTargets.EUC_KR),
            createCollation(20, "latin7_estonian_cs", CharsetTargets.LATIN7),
            createCollation(21, "latin2_hungarian_ci", CharsetTargets.LATIN2),
            createCollation(22, "koi8u_general_ci", CharsetTargets.KOI8_U),
            createCollation(23, "cp1251_ukrainian_ci", CharsetTargets.CP1251),
            createCollation(24, "gb2312_chinese_ci", CharsetTargets.GB2312),
            createCollation(25, "greek_general_ci", CharsetTargets.GREEK),
            createCollation(26, "cp1250_general_ci", CharsetTargets.CP1250),
            createCollation(27, "latin2_croatian_ci", CharsetTargets.LATIN2),
            createCollation(28, "gbk_chinese_ci", CharsetTargets.GBK),
            createCollation(29, "cp1257_lithuanian_ci", CharsetTargets.CP1257),
            createCollation(30, "latin5_turkish_ci", CharsetTargets.LATIN5),
            createCollation(31, "latin1_german2_ci", CharsetTargets.LATIN1),
            createCollation(32, "armscii8_general_ci", CharsetTargets.ARMSCII8),
            createCollation(33, "utf8_general_ci", CharsetTargets.UTF8),
            createCollation(34, "cp1250_czech_cs", CharsetTargets.CP1250),
            createCollation(35, "ucs2_general_ci", CharsetTargets.UCS2),
            createCollation(36, "cp866_general_ci", CharsetTargets.CP866),
            createCollation(37, "keybcs2_general_ci", CharsetTargets.KEYBCS2),
            createCollation(38, "macce_general_ci", CharsetTargets.MAC_CE),
            createCollation(39, "macroman_general_ci", CharsetTargets.MAC_ROMAN),
            createCollation(40, "cp852_general_ci", CharsetTargets.CP852),
            createCollation(41, "latin7_general_ci", CharsetTargets.LATIN7),
            createCollation(42, "latin7_general_cs", CharsetTargets.LATIN7),
            createCollation(43, "macce_bin", CharsetTargets.MAC_CE),
            createCollation(44, "cp1250_croatian_ci", CharsetTargets.CP1250),
            UTF8MB4_GENERAL_CI,
            createCollation(46, "utf8mb4_bin", CharsetTargets.UTF8MB4),
            createCollation(47, "latin1_bin", CharsetTargets.LATIN1),
            LATIN1_GENERAL_CI,
            createCollation(49, "latin1_general_cs", CharsetTargets.LATIN1),
            createCollation(50, "cp1251_bin", CharsetTargets.CP1251),
            createCollation(51, "cp1251_general_ci", CharsetTargets.CP1251),
            createCollation(52, "cp1251_general_cs", CharsetTargets.CP1251),
            createCollation(53, "macroman_bin", CharsetTargets.MAC_ROMAN),
            createCollation(54, "utf16_general_ci", CharsetTargets.UTF16),
            createCollation(55, "utf16_bin", CharsetTargets.UTF16),
            createCollation(56, "utf16le_general_ci", CharsetTargets.UTF16LE),
            createCollation(57, "cp1256_general_ci", CharsetTargets.CP1256),
            createCollation(58, "cp1257_bin", CharsetTargets.CP1257),
            createCollation(59, "cp1257_general_ci", CharsetTargets.CP1257),
            createCollation(60, "utf32_general_ci", CharsetTargets.UTF32),
            createCollation(61, "utf32_bin", CharsetTargets.UTF32),
            createCollation(62, "utf16le_bin", CharsetTargets.UTF16LE),
            BINARY,
            createCollation(64, "armscii8_bin", CharsetTargets.ARMSCII8),
            createCollation(65, "ascii_bin", CharsetTargets.ASCII),
            createCollation(66, "cp1250_bin", CharsetTargets.CP1250),
            createCollation(67, "cp1256_bin", CharsetTargets.CP1256),
            createCollation(68, "cp866_bin", CharsetTargets.CP866),
            createCollation(69, "dec8_bin", CharsetTargets.DEC8),
            createCollation(70, "greek_bin", CharsetTargets.GREEK),
            createCollation(71, "hebrew_bin", CharsetTargets.HEBREW),
            createCollation(72, "hp8_bin", CharsetTargets.HP8),
            createCollation(73, "keybcs2_bin", CharsetTargets.KEYBCS2),
            createCollation(74, "koi8r_bin", CharsetTargets.KOI8_R),
            createCollation(75, "koi8u_bin", CharsetTargets.KOI8_U),
            createCollation(76, "utf8_tolower_ci", CharsetTargets.UTF8),
            createCollation(77, "latin2_bin", CharsetTargets.LATIN2),
            createCollation(78, "latin5_bin", CharsetTargets.LATIN5),
            createCollation(79, "latin7_bin", CharsetTargets.LATIN7),
            createCollation(80, "cp850_bin", CharsetTargets.CP850),
            createCollation(81, "cp852_bin", CharsetTargets.CP852),
            createCollation(82, "swe7_bin", CharsetTargets.SWE7),
            createCollation(83, "utf8_bin", CharsetTargets.UTF8),
            createCollation(84, "big5_bin", CharsetTargets.BIG5),
            createCollation(85, "euckr_bin", CharsetTargets.EUC_KR),
            createCollation(86, "gb2312_bin", CharsetTargets.GB2312),
            createCollation(87, "gbk_bin", CharsetTargets.GBK),
            createCollation(88, "sjis_bin", CharsetTargets.SHIFT_JIS),
            createCollation(89, "tis620_bin", CharsetTargets.TIS620),
            createCollation(90, "ucs2_bin", CharsetTargets.UCS2),
            createCollation(91, "ujis_bin", CharsetTargets.EUC_JIS),
            createCollation(92, "geostd8_general_ci", CharsetTargets.GEOSTD8),
            createCollation(93, "geostd8_bin", CharsetTargets.GEOSTD8),
            createCollation(94, "latin1_spanish_ci", CharsetTargets.LATIN1),
            createCollation(95, "cp932_japanese_ci", CharsetTargets.CP932),
            createCollation(96, "cp932_bin", CharsetTargets.CP932),
            createCollation(97, "eucjpms_japanese_ci", CharsetTargets.EUC_JPMS),
            createCollation(98, "eucjpms_bin", CharsetTargets.EUC_JPMS),
            createCollation(99, "cp1250_polish_ci", CharsetTargets.CP1250),
            createCollation(101, "utf16_unicode_ci", CharsetTargets.UTF16),
            createCollation(102, "utf16_icelandic_ci", CharsetTargets.UTF16),
            createCollation(103, "utf16_latvian_ci", CharsetTargets.UTF16),
            createCollation(104, "utf16_romanian_ci", CharsetTargets.UTF16),
            createCollation(105, "utf16_slovenian_ci", CharsetTargets.UTF16),
            createCollation(106, "utf16_polish_ci", CharsetTargets.UTF16),
            createCollation(107, "utf16_estonian_ci", CharsetTargets.UTF16),
            createCollation(108, "utf16_spanish_ci", CharsetTargets.UTF16),
            createCollation(109, "utf16_swedish_ci", CharsetTargets.UTF16),
            createCollation(110, "utf16_turkish_ci", CharsetTargets.UTF16),
            createCollation(111, "utf16_czech_ci", CharsetTargets.UTF16),
            createCollation(112, "utf16_danish_ci", CharsetTargets.UTF16),
            createCollation(113, "utf16_lithuanian_ci", CharsetTargets.UTF16),
            createCollation(114, "utf16_slovak_ci", CharsetTargets.UTF16),
            createCollation(115, "utf16_spanish2_ci", CharsetTargets.UTF16),
            createCollation(116, "utf16_roman_ci", CharsetTargets.UTF16),
            createCollation(117, "utf16_persian_ci", CharsetTargets.UTF16),
            createCollation(118, "utf16_esperanto_ci", CharsetTargets.UTF16),
            createCollation(119, "utf16_hungarian_ci", CharsetTargets.UTF16),
            createCollation(120, "utf16_sinhala_ci", CharsetTargets.UTF16),
            createCollation(121, "utf16_german2_ci", CharsetTargets.UTF16),
            createCollation(122, "utf16_croatian_ci", CharsetTargets.UTF16),
            createCollation(123, "utf16_unicode_520_ci", CharsetTargets.UTF16),
            createCollation(124, "utf16_vietnamese_ci", CharsetTargets.UTF16),
            createCollation(128, "ucs2_unicode_ci", CharsetTargets.UCS2),
            createCollation(129, "ucs2_icelandic_ci", CharsetTargets.UCS2),
            createCollation(130, "ucs2_latvian_ci", CharsetTargets.UCS2),
            createCollation(131, "ucs2_romanian_ci", CharsetTargets.UCS2),
            createCollation(132, "ucs2_slovenian_ci", CharsetTargets.UCS2),
            createCollation(133, "ucs2_polish_ci", CharsetTargets.UCS2),
            createCollation(134, "ucs2_estonian_ci", CharsetTargets.UCS2),
            createCollation(135, "ucs2_spanish_ci", CharsetTargets.UCS2),
            createCollation(136, "ucs2_swedish_ci", CharsetTargets.UCS2),
            createCollation(137, "ucs2_turkish_ci", CharsetTargets.UCS2),
            createCollation(138, "ucs2_czech_ci", CharsetTargets.UCS2),
            createCollation(139, "ucs2_danish_ci", CharsetTargets.UCS2),
            createCollation(140, "ucs2_lithuanian_ci", CharsetTargets.UCS2),
            createCollation(141, "ucs2_slovak_ci", CharsetTargets.UCS2),
            createCollation(142, "ucs2_spanish2_ci", CharsetTargets.UCS2),
            createCollation(143, "ucs2_roman_ci", CharsetTargets.UCS2),
            createCollation(144, "ucs2_persian_ci", CharsetTargets.UCS2),
            createCollation(145, "ucs2_esperanto_ci", CharsetTargets.UCS2),
            createCollation(146, "ucs2_hungarian_ci", CharsetTargets.UCS2),
            createCollation(147, "ucs2_sinhala_ci", CharsetTargets.UCS2),
            createCollation(148, "ucs2_german2_ci", CharsetTargets.UCS2),
            createCollation(149, "ucs2_croatian_ci", CharsetTargets.UCS2),
            createCollation(150, "ucs2_unicode_520_ci", CharsetTargets.UCS2),
            createCollation(151, "ucs2_vietnamese_ci", CharsetTargets.UCS2),
            createCollation(159, "ucs2_general_mysql500_ci", CharsetTargets.UCS2),
            createCollation(160, "utf32_unicode_ci", CharsetTargets.UTF32),
            createCollation(161, "utf32_icelandic_ci", CharsetTargets.UTF32),
            createCollation(162, "utf32_latvian_ci", CharsetTargets.UTF32),
            createCollation(163, "utf32_romanian_ci", CharsetTargets.UTF32),
            createCollation(164, "utf32_slovenian_ci", CharsetTargets.UTF32),
            createCollation(165, "utf32_polish_ci", CharsetTargets.UTF32),
            createCollation(166, "utf32_estonian_ci", CharsetTargets.UTF32),
            createCollation(167, "utf32_spanish_ci", CharsetTargets.UTF32),
            createCollation(168, "utf32_swedish_ci", CharsetTargets.UTF32),
            createCollation(169, "utf32_turkish_ci", CharsetTargets.UTF32),
            createCollation(170, "utf32_czech_ci", CharsetTargets.UTF32),
            createCollation(171, "utf32_danish_ci", CharsetTargets.UTF32),
            createCollation(172, "utf32_lithuanian_ci", CharsetTargets.UTF32),
            createCollation(173, "utf32_slovak_ci", CharsetTargets.UTF32),
            createCollation(174, "utf32_spanish2_ci", CharsetTargets.UTF32),
            createCollation(175, "utf32_roman_ci", CharsetTargets.UTF32),
            createCollation(176, "utf32_persian_ci", CharsetTargets.UTF32),
            createCollation(177, "utf32_esperanto_ci", CharsetTargets.UTF32),
            createCollation(178, "utf32_hungarian_ci", CharsetTargets.UTF32),
            createCollation(179, "utf32_sinhala_ci", CharsetTargets.UTF32),
            createCollation(180, "utf32_german2_ci", CharsetTargets.UTF32),
            createCollation(181, "utf32_croatian_ci", CharsetTargets.UTF32),
            createCollation(182, "utf32_unicode_520_ci", CharsetTargets.UTF32),
            createCollation(183, "utf32_vietnamese_ci", CharsetTargets.UTF32),
            createCollation(192, "utf8_unicode_ci", CharsetTargets.UTF8),
            createCollation(193, "utf8_icelandic_ci", CharsetTargets.UTF8),
            createCollation(194, "utf8_latvian_ci", CharsetTargets.UTF8),
            createCollation(195, "utf8_romanian_ci", CharsetTargets.UTF8),
            createCollation(196, "utf8_slovenian_ci", CharsetTargets.UTF8),
            createCollation(197, "utf8_polish_ci", CharsetTargets.UTF8),
            createCollation(198, "utf8_estonian_ci", CharsetTargets.UTF8),
            createCollation(199, "utf8_spanish_ci", CharsetTargets.UTF8),
            createCollation(200, "utf8_swedish_ci", CharsetTargets.UTF8),
            createCollation(201, "utf8_turkish_ci", CharsetTargets.UTF8),
            createCollation(202, "utf8_czech_ci", CharsetTargets.UTF8),
            createCollation(203, "utf8_danish_ci", CharsetTargets.UTF8),
            createCollation(204, "utf8_lithuanian_ci", CharsetTargets.UTF8),
            createCollation(205, "utf8_slovak_ci", CharsetTargets.UTF8),
            createCollation(206, "utf8_spanish2_ci", CharsetTargets.UTF8),
            createCollation(207, "utf8_roman_ci", CharsetTargets.UTF8),
            createCollation(208, "utf8_persian_ci", CharsetTargets.UTF8),
            createCollation(209, "utf8_esperanto_ci", CharsetTargets.UTF8),
            createCollation(210, "utf8_hungarian_ci", CharsetTargets.UTF8),
            createCollation(211, "utf8_sinhala_ci", CharsetTargets.UTF8),
            createCollation(212, "utf8_german2_ci", CharsetTargets.UTF8),
            createCollation(213, "utf8_croatian_ci", CharsetTargets.UTF8),
            createCollation(214, "utf8_unicode_520_ci", CharsetTargets.UTF8),
            createCollation(215, "utf8_vietnamese_ci", CharsetTargets.UTF8),
            createCollation(223, "utf8_general_mysql500_ci", CharsetTargets.UTF8),
            createCollation(224, "utf8mb4_unicode_ci", CharsetTargets.UTF8MB4),
            createCollation(225, "utf8mb4_icelandic_ci", CharsetTargets.UTF8MB4),
            createCollation(226, "utf8mb4_latvian_ci", CharsetTargets.UTF8MB4),
            createCollation(227, "utf8mb4_romanian_ci", CharsetTargets.UTF8MB4),
            createCollation(228, "utf8mb4_slovenian_ci", CharsetTargets.UTF8MB4),
            createCollation(229, "utf8mb4_polish_ci", CharsetTargets.UTF8MB4),
            createCollation(230, "utf8mb4_estonian_ci", CharsetTargets.UTF8MB4),
            createCollation(231, "utf8mb4_spanish_ci", CharsetTargets.UTF8MB4),
            createCollation(232, "utf8mb4_swedish_ci", CharsetTargets.UTF8MB4),
            createCollation(233, "utf8mb4_turkish_ci", CharsetTargets.UTF8MB4),
            createCollation(234, "utf8mb4_czech_ci", CharsetTargets.UTF8MB4),
            createCollation(235, "utf8mb4_danish_ci", CharsetTargets.UTF8MB4),
            createCollation(236, "utf8mb4_lithuanian_ci", CharsetTargets.UTF8MB4),
            createCollation(237, "utf8mb4_slovak_ci", CharsetTargets.UTF8MB4),
            createCollation(238, "utf8mb4_spanish2_ci", CharsetTargets.UTF8MB4),
            createCollation(239, "utf8mb4_roman_ci", CharsetTargets.UTF8MB4),
            createCollation(240, "utf8mb4_persian_ci", CharsetTargets.UTF8MB4),
            createCollation(241, "utf8mb4_esperanto_ci", CharsetTargets.UTF8MB4),
            createCollation(242, "utf8mb4_hungarian_ci", CharsetTargets.UTF8MB4),
            createCollation(243, "utf8mb4_sinhala_ci", CharsetTargets.UTF8MB4),
            createCollation(244, "utf8mb4_german2_ci", CharsetTargets.UTF8MB4),
            createCollation(245, "utf8mb4_croatian_ci", CharsetTargets.UTF8MB4),
            createCollation(246, "utf8mb4_unicode_520_ci", CharsetTargets.UTF8MB4),
            createCollation(247, "utf8mb4_vietnamese_ci", CharsetTargets.UTF8MB4),
            createCollation(248, "gb18030_chinese_ci", CharsetTargets.GB18030),
            createCollation(249, "gb18030_bin", CharsetTargets.GB18030),
            createCollation(250, "gb18030_unicode_520_ci", CharsetTargets.GB18030),
            UTF8MB4_0900_AI_CI,
            createCollation(256, "utf8mb4_de_pb_0900_ai_ci", CharsetTargets.UTF8MB4),
            createCollation(257, "utf8mb4_is_0900_ai_ci", CharsetTargets.UTF8MB4),
            createCollation(258, "utf8mb4_lv_0900_ai_ci", CharsetTargets.UTF8MB4),
            createCollation(259, "utf8mb4_ro_0900_ai_ci", CharsetTargets.UTF8MB4),
            createCollation(260, "utf8mb4_sl_0900_ai_ci", CharsetTargets.UTF8MB4),
            createCollation(261, "utf8mb4_pl_0900_ai_ci", CharsetTargets.UTF8MB4),
            createCollation(262, "utf8mb4_et_0900_ai_ci", CharsetTargets.UTF8MB4),
            createCollation(263, "utf8mb4_es_0900_ai_ci", CharsetTargets.UTF8MB4),
            createCollation(264, "utf8mb4_sv_0900_ai_ci", CharsetTargets.UTF8MB4),
            createCollation(265, "utf8mb4_tr_0900_ai_ci", CharsetTargets.UTF8MB4),
            createCollation(266, "utf8mb4_cs_0900_ai_ci", CharsetTargets.UTF8MB4),
            createCollation(267, "utf8mb4_da_0900_ai_ci", CharsetTargets.UTF8MB4),
            createCollation(268, "utf8mb4_lt_0900_ai_ci", CharsetTargets.UTF8MB4),
            createCollation(269, "utf8mb4_sk_0900_ai_ci", CharsetTargets.UTF8MB4),
            createCollation(270, "utf8mb4_es_trad_0900_ai_ci", CharsetTargets.UTF8MB4),
            createCollation(271, "utf8mb4_la_0900_ai_ci", CharsetTargets.UTF8MB4),
            createCollation(273, "utf8mb4_eo_0900_ai_ci", CharsetTargets.UTF8MB4),
            createCollation(274, "utf8mb4_hu_0900_ai_ci", CharsetTargets.UTF8MB4),
            createCollation(275, "utf8mb4_hr_0900_ai_ci", CharsetTargets.UTF8MB4),
            createCollation(277, "utf8mb4_vi_0900_ai_ci", CharsetTargets.UTF8MB4),
            createCollation(278, "utf8mb4_0900_as_cs", CharsetTargets.UTF8MB4),
            createCollation(279, "utf8mb4_de_pb_0900_as_cs", CharsetTargets.UTF8MB4),
            createCollation(280, "utf8mb4_is_0900_as_cs", CharsetTargets.UTF8MB4),
            createCollation(281, "utf8mb4_lv_0900_as_cs", CharsetTargets.UTF8MB4),
            createCollation(282, "utf8mb4_ro_0900_as_cs", CharsetTargets.UTF8MB4),
            createCollation(283, "utf8mb4_sl_0900_as_cs", CharsetTargets.UTF8MB4),
            createCollation(284, "utf8mb4_pl_0900_as_cs", CharsetTargets.UTF8MB4),
            createCollation(285, "utf8mb4_et_0900_as_cs", CharsetTargets.UTF8MB4),
            createCollation(286, "utf8mb4_es_0900_as_cs", CharsetTargets.UTF8MB4),
            createCollation(287, "utf8mb4_sv_0900_as_cs", CharsetTargets.UTF8MB4),
            createCollation(288, "utf8mb4_tr_0900_as_cs", CharsetTargets.UTF8MB4),
            createCollation(289, "utf8mb4_cs_0900_as_cs", CharsetTargets.UTF8MB4),
            createCollation(290, "utf8mb4_da_0900_as_cs", CharsetTargets.UTF8MB4),
            createCollation(291, "utf8mb4_lt_0900_as_cs", CharsetTargets.UTF8MB4),
            createCollation(292, "utf8mb4_sk_0900_as_cs", CharsetTargets.UTF8MB4),
            createCollation(293, "utf8mb4_es_trad_0900_as_cs", CharsetTargets.UTF8MB4),
            createCollation(294, "utf8mb4_la_0900_as_cs", CharsetTargets.UTF8MB4),
            createCollation(296, "utf8mb4_eo_0900_as_cs", CharsetTargets.UTF8MB4),
            createCollation(297, "utf8mb4_hu_0900_as_cs", CharsetTargets.UTF8MB4),
            createCollation(298, "utf8mb4_hr_0900_as_cs", CharsetTargets.UTF8MB4),
            createCollation(300, "utf8mb4_vi_0900_as_cs", CharsetTargets.UTF8MB4),
            createCollation(303, "utf8mb4_ja_0900_as_cs", CharsetTargets.UTF8MB4),
            createCollation(304, "utf8mb4_ja_0900_as_cs_ks", CharsetTargets.UTF8MB4),
            createCollation(305, "utf8mb4_0900_as_ci", CharsetTargets.UTF8MB4),
            createCollation(306, "utf8mb4_ru_0900_ai_ci", CharsetTargets.UTF8MB4),
            createCollation(307, "utf8mb4_ru_0900_as_cs", CharsetTargets.UTF8MB4),
            createCollation(308, "utf8mb4_zh_0900_as_cs", CharsetTargets.UTF8MB4),
            createCollation(326, "utf8mb4_test_ci", CharsetTargets.UTF8MB4),
            createCollation(327, "utf16_test_ci", CharsetTargets.UTF16),
            createCollation(328, "utf8mb4_test_400_ci", CharsetTargets.UTF8MB4),
            createCollation(336, "utf8_bengali_standard_ci", CharsetTargets.UTF8),
            createCollation(337, "utf8_bengali_traditional_ci", CharsetTargets.UTF8),
            createCollation(352, "utf8_phone_ci", CharsetTargets.UTF8),
            createCollation(353, "utf8_test_ci", CharsetTargets.UTF8),
            createCollation(354, "utf8_5624_1", CharsetTargets.UTF8),
            createCollation(355, "utf8_5624_2", CharsetTargets.UTF8),
            createCollation(356, "utf8_5624_3", CharsetTargets.UTF8),
            createCollation(357, "utf8_5624_4", CharsetTargets.UTF8),
            createCollation(358, "ucs2_test_ci", CharsetTargets.UCS2),
            createCollation(359, "ucs2_vn_ci", CharsetTargets.UCS2),
            createCollation(360, "ucs2_5624_1", CharsetTargets.UCS2),
            createCollation(368, "utf8_5624_5", CharsetTargets.UTF8)
        };
    }

    private static CharCollation[] universeExtra() {
        // Note: all IDs must bigger than the max ID of universe.
        return new CharCollation[]{
            createCollation(391, "utf32_test_ci", CharsetTargets.UTF32),
            createCollation(2047, "utf8_maxuserid_ci", CharsetTargets.UTF8)
        };
    }
}
