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

package dev.miku.r2dbc.mysql;

import org.junit.jupiter.api.Test;
import reactor.util.annotation.Nullable;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link MySqlNames}.
 */
class MySqlNamesTest {

    private static final String[] NAMES = { "c", "dD", "cBc", "Dca", "ADC", "DcA", "abc", "b", "B", "dA",
        "AB", "a", "Abc", "ABC", "A", "ab", "cc", "Da", "CbC" };

    private static final String[] SINGLETON = { "name" };

    private static final Set<String> CS_NAME_SET = new HashSet<>();

    private static final Set<String> CI_NAME_SET = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

    static {
        CS_NAME_SET.addAll(Arrays.asList(NAMES));
        CI_NAME_SET.addAll(Arrays.asList(NAMES));
        Arrays.sort(NAMES, MySqlNames::compare);
    }

    @Test
    void nameSearch() {
        Consumer<String> judge = name -> {
            if (CS_NAME_SET.contains(name)) {
                assertEquals(NAMES[MySqlNames.nameSearch(NAMES, name)], name);
                assertEquals(NAMES[MySqlNames.nameSearch(NAMES, String.format("`%s`", name))], name);
            } else if (CI_NAME_SET.contains(name)) {
                assertTrue(NAMES[MySqlNames.nameSearch(NAMES, name)].equalsIgnoreCase(name));
                assertEquals(MySqlNames.nameSearch(NAMES, String.format("`%s`", name)), -1);
            } else {
                assertEquals(MySqlNames.nameSearch(NAMES, name), -1);
                assertEquals(MySqlNames.nameSearch(NAMES, String.format("`%s`", name)), -1);
            }
        };
        nameGenerate(1, judge);
        nameGenerate(2, judge);
        nameGenerate(3, judge);

        assertEquals(SINGLETON[MySqlNames.nameSearch(SINGLETON, "name")], "name");
        assertEquals(SINGLETON[MySqlNames.nameSearch(SINGLETON, "Name")], "name");
        assertEquals(SINGLETON[MySqlNames.nameSearch(SINGLETON, "nAMe")], "name");
        assertEquals(SINGLETON[MySqlNames.nameSearch(SINGLETON, "`name`")], "name");
        assertEquals(MySqlNames.nameSearch(SINGLETON, "`Name`"), -1);
        assertEquals(MySqlNames.nameSearch(SINGLETON, "`nAMe`"), -1);
    }

    /**
     * A full-arrangement of repeatable selections is generated in 'a' - 'd' and 'A' - 'D' of fixed length
     * String.
     * <p>
     * e.g. input: 2, publish: aa ab ac ad aA aB aC ... DB DC DD
     */
    private static void nameGenerate(int length, Consumer<String> nameConsumer) {
        nameGen0(length, null, nameConsumer);
    }

    private static void nameGen0(int length, @Nullable String prefix, Consumer<String> nameConsumer) {
        if (length <= 1) {
            for (char c = 'a'; c < 'e'; ++c) {
                if (prefix == null) {
                    nameConsumer.accept(Character.toString(c));
                } else {
                    nameConsumer.accept(prefix + c);
                }
            }
            for (char c = 'A'; c < 'E'; ++c) {
                if (prefix == null) {
                    nameConsumer.accept(Character.toString(c));
                } else {
                    nameConsumer.accept(prefix + c);
                }
            }
        } else {
            for (char c = 'a'; c < 'e'; ++c) {
                if (prefix == null) {
                    nameGen0(length - 1, Character.toString(c), nameConsumer);
                } else {
                    nameGen0(length - 1, prefix + c, nameConsumer);
                }
            }
            for (char c = 'A'; c < 'E'; ++c) {
                if (prefix == null) {
                    nameGen0(length - 1, Character.toString(c), nameConsumer);
                } else {
                    nameGen0(length - 1, prefix + c, nameConsumer);
                }
            }
        }
    }
}
