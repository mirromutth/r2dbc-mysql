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

package dev.miku.r2dbc.mysql;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link TextQuery}.
 */
class TextQueryTest extends QueryTestSupport {

    @Override
    @Test
    void parse() {
        assertTextQuery("SELECT * FROM `user` WHERE (? = ?)", "SELECT * FROM `user` WHERE (? = ?)", Collections.emptyMap(), 2);
        assertTextQuery("?", "?", Collections.emptyMap(), 1);
        assertTextQuery("?n", "?", mapOf(link("n", 0)), 1);
        assertTextQuery("?name", "?", mapOf(link("name", 0)), 1);
        assertTextQuery("? SELECT", "? SELECT", Collections.emptyMap(), 1);
        assertTextQuery("?n SELECT", "? SELECT", mapOf(link("n", 0)), 1);
        assertTextQuery("?name SELECT", "? SELECT", mapOf(link("name", 0)), 1);

        assertTextQuery("SELECT * FROM `user` WHERE `id` = ?", "SELECT * FROM `user` WHERE `id` = ?", Collections.emptyMap(), 1);
        assertTextQuery("SELECT * FROM `user` WHERE `id` = ?i", "SELECT * FROM `user` WHERE `id` = ?", mapOf(link("i", 0)), 1);
        assertTextQuery("SELECT * FROM `user` WHERE `id` = ?id", "SELECT * FROM `user` WHERE `id` = ?", mapOf(link("id", 0)), 1);
        assertTextQuery("INSERT INTO `user` VALUES (?, ?, ?, ?)", "INSERT INTO `user` VALUES (?, ?, ?, ?)", Collections.emptyMap(), 4);
        assertTextQuery("INSERT INTO `user` VALUES (?, ?a, ?, ?b)", "INSERT INTO `user` VALUES (?, ?, ?, ?)", mapOf(link("a", 1), link("b", 3)), 4);
        assertTextQuery("INSERT INTO `user` VALUES (?a, ?b, ?c, ?d)", "INSERT INTO `user` VALUES (?, ?, ?, ?)", mapOf(link("a", 0), link("b", 1), link("c", 2), link("d", 3)), 4);
        assertTextQuery("INSERT INTO `user` VALUES (?a, ?a, ?a, ?a, ?a, ?a, ?a, ?a, ?a)", "INSERT INTO `user` VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", mapOf(link("a", 0, 1, 2, 3, 4, 5, 6, 7, 8)), 9);

        assertTextQuery("UPDATE `user` SET `name` = ?", "UPDATE `user` SET `name` = ?", Collections.emptyMap(), 1);
        assertTextQuery("UPDATE `user` SET `name` = ?n", "UPDATE `user` SET `name` = ?", mapOf(link("n", 0)), 1);
        assertTextQuery("UPDATE `user` SET `name` = ?n/", "UPDATE `user` SET `name` = ?/", mapOf(link("n", 0)), 1);
        assertTextQuery("UPDATE `user` SET `name` = ?name", "UPDATE `user` SET `name` = ?", mapOf(link("name", 0)), 1);
        assertTextQuery("UPDATE `user` SET `name` = ?name, `good` = ?", "UPDATE `user` SET `name` = ?, `good` = ?", mapOf(link("name", 0)), 2);
        assertTextQuery("UPDATE `user` SET `name` = ?name, `good` = ?-", "UPDATE `user` SET `name` = ?, `good` = ?-", mapOf(link("name", 0)), 2);
        assertTextQuery("UPDATE `user` SET `name` = ?, `good` = ?g", "UPDATE `user` SET `name` = ?, `good` = ?", mapOf(link("g", 1)), 2);

        assertTextQuery("UPDATE `user` SET `name` = ?name, -- This is comment\n `good` = ?", "UPDATE `user` SET `name` = ?, -- This is comment\n `good` = ?", mapOf(link("name", 0)), 2);

        assertTextQuery(
            "INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?, ?) ON DUPLICATE KEY UPDATE `name` = ?, `age` = ?",
            "INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?, ?) ON DUPLICATE KEY UPDATE `name` = ?, `age` = ?",
            Collections.emptyMap(),
            5
        );
        assertTextQuery(
            "INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?name, ?) ON DUPLICATE KEY UPDATE `name` = ?name",
            "INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?, ?) ON DUPLICATE KEY UPDATE `name` = ?",
            mapOf(link("name", 1, 3)),
            4
        );
        assertTextQuery(
            "INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?, ?age) ON DUPLICATE KEY UPDATE `age` = ?age",
            "INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?, ?) ON DUPLICATE KEY UPDATE `age` = ?",
            mapOf(link("age", 2, 3)),
            4
        );
        assertTextQuery(
            "INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?name, ?age) ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age",
            "INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?, ?) ON DUPLICATE KEY UPDATE `name` = ?, `age` = ?",
            mapOf(link("name", 1, 3), link("age", 2, 4)),
            5
        );
        assertTextQuery(
            "INSERT INTO `user` (`id`, `name`, `age`) VALUE (?id, ?name, ?age) ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age",
            "INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?, ?) ON DUPLICATE KEY UPDATE `name` = ?, `age` = ?",
            mapOf(link("id", 0), link("name", 1, 3), link("age", 2, 4)),
            5
        );

        assertTextQuery("UPDATE `user` SET `name` = 2-?", "UPDATE `user` SET `name` = 2-?", Collections.emptyMap(), 1);
    }

    @Override
    @Test
    void getIndexes() {
        Query query = Query.parse("INSERT INTO `user` (`id`, `name`, `age`) VALUE (?id, ?name, ?age) ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age", false);

        assertArrayEquals(query.getIndexes("id").toIntArray(), new int[]{0});
        assertArrayEquals(query.getIndexes("name").toIntArray(), new int[]{1, 3});
        assertArrayEquals(query.getIndexes("age").toIntArray(), new int[]{2, 4});

        assertArrayEquals(Query.parse("INSERT INTO `user` VALUES (?a, ?a, ?a, ?a, ?a, ?a, ?a, ?a, ?a)", false)
            .getIndexes("a").toIntArray(), new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8});
    }

    @Override
    @Test
    void bind() {
        Query query = Query.parse("INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?name, ?age) ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age", false);
        Binding binding = new Binding(5);
        assertEquals(binding.findUnbind(), 0);

        binding.add(0, MockParameter.INSTANCE);
        assertEquals(binding.findUnbind(), 1);

        assertEquals(2, query.getIndexes("name").toIntArray().length);
        query.getIndexes("name").bind(binding, MockParameter.INSTANCE);
        assertEquals(binding.findUnbind(), 2);

        assertEquals(2, query.getIndexes("age").toIntArray().length);
        query.getIndexes("age").bind(binding, MockParameter.INSTANCE);
        assertEquals(binding.findUnbind(), -1);

        query = Query.parse("INSERT INTO `user` (`id`, `nickname`, `real_name`) VALUE (?, ?initName, ?initName) ON DUPLICATE KEY UPDATE `nickname` = ?updateName, `real_name` = ?updateName", false);

        binding = new Binding(5);
        assertEquals(binding.findUnbind(), 0);

        binding.add(0, MockParameter.INSTANCE);
        assertEquals(binding.findUnbind(), 1);

        assertEquals(2, query.getIndexes("initName").toIntArray().length);
        query.getIndexes("initName").bind(binding, MockParameter.INSTANCE);
        assertEquals(binding.findUnbind(), 3);

        assertEquals(2, query.getIndexes("updateName").toIntArray().length);
        query.getIndexes("updateName").bind(binding, MockParameter.INSTANCE);
        assertEquals(binding.findUnbind(), -1);
    }

    @Override
    @Test
    void indexesEquals() {
        Query query1 = Query.parse("INSERT INTO `user` (`id`, `name`, `age`, `updated_at`, `created_at`) VALUE (?id, ?name, ?age, ?now, ?now) ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age, `updated_at` = ?now", false);
        Query query2 = Query.parse("INSERT INTO `user` (`id`, `name`, `age`, `updated_at`, `created_at`) VALUE (?id, ?name, ?age, ?now, ?now) ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age, `updated_at` = ?now", false);

        assertEquals(query1.getIndexes("name"), query1.getIndexes("name"));
        assertEquals(query1.getIndexes("now"), query1.getIndexes("now"));
        assertEquals(query1.getIndexes("name"), query2.getIndexes("name"));
        assertEquals(query1.getIndexes("now"), query2.getIndexes("now"));

        assertNotEquals(query1.getIndexes("name"), "name");
        assertNotEquals(query1.getIndexes("name"), query1.getIndexes("age"));
        assertNotEquals(query1.getIndexes("name"), query1.getIndexes("id"));
        assertNotEquals(query1.getIndexes("name"), query1.getIndexes("now"));
    }

    @Override
    @Test
    void selfHashCode() {
        Query query1 = Query.parse("INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?name, ?age) ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age", false);
        Query query2 = Query.parse("INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?name, ?age) ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age", false);

        assertEquals(query1.hashCode(), query2.hashCode());

        query1 = Query.parse("INSERT INTO `user` (`id`, `name`, `age`) VALUE (?id, ?name, ?age) ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age", false);
        query2 = Query.parse("INSERT INTO `user` (`id`, `name`, `age`) VALUE (?id, ?name, ?age) ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age", false);

        assertEquals(query1.hashCode(), query2.hashCode());
    }

    @Override
    @Test
    void selfEquals() {
        Query query1 = Query.parse("INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?name, ?age) ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age", false);
        Query query2 = Query.parse("INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?name, ?age) ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age", false);

        assertEquals(query1, query1);
        assertEquals(query1, query2);
        assertNotEquals(query1, "INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?name, ?age) ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age");

        query1 = Query.parse("INSERT INTO `user` (`id`, `name`, `age`) VALUE (?id, ?name, ?age) ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age", false);
        query2 = Query.parse("INSERT INTO `user` (`id`, `name`, `age`) VALUE (?id, ?name, ?age) ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age", false);

        assertEquals(query1, query1);
        assertEquals(query1, query2);
        assertNotEquals(query1, "INSERT INTO `user` (`id`, `name`, `age`) VALUE (?id, ?name, ?age) ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age");

        query1 = Query.parse("UPDATE `user` SET `name` = ?name, `good` = ?", false);
        assertNotEquals(query1, query2);

        query1 = Query.parse("UPDATE `user` SET `name` = ?, `good` = ?, `age` = ?, `data` = ?, `content` = ?", false);
        assertNotEquals(query1, query2);

        query1 = Query.parse("INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?name, ?age) ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age", false);
        assertNotEquals(query1, query2);
    }

    @Override
    @Test
    void rejectGetIndexes() {
        assertThrows(IllegalArgumentException.class, () -> Query.parse("UPDATE `user` SET `name` = ?", false).getIndexes("name"));
        assertThrows(IllegalArgumentException.class, () -> Query.parse("UPDATE `user` SET `name` = ?n", false).getIndexes("name"));
        assertThrows(IllegalArgumentException.class, () -> Query.parse("UPDATE `user` SET `name` = ?name", false).getIndexes("n"));
    }

    private static void assertTextQuery(String sql, String parsedSql, Map<String, int[]> nameKeyedIndexes, int parameters) {
        assertThat(Query.parse(sql, false)).isExactlyInstanceOf(TextQuery.class)
            .extracting(TextQuery.class::cast)
            .satisfies(it -> {
                assertEquals(String.join("?", it.getSqlParts()), parsedSql);
                assertEquals(it.getParameters(), parameters);
                assertEquals(it.getSqlParts().size() - 1, it.getParameters());
                assertEquals(it.getParameterNames(), nameKeyedIndexes.keySet());

                for (Map.Entry<String, int[]> entry : nameKeyedIndexes.entrySet()) {
                    ParameterIndex indexes = it.getIndexes(entry.getKey());
                    int[] right = entry.getValue();
                    int[] left = indexes.toIntArray();

                    assertArrayEquals(left, right, () -> String.format("expected: %s but was: %s", indexes, Arrays.toString(right)));
                }
            });
    }
}
