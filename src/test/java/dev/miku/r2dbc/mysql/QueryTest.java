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

package dev.miku.r2dbc.mysql;

import dev.miku.r2dbc.mysql.constant.DataTypes;
import dev.miku.r2dbc.mysql.message.ParameterValue;
import dev.miku.r2dbc.mysql.message.client.ParameterWriter;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link Query}.
 */
class QueryTest {

    @Test
    void parse() {
        // Note: not be parametrized query, just for test.
        assertSimpleQuery("INSERT INTO `user` (`id`, `name`) VALUES (1, 'hello')");
        assertSimpleQuery("INSERT INTO `user` (`id`, `name`) VALUES (1, 'hello''world')");
        assertSimpleQuery("SELECT * FROM `user ? hello` WHERE `id` = '?' AND `name` LIKE '?name' AND `good` = \"dsa?\" /* ??? */ -- ??");

        assertPrepareQuery("SELECT * FROM `user` WHERE (? = ?)", "SELECT * FROM `user` WHERE (? = ?)", Collections.emptyMap(), 2);
        assertPrepareQuery("?", "?", Collections.emptyMap(), 1);
        assertPrepareQuery("?n", "?", mapOf(link("n", 0)), 1);
        assertPrepareQuery("?name", "?", mapOf(link("name", 0)), 1);
        assertPrepareQuery("? SELECT", "? SELECT", Collections.emptyMap(), 1);
        assertPrepareQuery("?n SELECT", "? SELECT", mapOf(link("n", 0)), 1);
        assertPrepareQuery("?name SELECT", "? SELECT", mapOf(link("name", 0)), 1);

        assertPrepareQuery("SELECT * FROM `user` WHERE `id` = ?", "SELECT * FROM `user` WHERE `id` = ?", Collections.emptyMap(), 1);
        assertPrepareQuery("SELECT * FROM `user` WHERE `id` = ?i", "SELECT * FROM `user` WHERE `id` = ?", mapOf(link("i", 0)), 1);
        assertPrepareQuery("SELECT * FROM `user` WHERE `id` = ?id", "SELECT * FROM `user` WHERE `id` = ?", mapOf(link("id", 0)), 1);
        assertPrepareQuery("INSERT INTO `user` VALUES (?, ?, ?, ?)", "INSERT INTO `user` VALUES (?, ?, ?, ?)", Collections.emptyMap(), 4);
        assertPrepareQuery("INSERT INTO `user` VALUES (?, ?a, ?, ?b)", "INSERT INTO `user` VALUES (?, ?, ?, ?)", mapOf(link("a", 1), link("b", 3)), 4);
        assertPrepareQuery("INSERT INTO `user` VALUES (?a, ?b, ?c, ?d)", "INSERT INTO `user` VALUES (?, ?, ?, ?)", mapOf(link("a", 0), link("b", 1), link("c", 2), link("d", 3)), 4);
        assertPrepareQuery("INSERT INTO `user` VALUES (?a, ?a, ?a, ?a, ?a, ?a, ?a, ?a, ?a)", "INSERT INTO `user` VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", mapOf(link("a", 0, 1, 2, 3, 4, 5, 6, 7, 8)), 9);

        assertPrepareQuery("UPDATE `user` SET `name` = ?", "UPDATE `user` SET `name` = ?", Collections.emptyMap(), 1);
        assertPrepareQuery("UPDATE `user` SET `name` = ?n", "UPDATE `user` SET `name` = ?", mapOf(link("n", 0)), 1);
        assertPrepareQuery("UPDATE `user` SET `name` = ?n/", "UPDATE `user` SET `name` = ?/", mapOf(link("n", 0)), 1);
        assertPrepareQuery("UPDATE `user` SET `name` = ?name", "UPDATE `user` SET `name` = ?", mapOf(link("name", 0)), 1);
        assertPrepareQuery("UPDATE `user` SET `name` = ?name, `good` = ?", "UPDATE `user` SET `name` = ?, `good` = ?", mapOf(link("name", 0)), 2);
        assertPrepareQuery("UPDATE `user` SET `name` = ?name, `good` = ?-", "UPDATE `user` SET `name` = ?, `good` = ?-", mapOf(link("name", 0)), 2);
        assertPrepareQuery("UPDATE `user` SET `name` = ?, `good` = ?g", "UPDATE `user` SET `name` = ?, `good` = ?", mapOf(link("g", 1)), 2);

        assertPrepareQuery("UPDATE `user` SET `name` = ?name, -- This is comment\n `good` = ?", "UPDATE `user` SET `name` = ?, -- This is comment\n `good` = ?", mapOf(link("name", 0)), 2);

        assertPrepareQuery(
            "INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?, ?) ON DUPLICATE KEY UPDATE `name` = ?, `age` = ?",
            "INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?, ?) ON DUPLICATE KEY UPDATE `name` = ?, `age` = ?",
            Collections.emptyMap(),
            5
        );
        assertPrepareQuery(
            "INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?name, ?) ON DUPLICATE KEY UPDATE `name` = ?name",
            "INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?, ?) ON DUPLICATE KEY UPDATE `name` = ?",
            mapOf(link("name", 1, 3)),
            4
        );
        assertPrepareQuery(
            "INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?, ?age) ON DUPLICATE KEY UPDATE `age` = ?age",
            "INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?, ?) ON DUPLICATE KEY UPDATE `age` = ?",
            mapOf(link("age", 2, 3)),
            4
        );
        assertPrepareQuery(
            "INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?name, ?age) ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age",
            "INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?, ?) ON DUPLICATE KEY UPDATE `name` = ?, `age` = ?",
            mapOf(link("name", 1, 3), link("age", 2, 4)),
            5
        );
        assertPrepareQuery(
            "INSERT INTO `user` (`id`, `name`, `age`) VALUE (?id, ?name, ?age) ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age",
            "INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?, ?) ON DUPLICATE KEY UPDATE `name` = ?, `age` = ?",
            mapOf(link("id", 0), link("name", 1, 3), link("age", 2, 4)),
            5
        );

        assertPrepareQuery("UPDATE `user` SET `name` = 2-?", "UPDATE `user` SET `name` = 2-?", Collections.emptyMap(), 1);
    }

    @Test
    void bind() {
        Query query = Query.parse("INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?name, ?age) ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age");
        Binding binding = new Binding(5);
        assertEquals(binding.findUnbind(), 0);
        binding.add(0, MockValue.INSTANCE);
        assertEquals(binding.findUnbind(), 1);
        Object indexes = query.getIndexes("name");
        assertTrue(indexes instanceof Query.Indexes);
        ((Query.Indexes) indexes).bind(binding, MockValue.INSTANCE);
        assertEquals(binding.findUnbind(), 2);
        indexes = query.getIndexes("age");
        assertTrue(indexes instanceof Query.Indexes);
        ((Query.Indexes) indexes).bind(binding, MockValue.INSTANCE);
        assertEquals(binding.findUnbind(), -1);

        query = Query.parse("INSERT INTO `user` (`id`, `nickname`, `real_name`) VALUE (?, ?initName, ?initName) ON DUPLICATE KEY UPDATE `nickname` = ?updateName, `real_name` = ?updateName");
        binding = new Binding(5);
        assertEquals(binding.findUnbind(), 0);
        binding.add(0, MockValue.INSTANCE);
        assertEquals(binding.findUnbind(), 1);
        indexes = query.getIndexes("initName");
        assertTrue(indexes instanceof Query.Indexes);
        ((Query.Indexes) indexes).bind(binding, MockValue.INSTANCE);
        assertEquals(binding.findUnbind(), 3);
        indexes = query.getIndexes("updateName");
        assertTrue(indexes instanceof Query.Indexes);
        ((Query.Indexes) indexes).bind(binding, MockValue.INSTANCE);
        assertEquals(binding.findUnbind(), -1);
    }

    @Test
    void getIndexes() {
        Query query = Query.parse("INSERT INTO `user` (`id`, `name`, `age`) VALUE (?id, ?name, ?age) ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age");
        Object indexes = query.getIndexes("id");
        assertEquals(indexes, 0);

        indexes = query.getIndexes("name");
        assertTrue(indexes instanceof Query.Indexes);
        int[] data = ((Query.Indexes) indexes).toIntArray();
        assertArrayEquals(data, new int[]{1, 3});

        indexes = query.getIndexes("age");
        assertTrue(indexes instanceof Query.Indexes);
        data = ((Query.Indexes) indexes).toIntArray();
        assertArrayEquals(data, new int[]{2, 4});

        query = Query.parse("INSERT INTO `user` VALUES (?a, ?a, ?a, ?a, ?a, ?a, ?a, ?a, ?a)");
        indexes = query.getIndexes("a");
        assertTrue(indexes instanceof Query.Indexes);
        data = ((Query.Indexes) indexes).toIntArray();
        assertArrayEquals(data, new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8});
    }

    @Test
    void indexesEquals() {
        Query query1 = Query.parse("INSERT INTO `user` (`id`, `name`, `age`, `updated_at`, `created_at`) VALUE (?id, ?name, ?age, ?now, ?now) ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age, `updated_at` = ?now");
        Query query2 = Query.parse("INSERT INTO `user` (`id`, `name`, `age`, `updated_at`, `created_at`) VALUE (?id, ?name, ?age, ?now, ?now) ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age, `updated_at` = ?now");

        assertEquals(query1.getIndexes("name"), query1.getIndexes("name"));
        assertEquals(query1.getIndexes("now"), query1.getIndexes("now"));
        assertEquals(query1.getIndexes("name"), query2.getIndexes("name"));
        assertEquals(query1.getIndexes("now"), query2.getIndexes("now"));

        assertNotEquals(query1.getIndexes("name"), "name");
        assertNotEquals(query1.getIndexes("name"), query1.getIndexes("age"));
        assertNotEquals(query1.getIndexes("name"), query1.getIndexes("id"));
        assertNotEquals(query1.getIndexes("name"), query1.getIndexes("now"));
    }

    @Test
    void selfHashCode() {
        Query query1 = Query.parse("INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?name, ?age) ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age");
        Query query2 = Query.parse("INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?name, ?age) ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age");

        assertEquals(query1.hashCode(), query2.hashCode());

        query1 = Query.parse("INSERT INTO `user` (`id`, `name`, `age`) VALUE (?id, ?name, ?age) ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age");
        query2 = Query.parse("INSERT INTO `user` (`id`, `name`, `age`) VALUE (?id, ?name, ?age) ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age");

        assertEquals(query1.hashCode(), query2.hashCode());
    }

    @Test
    void selfEquals() {
        Query query1 = Query.parse("INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?name, ?age) ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age");
        Query query2 = Query.parse("INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?name, ?age) ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age");

        assertEquals(query1, query1);
        assertEquals(query1, query2);
        assertNotEquals(query1, "INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?name, ?age) ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age");

        query1 = Query.parse("INSERT INTO `user` (`id`, `name`, `age`) VALUE (?id, ?name, ?age) ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age");
        query2 = Query.parse("INSERT INTO `user` (`id`, `name`, `age`) VALUE (?id, ?name, ?age) ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age");

        assertEquals(query1, query1);
        assertEquals(query1, query2);
        assertNotEquals(query1, "INSERT INTO `user` (`id`, `name`, `age`) VALUE (?id, ?name, ?age) ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age");

        query1 = Query.parse("UPDATE `user` SET `name` = ?name, `good` = ?");
        assertNotEquals(query1, query2);

        query1 = Query.parse("UPDATE `user` SET `name` = ?, `good` = ?, `age` = ?, `data` = ?, `content` = ?");
        assertNotEquals(query1, query2);

        query1 = Query.parse("INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?name, ?age) ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age");
        assertNotEquals(query1, query2);
    }

    @Test
    void rejectGetIndexes() {
        assertThrows(IllegalArgumentException.class, () -> Query.parse("INSERT INTO `user` (`id`, `name`) VALUES (1, 'hello')").getIndexes("v"));
        assertThrows(IllegalArgumentException.class, () -> Query.parse("UPDATE `user` SET `name` = ?").getIndexes("name"));
        assertThrows(IllegalArgumentException.class, () -> Query.parse("UPDATE `user` SET `name` = ?n").getIndexes("name"));
        assertThrows(IllegalArgumentException.class, () -> Query.parse("UPDATE `user` SET `name` = ?name").getIndexes("n"));
    }

    @Test
    void rejectGetData() {
        Object indexes = Query.parse("UPDATE `user` SET `name` = ?n").getIndexes("n");
        assertTrue(indexes instanceof Integer);
    }

    private static void assertPrepareQuery(String sql, String parsedSql, Map<String, int[]> nameKeyedIndexes, int parameters) {
        Query query = Query.parse(sql);
        assertTrue(query.toString().contains("parameters"));
        assertTrue(query.isPrepared());
        assertEquals(query.getSql(), parsedSql);
        assertEquals(query.getParameters(), parameters);
        assertEquals(query.getParameterNames(), nameKeyedIndexes.keySet());
        for (Map.Entry<String, int[]> entry : nameKeyedIndexes.entrySet()) {
            Object indexes = query.getIndexes(entry.getKey());
            int[] right = entry.getValue();
            int[] left;

            if (indexes instanceof Integer) {
                left = new int[]{(int) indexes};
            } else {
                left = ((Query.Indexes) indexes).toIntArray();
            }

            assertArrayEquals(left, right, () -> String.format("expected: %s but was: %s", indexes, Arrays.toString(right)));
        }
    }

    private static void assertSimpleQuery(String sql) {
        Query query = Query.parse(sql);
        assertFalse(query.toString().contains("parameters"));
        assertFalse(query.isPrepared());
    }

    private static Tuple2<String, int[]> link(String name, int... indexes) {
        if (indexes.length == 0) {
            throw new IllegalArgumentException("must has least one index");
        }

        return Tuples.of(name, indexes);
    }

    @SafeVarargs
    private static Map<String, int[]> mapOf(Tuple2<String, int[]>... tuples) {
        // ceil(size / 0.75) = ceil((size * 4) / 3) = floor((size * 4 + 3 - 1) / 3)
        Map<String, int[]> result = new HashMap<>(((tuples.length << 2) + 2) / 3, 0.75f);

        for (Tuple2<String, int[]> tuple : tuples) {
            result.put(tuple.getT1(), tuple.getT2());
        }

        return result;
    }

    private static final class MockValue implements ParameterValue {

        private static final MockValue INSTANCE = new MockValue();

        private MockValue() {
        }

        @Override
        public boolean isNull() {
            return false;
        }

        @Override
        public Mono<Void> writeTo(ParameterWriter writer) {
            return Mono.empty();
        }

        @Override
        public short getType() {
            return DataTypes.INT;
        }

        @Override
        public void dispose() {
        }

        @Override
        public String toString() {
            return "MockValue{}";
        }
    }
}
