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

import dev.miku.r2dbc.mysql.constant.MySqlType;
import io.netty.buffer.ByteBuf;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link Query}.
 */
class QueryTest {

    @Test
    void parse() {
        assertQuery("INSERT INTO `user` (`id`, `name`) VALUES (1, 'hello')");
        assertQuery("INSERT INTO `user` (`id`, `name`) VALUES (1, 'hello''world')");
        assertQuery("SELECT * FROM `user ? hello` WHERE `id` = '?' AND `name` /* ??? */ " +
            "LIKE '?name' AND `good` = \"dsa?\" -- ??");

        assertQuery("SELECT * FROM `user` WHERE (? = ?)", "SELECT * FROM `user` WHERE (? = ?)",
            Collections.emptyMap(), 2);
        assertQuery("?", "?", Collections.emptyMap(), 1);
        assertQuery("?n", "?", mapOf(link("n", 0)), 1);
        assertQuery("?name", "?", mapOf(link("name", 0)), 1);
        assertQuery("? SELECT", "? SELECT", Collections.emptyMap(), 1);
        assertQuery("?n SELECT", "? SELECT", mapOf(link("n", 0)), 1);
        assertQuery("?name SELECT", "? SELECT", mapOf(link("name", 0)), 1);

        assertQuery("SELECT * FROM `user` WHERE `id` = ?", "SELECT * FROM `user` WHERE `id` = ?",
            Collections.emptyMap(), 1);
        assertQuery("SELECT * FROM `user` WHERE `id` = ?i", "SELECT * FROM `user` WHERE `id` = ?",
            mapOf(link("i", 0)), 1);
        assertQuery("SELECT * FROM `user` WHERE `id` = ?id", "SELECT * FROM `user` WHERE `id` = ?",
            mapOf(link("id", 0)), 1);
        assertQuery("INSERT INTO `user` VALUES (?, ?, ?, ?)", "INSERT INTO `user` VALUES (?, ?, ?, ?)",
            Collections.emptyMap(), 4);
        assertQuery("INSERT INTO `user` VALUES (?, ?a, ?, ?b)", "INSERT INTO `user` VALUES (?, ?, ?, ?)",
            mapOf(link("a", 1), link("b", 3)), 4);
        assertQuery("INSERT INTO `user` VALUES (?a, ?b, ?c, ?d)", "INSERT INTO `user` VALUES (?, ?, ?, ?)",
            mapOf(link("a", 0), link("b", 1), link("c", 2), link("d", 3)), 4);
        assertQuery("INSERT INTO `user` VALUES (?a, ?a, ?a, ?a, ?a, ?a, ?a, ?a, ?a)",
            "INSERT INTO `user` VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            mapOf(link("a", 0, 1, 2, 3, 4, 5, 6, 7, 8)), 9);

        assertQuery("UPDATE `user` SET `name` = ?", "UPDATE `user` SET `name` = ?",
            Collections.emptyMap(), 1);
        assertQuery("UPDATE `user` SET `name` = ?n", "UPDATE `user` SET `name` = ?",
            mapOf(link("n", 0)), 1);
        assertQuery("UPDATE `user` SET `name` = ?n/", "UPDATE `user` SET `name` = ?/",
            mapOf(link("n", 0)), 1);
        assertQuery("UPDATE `user` SET `name` = ?name", "UPDATE `user` SET `name` = ?",
            mapOf(link("name", 0)), 1);
        assertQuery("UPDATE `user` SET `name` = ?name, `good` = ?",
            "UPDATE `user` SET `name` = ?, `good` = ?",
            mapOf(link("name", 0)), 2);
        assertQuery("UPDATE `user` SET `name` = ?name, `good` = ?-",
            "UPDATE `user` SET `name` = ?, `good` = ?-",
            mapOf(link("name", 0)), 2);
        assertQuery("UPDATE `user` SET `name` = ?, `good` = ?g", "UPDATE `user` SET `name` = ?, `good` = ?",
            mapOf(link("g", 1)), 2);

        assertQuery("UPDATE `user` SET `name` = ?name, -- This is comment\n `good` = ?",
            "UPDATE `user` SET `name` = ?, -- This is comment\n `good` = ?",
            mapOf(link("name", 0)), 2);

        assertQuery("INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?, ?) " +
                "ON DUPLICATE KEY UPDATE `name` = ?, `age` = ?",
            "INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?, ?) " +
                "ON DUPLICATE KEY UPDATE `name` = ?, `age` = ?",
            Collections.emptyMap(), 5);
        assertQuery("INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?name, ?) " +
                "ON DUPLICATE KEY UPDATE `name` = ?name",
            "INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?, ?) " +
                "ON DUPLICATE KEY UPDATE `name` = ?",
            mapOf(link("name", 1, 3)), 4);
        assertQuery("INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?, ?age) " +
                "ON DUPLICATE KEY UPDATE `age` = ?age",
            "INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?, ?) " +
                "ON DUPLICATE KEY UPDATE `age` = ?",
            mapOf(link("age", 2, 3)), 4);
        assertQuery("INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?name, ?age) " +
                "ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age",
            "INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?, ?) " +
                "ON DUPLICATE KEY UPDATE `name` = ?, `age` = ?",
            mapOf(link("name", 1, 3), link("age", 2, 4)), 5);
        assertQuery("INSERT INTO `user` (`id`, `name`, `age`) VALUE (?id, ?name, ?age) " +
                "ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age",
            "INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?, ?) " +
                "ON DUPLICATE KEY UPDATE `name` = ?, `age` = ?",
            mapOf(link("id", 0), link("name", 1, 3), link("age", 2, 4)),
            5);

        assertQuery("UPDATE `user` SET `name` = 2-?", "UPDATE `user` SET `name` = 2-?",
            Collections.emptyMap(), 1);
    }

    @Test
    void getNamedIndexes() {
        Query query = Query.parse("INSERT INTO `user` (`id`, `name`, `age`) VALUE (?id, ?name, ?age) " +
            "ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age");
        Map<String, ParameterIndex> namedIndexes = query.getNamedIndexes();

        assertThat(namedIndexes.get("id")).isEqualTo(index(0));
        assertThat(namedIndexes.get("name")).isEqualTo(index(1, 3));
        assertThat(namedIndexes.get("age")).isEqualTo(index(2, 4));

        assertThat(Query.parse("INSERT INTO `user` VALUES (?a, ?a, ?a, ?a, ?a, ?a, ?a, ?a, ?a)")
            .getNamedIndexes().get("a")).isEqualTo(index(0, 1, 2, 3, 4, 5, 6, 7, 8));

        assertThat(Query.parse("UPDATE `user` SET `name` = ?").getNamedIndexes()).isEmpty();
        assertThat(Query.parse("UPDATE `user` SET `name` = ?n").getNamedIndexes()).containsOnlyKeys("n");
        assertThat(Query.parse("UPDATE `user` SET `name` = ?name").getNamedIndexes())
            .containsOnlyKeys("name");
    }

    @Test
    void bind() {
        Query query = Query.parse("INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?name, ?age) " +
            "ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age");
        Binding binding = new Binding(5);

        assertThat(binding.findUnbind()).isZero();

        binding.add(0, MockMySqlParameter.INSTANCE);
        assertThat(binding.findUnbind()).isOne();

        query.getNamedIndexes().get("name").bind(binding, MockMySqlParameter.INSTANCE);
        assertThat(binding.findUnbind()).isEqualTo(2);

        query.getNamedIndexes().get("age").bind(binding, MockMySqlParameter.INSTANCE);
        assertThat(binding.findUnbind()).isEqualTo(-1);

        query = Query.parse("INSERT INTO `user` (`id`, `nickname`, `real_name`) VALUE " +
            "(?, ?initName, ?initName) ON DUPLICATE KEY UPDATE " +
            "`nickname` = ?updateName, `real_name` = ?updateName");

        binding = new Binding(5);

        assertThat(binding.findUnbind()).isZero();

        binding.add(0, MockMySqlParameter.INSTANCE);
        assertThat(binding.findUnbind()).isOne();

        query.getNamedIndexes().get("initName").bind(binding, MockMySqlParameter.INSTANCE);
        assertThat(binding.findUnbind()).isEqualTo(3);

        query.getNamedIndexes().get("updateName").bind(binding, MockMySqlParameter.INSTANCE);
        assertThat(binding.findUnbind()).isEqualTo(-1);
    }

    @Test
    void selfHashCode() {
        Query query1 = Query.parse("INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?name, ?age) " +
            "ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age");
        Query query2 = Query.parse("INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?name, ?age) " +
            "ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age");

        assertThat(query1.hashCode()).isEqualTo(query2.hashCode());

        Query query3 = Query.parse("INSERT INTO `user` (`id`, `name`, `age`) VALUE (?id, ?name, ?age) " +
            "ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age");
        Query query4 = Query.parse("INSERT INTO `user` (`id`, `name`, `age`) VALUE (?id, ?name, ?age) " +
            "ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age");

        assertThat(query3.hashCode()).isEqualTo(query4.hashCode());
        assertThat(query1.hashCode()).isNotEqualTo(query3.hashCode());

        Query query5 = Query.parse("INSERT INTO `user` (`id`, `name`) VALUES (1, 'hello')");
        Query query6 = Query.parse("INSERT INTO `user` (`id`, `name`) VALUES (1, 'hello')");

        assertThat(query5.hashCode()).isEqualTo(query6.hashCode());
        assertThat(query1.hashCode()).isNotEqualTo(query5.hashCode());
        assertThat(query3.hashCode()).isNotEqualTo(query5.hashCode());

        Query query7 = Query.parse("SELECT * FROM `user ? hello` WHERE `id` = '?' AND `name` /* ??? */ " +
            "LIKE '?name' AND `good` = \"dsa?\" -- ??");
        Query query8 = Query.parse("SELECT * FROM `user ? hello` WHERE `id` = '?' AND `name` /* ??? */ " +
            "LIKE '?name' AND `good` = \"dsa?\" -- ??");

        assertThat(query7.hashCode()).isEqualTo(query8.hashCode());
        assertThat(query1.hashCode()).isNotEqualTo(query7.hashCode());
        assertThat(query3.hashCode()).isNotEqualTo(query7.hashCode());
        assertThat(query5.hashCode()).isNotEqualTo(query7.hashCode());

        Query query9 = Query.parse("SELECT * FROM `user ? hello` WHERE `id` = '?' AND " +
            "`name` LIKE '?name' AND `good` = \"dsa?\" -- ??");

        assertThat(query7.hashCode()).isNotEqualTo(query9.hashCode());
    }

    @Test
    void selfEquals() {
        Query query1 = Query.parse("INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?name, ?age) " +
            "ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age");
        Query query2 = Query.parse("INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?name, ?age) " +
            "ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age");

        assertThat(query1).isEqualTo(query2);

        query1 = Query.parse("INSERT INTO `user` (`id`, `name`, `age`) VALUE (?id, ?name, ?age) " +
            "ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age");
        query2 = Query.parse("INSERT INTO `user` (`id`, `name`, `age`) VALUE (?id, ?name, ?age) " +
            "ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age");

        assertThat(query1).isEqualTo(query2);

        query1 = Query.parse("UPDATE `user` SET `name` = ?name, `good` = ?");
        assertThat(query1).isNotEqualTo(query2);

        query1 = Query.parse("UPDATE `user` SET `name` = ?, `good` = ?, `age` = ?, `data` = ?," +
            "`content` = ?");
        assertThat(query1).isNotEqualTo(query2);

        query1 = Query.parse("INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?name, ?age) " +
            "ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age");
        assertThat(query1).isNotEqualTo(query2);

        query1 = Query.parse("INSERT INTO `user` (`id`, `name`) VALUES (1, 'hello')");
        query2 = Query.parse("INSERT INTO `user` (`id`, `name`) VALUES (1, 'hello')");

        assertThat(query1).isEqualTo(query2);

        query1 = Query.parse("SELECT * FROM `user ? hello` WHERE `id` = '?' AND " +
            "`name` LIKE '?name' AND `good` = \"dsa?\" /* ??? */ -- ??");
        query2 = Query.parse("SELECT * FROM `user ? hello` WHERE `id` = '?' AND " +
            "`name` LIKE '?name' AND `good` = \"dsa?\" /* ??? */ -- ??");

        assertThat(query1).isEqualTo(query2);
    }

    @Test
    void indexesEquals() {
        Query query1 = Query.parse("INSERT INTO `user` (`id`, `name`, `age`, `updated_at`, `created_at`) " +
            "VALUE (?id, ?name, ?age, ?now, ?now) " +
            "ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age, `updated_at` = ?now");
        Query query2 = Query.parse("INSERT INTO `user` (`id`, `name`, `age`, `updated_at`, `created_at`) " +
            "VALUE (?id, ?name, ?age, ?now, ?now) " +
            "ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age, `updated_at` = ?now");

        assertThat(query1.getNamedIndexes().get("name")).isEqualTo(query1.getNamedIndexes().get("name"));
        assertThat(query1.getNamedIndexes().get("now")).isEqualTo(query1.getNamedIndexes().get("now"));
        assertThat(query1.getNamedIndexes().get("name")).isEqualTo(query2.getNamedIndexes().get("name"));
        assertThat(query1.getNamedIndexes().get("now")).isEqualTo(query2.getNamedIndexes().get("now"));

        assertThat(query1.getNamedIndexes().get("name")).isNotEqualTo(query1.getNamedIndexes().get("age"));
        assertThat(query1.getNamedIndexes().get("name")).isNotEqualTo(query1.getNamedIndexes().get("id"));
        assertThat(query1.getNamedIndexes().get("name")).isNotEqualTo(query1.getNamedIndexes().get("now"));
    }

    private static void assertQuery(String sql) {
        Query query = Query.parse(sql);

        assertThat(query.isSimple()).isTrue();
        assertThat(query.getPartSize()).isZero();
        assertThat(query.getFormattedSize()).isEqualTo(sql.length());
        assertThat(query.getFormattedSql()).isSameAs(sql);
        assertThat(query.getParameters()).isZero();
        assertThat(query.getNamedIndexes()).isEmpty();
    }

    private static void assertQuery(String sql, String formatted, Map<String, ParameterIndex> namedIndexes,
        int parameters) {
        Query query = Query.parse(sql);

        assertThat(query.isSimple()).isFalse();
        assertThat(query.getPartSize()).isEqualTo(parameters + 1);
        assertThat(query.getFormattedSize()).isEqualTo(formatted.length());
        assertThat(query.getFormattedSql()).isEqualTo(formatted);
        assertThat(query.getParameters()).isEqualTo(parameters);
        assertThat(query.getNamedIndexes()).isEqualTo(namedIndexes);
    }

    private static ParameterIndex index(int... indexes) {
        ParameterIndex index = new ParameterIndex(indexes[0]);

        for (int i = 1, n = indexes.length; i < n; ++i) {
            index.push(indexes[i]);
        }

        return index;
    }

    private static Tuple2<String, ParameterIndex> link(String name, int... indexes) {
        if (indexes.length == 0) {
            throw new IllegalArgumentException("must has least one index");
        }

        return Tuples.of(name, index(indexes));
    }

    @SafeVarargs
    private static Map<String, ParameterIndex> mapOf(Tuple2<String, ParameterIndex>... tuples) {
        Map<String, ParameterIndex> result = new HashMap<>(((tuples.length << 2) + 2) / 3, 0.75f);

        for (Tuple2<String, ParameterIndex> tuple : tuples) {
            result.put(tuple.getT1(), tuple.getT2());
        }

        return result;
    }

    private static final class MockMySqlParameter implements MySqlParameter {

        static final MockMySqlParameter INSTANCE = new MockMySqlParameter();

        @Override
        public boolean isNull() {
            return false;
        }

        @Override
        public Mono<ByteBuf> publishBinary() {
            return Mono.error(() -> new IllegalStateException("Mock parameter, has no value"));
        }

        @Override
        public Mono<Void> publishText(ParameterWriter writer) {
            return Mono.error(() -> new IllegalStateException("Mock parameter, has no value"));
        }

        @Override
        public MySqlType getType() {
            return MySqlType.INT;
        }

        @Override
        public void dispose() {
            // No need dispose.
        }

        @Override
        public String toString() {
            return "MockParameter{}";
        }

        private MockMySqlParameter() { }
    }
}
