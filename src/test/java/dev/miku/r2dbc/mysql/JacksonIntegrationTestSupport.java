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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.miku.r2dbc.mysql.json.JacksonCodecRegistrar;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Base class considers integration tests for jackson codec.
 */
abstract class JacksonIntegrationTestSupport extends IntegrationTestSupport {

    private static final String TDL = "CREATE TEMPORARY TABLE test" +
        "(id INT PRIMARY KEY AUTO_INCREMENT,value JSON)";

    private static final Foo[] FOO = {
        new Foo("Smart", Arrays.asList(1, 2, 3), Collections.singletonList("Human")),
        new Foo("Fool", Collections.emptyList(), Collections.singletonList("Human")),
    };

    private static final Bar[] BARS = {
        new Bar("Earth", Arrays.asList(FOO)),
        new Bar("Mars", Collections.emptyList())
    };

    JacksonIntegrationTestSupport(MySqlConnectionConfiguration configuration) {
        super(configuration);
    }

    @BeforeEach
    void setUp() {
        JacksonCodecRegistrar.setUp();
    }

    @AfterEach
    void tearDown() {
        JacksonCodecRegistrar.tearDown();
    }

    @DisabledIfSystemProperty(named = "test.mysql.version", matches = "5\\.[56](\\.\\d+)?")
    @Test
    void json() {
        create().flatMap(connection -> Mono.from(connection.createStatement(TDL).execute())
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .thenMany(insert(connection))
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .thenMany(connection.createStatement("SELECT value FROM test WHERE id > ?")
                .bind(0, 0)
                .execute())
            .concatMap(r -> r.map((row, meta) -> row.get(0, Bar.class)))
            .collectList())
            .as(StepVerifier::create)
            .expectNext(Arrays.asList(BARS))
            .verifyComplete();
    }

    private static Publisher<MySqlResult> insert(MySqlConnection connection) {
        MySqlStatement statement = connection.createStatement("INSERT INTO test VALUES (DEFAULT, ?)");

        for (Bar bar : BARS) {
            statement.bind(0, bar).add();
        }

        return statement.execute();
    }
}

final class Foo {

    private final String name;

    private final List<Integer> priorities;

    private final List<String> authorities;

    @JsonCreator
    Foo(@JsonProperty("n") String name, @JsonProperty("p") List<Integer> priorities,
        @JsonProperty("a") List<String> authorities) {
        this.name = name;
        this.priorities = priorities;
        this.authorities = authorities;
    }

    @JsonProperty("n")
    public String getName() {
        return name;
    }

    @JsonProperty("p")
    public List<Integer> getPriorities() {
        return priorities;
    }

    @JsonProperty("a")
    public List<String> getAuthorities() {
        return authorities;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Foo foo = (Foo) o;
        return name.equals(foo.name) &&
            priorities.equals(foo.priorities) &&
            authorities.equals(foo.authorities);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, priorities, authorities);
    }

    @Override
    public String toString() {
        return "Foo{" +
            "name='" + name + '\'' +
            ", priorities=" + priorities +
            ", authorities=" + authorities +
            '}';
    }
}

final class Bar {

    private final String name;

    private final List<Foo> foo;

    @JsonCreator
    Bar(@JsonProperty("name") String name, @JsonProperty("foo") List<Foo> foo) {
        this.name = name;
        this.foo = foo;
    }

    public String getName() {
        return name;
    }

    public List<Foo> getFoo() {
        return foo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Bar bar = (Bar) o;
        return name.equals(bar.name) &&
            foo.equals(bar.foo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, foo);
    }

    @Override
    public String toString() {
        return "Bar{" +
            "name='" + name + '\'' +
            ", foo=" + foo +
            '}';
    }
}
