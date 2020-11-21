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

package dev.miku.r2dbc.mysql.codec;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.miku.r2dbc.mysql.json.JacksonCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Unit tests for {@link JacksonCodec}.
 */
class JacksonCodecTest implements CodecTestSupport<Object> {

    private static final ObjectMapper MAPPER = new ObjectMapper().findAndRegisterModules();

    private final Account[] accounts = {
        new Account(1, "The devil of mirror", "UNKNOWN", Arrays.asList("Human", "\r\n\0\032\\'\"\u00a5\u20a9", "Github user")),
        new Account(2, "The super man", "Good job", Collections.singletonList("Krypton")),
        new Account(3, "Nothing", "Nothing interesting", Collections.emptyList())
    };

    private final Group[] groups = {
        new Group(101, "Creature", 217389, Arrays.asList(accounts)),
        new Group(102, "Real life", 0, Collections.singletonList(accounts[0])),
        new Group(103, "\"My\" 'empty' group", 21321, Collections.emptyList())
    };

    private final Object[] everything = {
        0,
        "",
        Collections.emptyList(),
        Collections.singleton(1),
        Arrays.asList("1", 2, 3.4),
        accounts[0],
        accounts[1],
        accounts[2],
        groups[0],
        groups[1],
        groups[2]
    };

    @Override
    public Codec<Object> getCodec(ByteBufAllocator allocator) {
        return new JacksonCodec(allocator, JacksonCodec.Mode.ALL);
    }

    @Override
    public Object[] originParameters() {
        return everything;
    }

    @Override
    public Object[] stringifyParameters() {
        return Arrays.stream(everything)
            .map(it -> {
                try {
                    return MAPPER.writeValueAsString(it);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            })
            .map(it -> String.format("'%s'", ESCAPER.escape(it)))
            .toArray();
    }

    @Override
    public ByteBuf[] binaryParameters(Charset charset) {
        return Arrays.stream(everything)
            .map(it -> {
                try {
                    return Unpooled.wrappedBuffer(MAPPER.writeValueAsBytes(it));
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            })
            .toArray(ByteBuf[]::new);
    }
}

final class Group {

    private final int id;

    private final String name;

    private final int liked;

    private final List<Account> accounts;

    @JsonCreator
    Group(
        @JsonProperty("id") int id,
        @JsonProperty("name") String name,
        @JsonProperty("liked") int liked,
        @JsonProperty("accounts") List<Account> accounts
    ) {
        this.id = id;
        this.name = name;
        this.liked = liked;
        this.accounts = accounts;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public int getLiked() {
        return liked;
    }

    public List<Account> getAccounts() {
        return accounts;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Group group = (Group) o;
        return id == group.id &&
            liked == group.liked &&
            name.equals(group.name) &&
            accounts.equals(group.accounts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, liked, accounts);
    }
}

final class Account {

    private final int id;

    private final String username;

    private final String password;

    private final List<String> authorities;

    @JsonCreator
    Account(
        @JsonProperty("id") int id,
        @JsonProperty("name") String username,
        @JsonProperty("password") String password,
        @JsonProperty("authorities") List<String> authorities
    ) {
        this.id = id;
        this.username = username;
        this.password = password;
        this.authorities = authorities;
    }

    public int getId() {
        return id;
    }

    @JsonProperty("name")
    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

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
        Account account = (Account) o;
        return id == account.id &&
            username.equals(account.username) &&
            password.equals(account.password) &&
            authorities.equals(account.authorities);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, username, password, authorities);
    }
}
