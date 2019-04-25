/*
 * Copyright 2019 the original author or authors.
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

package io.github.mirromutth.r2dbc.mysql;

import io.github.mirromutth.r2dbc.mysql.client.Client;
import io.github.mirromutth.r2dbc.mysql.config.ConnectProperties;
import io.github.mirromutth.r2dbc.mysql.message.frontend.FrontendMessage;
import io.github.mirromutth.r2dbc.mysql.message.frontend.PingMessage;
import io.github.mirromutth.r2dbc.mysql.message.frontend.SimpleQueryMessage;
import io.r2dbc.spi.IsolationLevel;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link MySqlConnection}.
 */
class MySqlConnectionTest {

    Client client = mock(Client.class);

    ConnectProperties properties = mock(ConnectProperties.class);

    MySqlConnection connection = new MySqlConnection(this.client, this.properties);

    ArgumentCaptor<FrontendMessage> captor = ArgumentCaptor.forClass(FrontendMessage.class);

    @Test
    void shouldStartTransaction() {

        when(this.client.exchange(any())).thenReturn(Flux.empty());

        this.connection.beginTransaction()
            .as(StepVerifier::create)
            .verifyComplete();

        verify(this.client, times(2)).exchange(this.captor.capture());

        List<FrontendMessage> allValues = this.captor.getAllValues();

        SimpleQueryMessage autoCommit = (SimpleQueryMessage) allValues.get(0);
        assertEquals("SET autocommit=0", autoCommit.getSql());

        SimpleQueryMessage begin = (SimpleQueryMessage) allValues.get(1);
        assertEquals("START TRANSACTION", begin.getSql());
    }

    @Test
    void shouldNotDisableAutoCommitIfTransactionWasStarted() {

        when(this.client.exchange(any())).thenReturn(Flux.empty());

        this.connection.beginTransaction()
            .as(StepVerifier::create)
            .verifyComplete();

        this.connection.beginTransaction()
            .as(StepVerifier::create)
            .verifyComplete();

        verify(this.client, times(3)).exchange(this.captor.capture());

        List<FrontendMessage> allValues = this.captor.getAllValues();

        SimpleQueryMessage autoCommit = (SimpleQueryMessage) allValues.get(0);
        assertEquals("SET autocommit=0", autoCommit.getSql());

        SimpleQueryMessage begin1 = (SimpleQueryMessage) allValues.get(1);
        assertEquals("START TRANSACTION", begin1.getSql());

        SimpleQueryMessage begin2 = (SimpleQueryMessage) allValues.get(2);
        assertEquals("START TRANSACTION", begin2.getSql());
    }

    @Test
    void shouldCommitTransaction() {

        when(this.client.exchange(any())).thenReturn(Flux.empty());

        this.connection.commitTransaction()
            .as(StepVerifier::create)
            .verifyComplete();

        verify(this.client).exchange(this.captor.capture());

        SimpleQueryMessage commit = (SimpleQueryMessage) this.captor.getValue();
        assertEquals("COMMIT", commit.getSql());
    }

    @Test
    void shouldRollbackTransaction() {

        when(this.client.exchange(any())).thenReturn(Flux.empty());

        this.connection.rollbackTransaction()
            .as(StepVerifier::create)
            .verifyComplete();

        verify(this.client).exchange(this.captor.capture());

        SimpleQueryMessage rollback = (SimpleQueryMessage) this.captor.getValue();
        assertEquals("ROLLBACK", rollback.getSql());
    }

    @Test
    void shouldRejectInvalidSavepoint() {

        assertThrows(IllegalArgumentException.class, () -> this.connection.createSavepoint(null));
        assertThrows(IllegalArgumentException.class, () -> this.connection.createSavepoint(""));
        assertThrows(IllegalArgumentException.class, () -> this.connection.createSavepoint("`"));
    }

    @Test
    void shouldCreateSavepoint() {

        when(this.client.exchange(any())).thenReturn(Flux.empty());

        this.connection.createSavepoint("foo")
            .as(StepVerifier::create)
            .verifyComplete();

        verify(this.client).exchange(this.captor.capture());

        SimpleQueryMessage savepoint = (SimpleQueryMessage) this.captor.getValue();
        assertEquals("SAVEPOINT `foo`", savepoint.getSql());
    }

    @Test
    void shouldReleaseSavepoint() {

        when(this.client.exchange(any())).thenReturn(Flux.empty());

        this.connection.releaseSavepoint("foo")
            .as(StepVerifier::create)
            .verifyComplete();

        verify(this.client).exchange(this.captor.capture());

        SimpleQueryMessage savepoint = (SimpleQueryMessage) this.captor.getValue();
        assertEquals("RELEASE SAVEPOINT `foo`", savepoint.getSql());
    }

    @Test
    void shouldRollbackToSavepoint() {

        when(this.client.exchange(any())).thenReturn(Flux.empty());

        this.connection.rollbackTransactionToSavepoint("foo")
            .as(StepVerifier::create)
            .verifyComplete();

        verify(this.client).exchange(this.captor.capture());

        SimpleQueryMessage savepoint = (SimpleQueryMessage) this.captor.getValue();
        assertEquals("ROLLBACK TO SAVEPOINT `foo`", savepoint.getSql());
    }

    @Test
    void shouldSetTransactionIsolationLevel() {

        when(this.client.exchange(any())).thenReturn(Flux.empty());

        this.connection.setTransactionIsolationLevel(IsolationLevel.READ_COMMITTED)
            .as(StepVerifier::create)
            .verifyComplete();

        verify(this.client).exchange(this.captor.capture());

        SimpleQueryMessage isolation = (SimpleQueryMessage) this.captor.getValue();
        assertEquals("SET TRANSACTION ISOLATION LEVEL READ COMMITTED", isolation.getSql());
    }

    @Test
    void shouldPingServer() {

        when(this.client.exchange(any())).thenReturn(Flux.empty());

        this.connection.ping()
            .as(StepVerifier::create)
            .verifyComplete();

        verify(this.client).exchange(PingMessage.getInstance());
    }
}
