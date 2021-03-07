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

import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.TransactionDefinition;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link MySqlTransactionDefinition}.
 */
class MySqlTransactionDefinitionTest {

    @Test
    void builder() {
        Duration lockWaitTimeout = Duration.ofSeconds(118);
        Long sessionId = 123456789L;
        MySqlTransactionDefinition definition = MySqlTransactionDefinition.builder()
            .isolationLevel(IsolationLevel.READ_COMMITTED)
            .lockWaitTimeout(lockWaitTimeout)
            .withConsistentSnapshot(true)
            .consistentSnapshotEngine(ConsistentSnapshotEngine.ROCKSDB)
            .consistentSnapshotFromSession(sessionId)
            .build();

        assertThat(definition.getAttribute(TransactionDefinition.ISOLATION_LEVEL))
            .isSameAs(IsolationLevel.READ_COMMITTED);
        assertThat(definition.getAttribute(TransactionDefinition.LOCK_WAIT_TIMEOUT))
            .isSameAs(lockWaitTimeout);
        assertThat(definition.getAttribute(MySqlTransactionDefinition.WITH_CONSISTENT_SNAPSHOT))
            .isTrue();
        assertThat(definition.getAttribute(MySqlTransactionDefinition.CONSISTENT_SNAPSHOT_ENGINE))
            .isSameAs(ConsistentSnapshotEngine.ROCKSDB);
        assertThat(definition.getAttribute(MySqlTransactionDefinition.CONSISTENT_SNAPSHOT_FROM_SESSION))
            .isSameAs(sessionId);
    }

    @Test
    void mutate() {
        Duration lockWaitTimeout = Duration.ofSeconds(118);
        MySqlTransactionDefinition def1 = MySqlTransactionDefinition.builder()
            .isolationLevel(IsolationLevel.SERIALIZABLE)
            .lockWaitTimeout(lockWaitTimeout)
            .readOnly(true)
            .build();
        MySqlTransactionDefinition def2 = def1.mutate()
            .isolationLevel(IsolationLevel.READ_COMMITTED)
            .build();

        assertThat(def1).isNotEqualTo(def2);
        assertThat(def1.getAttribute(TransactionDefinition.LOCK_WAIT_TIMEOUT))
            .isSameAs(def2.getAttribute(TransactionDefinition.LOCK_WAIT_TIMEOUT))
            .isSameAs(lockWaitTimeout);
        assertThat(def1.getAttribute(TransactionDefinition.ISOLATION_LEVEL))
            .isSameAs(IsolationLevel.SERIALIZABLE);
        assertThat(def2.getAttribute(TransactionDefinition.ISOLATION_LEVEL))
            .isSameAs(IsolationLevel.READ_COMMITTED);
        assertThat(def1.getAttribute(TransactionDefinition.READ_ONLY))
            .isSameAs(def2.getAttribute(TransactionDefinition.READ_ONLY))
            .isEqualTo(true);
    }
}
