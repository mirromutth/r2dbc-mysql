package io.github.mirromutth.r2dbc.mysql;

import io.github.mirromutth.r2dbc.mysql.client.Client;
import io.github.mirromutth.r2dbc.mysql.codec.Codecs;
import io.github.mirromutth.r2dbc.mysql.constant.ZeroDateOption;
import io.github.mirromutth.r2dbc.mysql.internal.MySqlSession;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link MySqlBatchingBatch}.
 */
class MySqlBatchingBatchTest {

    private final Client client = mock(Client.class);

    private final Codecs codecs = mock(Codecs.class);

    private final MySqlSession session = new MySqlSession("", ZeroDateOption.USE_NULL);

    private final MySqlBatchingBatch batch = new MySqlBatchingBatch(client, codecs, session);

    @Test
    void add() {
        batch.add("");
        batch.add("INSERT INTO `test` VALUES (100)");
        batch.add("    ");
        batch.add("INSERT INTO `test` VALUES (100);");
        batch.add("INSERT INTO `test` VALUES (100);    ");
        batch.add("INSERT INTO `test` VALUES (100);    INSERT INTO `test` VALUES (100); INSERT INTO `test` VALUES (100)   ");
        batch.add("");
        batch.add("   ;   INSERT INTO `test` VALUES (100);    INSERT INTO `test` VALUES (100); INSERT INTO `test` VALUES (100);  ");

        assertEquals(batch.getSql(), ";INSERT INTO `test` VALUES (100);" +
            "    ;INSERT INTO `test` VALUES (100);" +
            "INSERT INTO `test` VALUES (100);" +
            "INSERT INTO `test` VALUES (100);    INSERT INTO `test` VALUES (100); INSERT INTO `test` VALUES (100)   ;" +
            ";   ;   INSERT INTO `test` VALUES (100);    INSERT INTO `test` VALUES (100); INSERT INTO `test` VALUES (100)");
    }
}