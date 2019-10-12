package dev.miku.r2dbc.mysql;

import dev.miku.r2dbc.mysql.client.Client;
import dev.miku.r2dbc.mysql.codec.Codecs;
import dev.miku.r2dbc.mysql.constant.ZeroDateOption;
import dev.miku.r2dbc.mysql.util.ConnectionContext;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link MySqlBatchingBatch}.
 */
class MySqlBatchingBatchTest {

    private static final Client CLIENT = mock(Client.class);

    private static final Codecs CODECS = mock(Codecs.class);

    @Test
    void add() {
        MySqlBatchingBatch batch = new MySqlBatchingBatch(CLIENT, CODECS, new ConnectionContext(ZeroDateOption.USE_NULL));

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