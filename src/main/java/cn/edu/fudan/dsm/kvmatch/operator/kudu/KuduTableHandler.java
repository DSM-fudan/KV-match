/*
 * Copyright 2017 Jiaye Wu
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.edu.fudan.dsm.kvmatch.operator.kudu;

import org.apache.kudu.Schema;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

public class KuduTableHandler implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(KuduTableHandler.class.getName());

    private static final String KUDU_MASTER = System.getProperty("kuduMaster", "hadoop9");

    private KuduClient client;
    private KuduTable table;
    private KuduSession session;

    public KuduTableHandler(String tableName, Schema schema, CreateTableOptions options, boolean rebuild) throws IOException {
        System.out.println("-----------------------------------------------");
        System.out.println("Will try to connect to Kudu master at " + KUDU_MASTER);
        System.out.println("Run with -DkuduMaster=myHost:port to override.");
        System.out.println("-----------------------------------------------");

        client = new KuduClient.KuduClientBuilder(KUDU_MASTER)
                .defaultSocketReadTimeoutMs(86400000)  // bug: KUDU-2116
                .defaultOperationTimeoutMs(86400000)
                .build();

        if (rebuild) {
            if (client.tableExists(tableName)) {
                logger.info("Deleting Kudu table `{}`", tableName);
                client.deleteTable(tableName);
            }
            logger.info("Creating Kudu table `{}`", tableName);
            client.createTable(tableName, schema, options);
        }

        table = client.openTable(tableName);
        session = client.newSession();
        session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);  // speedup
        session.setTimeoutMillis(86400000);  // 24h: avoid timeout during index building
    }

    public KuduTable getTable() {
        return table;
    }

    public KuduClient getClient() {
        return client;
    }

    public void apply(Operation operation) throws IOException {
        session.apply(operation);
    }

    /**
     * Close the handler, after that you should not use it.
     *
     * @throws IOException if any error occurred during close process
     */
    public void close() throws IOException {
        if (session != null) {
            session.close();  // flush and close
        }
        if (client != null) {
            client.close();
        }
    }
}
