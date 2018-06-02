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
package cn.edu.fudan.dsm.kvmatch.operator.hbase;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * A base class to maintain a HBase connection.
 * <p>
 * Created by Jiaye Wu on 16-2-16.
 */
public class HBaseTableHandler implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(HBaseTableHandler.class.getName());

    private Connection connection = null;
    private Admin admin = null;
    private Table table = null;
    private BufferedMutator bufferedMutator = null;
    private RegionLocator regionLocator = null;
    private TableName tableName = null;

    private boolean writeBuffer = true;

    /**
     * Initialize the connection and make sure the table is available
     *
     * @param tableName        the table to handle
     * @param tableDescriptors the description of the table
     * @param initialSplitKeys initial split the table based on the row keys to balance
     * @param rebuild          whether to purge the table data
     * @throws IOException
     */
    public HBaseTableHandler(TableName tableName, HTableDescriptor tableDescriptors, byte[][] initialSplitKeys, boolean rebuild) throws IOException {
        this.tableName = tableName;

        connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
        admin = connection.getAdmin();

        if (rebuild) {
            createNamespaceIfNotExists(tableName.getNamespaceAsString());
            createTableIfNotExists(tableDescriptors, initialSplitKeys);
        }

        table = connection.getTable(tableName);
        if (writeBuffer) {
            bufferedMutator = connection.getBufferedMutator(tableName);
        }

        regionLocator = connection.getRegionLocator(tableName);
    }

    public boolean isWriteBuffer() {
        return writeBuffer;
    }

    public void setWriteBuffer(boolean writeBuffer) {
        this.writeBuffer = writeBuffer;
    }

    public Connection getConnection() {
        return connection;
    }

    public Table getTable() {
        return table;
    }

    public BufferedMutator getBufferedMutator() {
        return bufferedMutator;
    }

    public RegionLocator getRegionLocator() {
        return regionLocator;
    }

    private void createNamespaceIfNotExists(String namespace) throws IOException {
        logger.trace("Begin checking HBase Namespace `{}`", namespace);
        try {
            admin.listTableNamesByNamespace(namespace);
        } catch (NamespaceNotFoundException e) {
            logger.info("Creating HBase Namespace `{}`", namespace);
            admin.createNamespace(NamespaceDescriptor.create(namespace).build());
        }
        logger.trace("Finish checking HBase Namespace `{}`", namespace);
    }

    private void createTableIfNotExists(HTableDescriptor tableDescriptors, byte[][] initialSplitKeys) throws IOException {
        deleleTableIfExists();

        if (!admin.tableExists(tableName)) {
            logger.info("Creating HBase table `{}`", tableName);
            if (initialSplitKeys != null) {
                admin.createTable(tableDescriptors, initialSplitKeys);
            } else {
                admin.createTable(tableDescriptors);
            }
        }

        if (!admin.isTableEnabled(tableName)) {
            logger.info("Enabling HBase table `{}`", tableName);
            admin.enableTable(tableName);
        }
        logger.trace("Finish checking HBase table `{}`", tableName);
    }

    /**
     * Call the BufferedMutator's flush() to ensure all data has been written into HBase before further test
     * Write buffer should be enabled
     */
    public void flushWriteBuffer() throws IOException {
        if (writeBuffer) {
            bufferedMutator.flush();
        }
    }

    public void put(Put put) throws IOException {
        if (writeBuffer) {
            bufferedMutator.mutate(put);
        } else {
            table.put(put);
        }
    }

    public void put(List<Put> puts) throws IOException {
        if (writeBuffer) {
            bufferedMutator.mutate(puts);
        } else {
            table.put(puts);
        }
    }

    public Result get(Get get) throws IOException {
        return table.get(get);
    }

    private void deleleTableIfExists() throws IOException {
        logger.trace("Begin checking HBase table `{}`", tableName);
        if (admin.tableExists(tableName)) {
            if (!admin.isTableDisabled(tableName)) {
                logger.info("Disabling HBase table `{}`", tableName);
                admin.disableTable(tableName);
            }
            logger.info("Deleting HBase table `{}`", tableName);
            admin.deleteTable(tableName);
        }
        logger.trace("Finish checking HBase table `{}`", tableName);
    }

    private void purgeTableIfExists() throws IOException {
        deleleTableIfExists();

        // purge namespace if there is no table in it
        logger.trace("Begin checking HBase Namespace `{}`", tableName.getNamespaceAsString());
        if (admin.listTableNamesByNamespace(tableName.getNamespaceAsString()).length == 0) {
            logger.info("Deleting HBase Namespace `{}`", tableName.getNamespaceAsString());
            admin.deleteNamespace(tableName.getNamespaceAsString());
        }
        logger.trace("Finish checking HBase Namespace `{}`", tableName.getNamespaceAsString());
    }

    /**
     * Close the handler, after that you should not use it.
     *
     * @throws IOException if any error occurred during close process
     */
    public void close() throws IOException {
        if (bufferedMutator != null) {
            bufferedMutator.close();
        }
        if (table != null) {
            table.close();
        }
        if (admin != null) {
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}
