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

import cn.edu.fudan.dsm.kvmatch.common.entity.TimeSeriesNode;
import cn.edu.fudan.dsm.kvmatch.common.entity.rowkey.TimeSeriesRowKey;
import cn.edu.fudan.dsm.kvmatch.operator.TimeSeriesOperator;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TimeSeriesKuduTableOperator implements TimeSeriesOperator {

    private static final Logger logger = LoggerFactory.getLogger(TimeSeriesKuduTableOperator.class.getName());

    private KuduTableHandler tableHandler;

    private Schema schema;

    public TimeSeriesKuduTableOperator(int N, boolean rebuild) throws IOException {
        String tableName = "kv-match_data-" + N;

        List<ColumnSchema> cols = new ArrayList<>();
        cols.add(new ColumnSchema.ColumnSchemaBuilder("time", Type.INT64).key(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("value", Type.BINARY).build());
        schema = new Schema(cols);

        // Need to set this up since we're not pre-partitioning.
        List<String> rangeKeys = new ArrayList<>();
        rangeKeys.add("time");
        CreateTableOptions options = new CreateTableOptions().setRangePartitionColumns(rangeKeys);

        tableHandler = new KuduTableHandler(tableName, schema, options, rebuild);
    }

    @Override
    public List<Double> readTimeSeries(int left, int length) throws IOException {
        List<Double> ret = new ArrayList<>(length);

        int begin = (left - 1) / TimeSeriesNode.ROW_LENGTH * TimeSeriesNode.ROW_LENGTH + 1;
        int end = (left + length - 2) / TimeSeriesNode.ROW_LENGTH * TimeSeriesNode.ROW_LENGTH + 1;

        if (begin == end) {  // `get` is faster
            TimeSeriesNode node = getTimeSeriesNode(new TimeSeriesRowKey(begin));
            int offset = (left - 1) % TimeSeriesNode.ROW_LENGTH;
            for (int i = offset; i < Math.min(offset + length, TimeSeriesNode.ROW_LENGTH); i++) {
                ret.add(node.getData().get(i));
            }
        } else {  // use `scan` instead
            PartialRow beginRow = schema.newPartialRow();
            beginRow.addLong(0, begin);
            PartialRow endRow = schema.newPartialRow();
            endRow.addLong(0, end + 1);
            KuduScanner scanner = tableHandler.getClient().newScannerBuilder(tableHandler.getTable())
                    .lowerBound(beginRow)
                    .exclusiveUpperBound(endRow)
                    .build();
            while (scanner.hasMoreRows()) {
                RowResultIterator results = scanner.nextRows();
                for (RowResult result : results) {
                    TimeSeriesNode node = new TimeSeriesNode();
                    node.parseBytes(result.getBinary(1).array());
                    int first = (int) result.getLong(0);
                    if (first == begin) {
                        int offset = (left - 1) % TimeSeriesNode.ROW_LENGTH;
                        for (int i = offset; i < Math.min(offset + length, TimeSeriesNode.ROW_LENGTH); i++) {
                            ret.add(node.getData().get(i));
                        }
                    } else if (first == end) {
                        int offset = (left + length - 2) % TimeSeriesNode.ROW_LENGTH;
                        for (int i = 0; i <= offset; i++) {
                            ret.add(node.getData().get(i));
                        }
                    } else {
                        ret.addAll(node.getData());
                    }
                }
            }
        }
        return ret;
    }

    private TimeSeriesNode getTimeSeriesNode(TimeSeriesRowKey rowKey) throws IOException {
        KuduScanner scanner = tableHandler.getClient().newScannerBuilder(tableHandler.getTable())
                .addPredicate(KuduPredicate.newComparisonPredicate(schema.getColumnByIndex(0), KuduPredicate.ComparisonOp.EQUAL, rowKey.getFirst()))
                .build();
        TimeSeriesNode node = null;
        if (scanner.hasMoreRows()) {
            RowResult result = scanner.nextRows().next();
            node = new TimeSeriesNode();
            node.parseBytes(result.getBinary(1).array());
        }
        return node;
    }

    @Override
    public Iterator readAllTimeSeries() throws IOException {
        KuduScanner scanner = tableHandler.getClient().newScannerBuilder(tableHandler.getTable()).build();
        return new TimeSeriesNodeKuduIterator(scanner);
    }

    @Override
    public void writeTimeSeriesNode(TimeSeriesRowKey rowKey, TimeSeriesNode node) throws IOException {
        logger.debug("Putting into Kudu table: {}-{}", rowKey, node);

        Insert insert = tableHandler.getTable().newInsert();
        PartialRow row = insert.getRow();
        row.addLong(0, rowKey.getFirst());
        row.addBinary(1, node.toBytes());

        tableHandler.apply(insert);
    }

    @Override
    public void close() throws IOException {
        tableHandler.close();
    }
}
