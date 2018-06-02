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

import cn.edu.fudan.dsm.kvmatch.common.Pair;
import cn.edu.fudan.dsm.kvmatch.common.entity.IndexNode;
import cn.edu.fudan.dsm.kvmatch.operator.IndexOperator;
import cn.edu.fudan.dsm.kvmatch.utils.MeanIntervalUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class IndexKuduTableOperator implements IndexOperator {

    private static final Logger logger = LoggerFactory.getLogger(IndexKuduTableOperator.class.getName());

    private static final long META_TABLE_ROW_KEY = 8888888888L;

    private KuduTableHandler tableHandler;

    private Schema schema;

    public IndexKuduTableOperator(String type, int N, int Wu, boolean rebuild) throws IOException {
        String tableName = "kv-match_index-" + (type.equals("standard") ? "" : type) + N + "-" + Wu;

        List<ColumnSchema> cols = new ArrayList<>();
        cols.add(new ColumnSchema.ColumnSchemaBuilder("mean", Type.INT64).key(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("position", Type.BINARY).build());
        schema = new Schema(cols);

        // Need to set this up since we're not pre-partitioning.
        List<String> rangeKeys = new ArrayList<>();
        rangeKeys.add("mean");
        CreateTableOptions options = new CreateTableOptions().setRangePartitionColumns(rangeKeys);

        tableHandler = new KuduTableHandler(tableName, schema, options, rebuild);
    }

    @Override
    public Map<Double, IndexNode> readIndexes(double keyFrom, double keyTo) throws IOException {
        Map<Double, IndexNode> ret = new TreeMap<>();
        PartialRow beginRow = schema.newPartialRow();
        beginRow.addLong(0, MeanIntervalUtils.toLong(keyFrom));
        PartialRow endRow = schema.newPartialRow();
        endRow.addLong(0, MeanIntervalUtils.toLong(keyTo) + 1);
        KuduScanner scanner = tableHandler.getClient().newScannerBuilder(tableHandler.getTable())
                .lowerBound(beginRow)
                .exclusiveUpperBound(endRow)
                .build();
        while (scanner.hasMoreRows()) {
            RowResultIterator results = scanner.nextRows();
            for (RowResult result : results) {
                IndexNode indexNode = new IndexNode();
                indexNode.parseBytesCompact(result.getBinary(1).array());
                ret.put(MeanIntervalUtils.toDouble(result.getLong(0)), indexNode);
            }
        }
        return ret;
    }

    @Override
    public List<Pair<Double, Pair<Integer, Integer>>> readStatisticInfo() throws IOException {
        PartialRow beginRow = schema.newPartialRow();
        beginRow.addLong(0, META_TABLE_ROW_KEY);
        PartialRow endRow = schema.newPartialRow();
        endRow.addLong(0, META_TABLE_ROW_KEY + 1);
        KuduScanner scanner = tableHandler.getClient().newScannerBuilder(tableHandler.getTable())
//                .lowerBound(beginRow)
//                .exclusiveUpperBound(endRow)
                .build();
//
//        KuduScanner scanner = tableHandler.getClient().newScannerBuilder(tableHandler.getTable())
//                .addPredicate(KuduPredicate.newComparisonPredicate(schema.getColumnByIndex(0), KuduPredicate.ComparisonOp.EQUAL, META_TABLE_ROW_KEY))
//                .build();
        List<Pair<Double, Pair<Integer, Integer>>> statisticInfo = new ArrayList<>();
        while (scanner.hasMoreRows()) {
            RowResultIterator results = scanner.nextRows();
            while (results.hasNext()) {
                RowResult result = results.next();
                System.out.println(result.getLong(0));
//                byte[] statisticData = result.getBinary(1).array();
//                byte[] tmp = new byte[Bytes.SIZEOF_DOUBLE];
//                for (int j = 0; j < statisticData.length; j += Bytes.SIZEOF_DOUBLE + 2 * Bytes.SIZEOF_INT) {
//                    System.arraycopy(statisticData, j, tmp, 0, Bytes.SIZEOF_DOUBLE);
//                    Double key = MeanIntervalUtils.toDouble(tmp);
//                    System.arraycopy(statisticData, j + Bytes.SIZEOF_DOUBLE, tmp, 0, Bytes.SIZEOF_INT);
//                    int value1 = Bytes.toInt(tmp);
//                    System.arraycopy(statisticData, j + Bytes.SIZEOF_DOUBLE + Bytes.SIZEOF_INT, tmp, 0, Bytes.SIZEOF_INT);
//                    int value2 = Bytes.toInt(tmp);
//                    statisticInfo.add(new Pair<>(key, new Pair<>(value1, value2)));
//                }
            }
        }
        return statisticInfo;
    }

    @Override
    public void writeAll(Map<Double, IndexNode> sortedIndexes, List<Pair<Double, Pair<Integer, Integer>>> statisticInfo) throws IOException {
        for (Map.Entry<Double, IndexNode> entry : sortedIndexes.entrySet()) {
            Insert insert = tableHandler.getTable().newInsert();
            PartialRow row = insert.getRow();
            row.addLong(0, MeanIntervalUtils.toLong(entry.getKey()));
            row.addBinary(1, entry.getValue().toBytesCompact());
            tableHandler.apply(insert);
        }

        // store statistic information for query order optimization
        statisticInfo.sort(Comparator.comparing(Pair::getFirst));
        byte[] result = new byte[(Bytes.SIZEOF_DOUBLE + 2 * Bytes.SIZEOF_INT) * statisticInfo.size()];
        System.arraycopy(MeanIntervalUtils.toBytes(statisticInfo.get(0).getFirst()), 0, result, 0, Bytes.SIZEOF_DOUBLE);
        System.arraycopy(Bytes.toBytes(statisticInfo.get(0).getSecond().getFirst()), 0, result, Bytes.SIZEOF_DOUBLE, Bytes.SIZEOF_INT);
        System.arraycopy(Bytes.toBytes(statisticInfo.get(0).getSecond().getSecond()), 0, result, Bytes.SIZEOF_DOUBLE + Bytes.SIZEOF_INT, Bytes.SIZEOF_INT);
        for (int i = 1; i < statisticInfo.size(); i++) {
            statisticInfo.get(i).getSecond().setFirst(statisticInfo.get(i).getSecond().getFirst() + statisticInfo.get(i - 1).getSecond().getFirst());
            statisticInfo.get(i).getSecond().setSecond(statisticInfo.get(i).getSecond().getSecond() + statisticInfo.get(i - 1).getSecond().getSecond());
            System.arraycopy(MeanIntervalUtils.toBytes(statisticInfo.get(i).getFirst()), 0, result, i * (Bytes.SIZEOF_DOUBLE + 2 * Bytes.SIZEOF_INT), Bytes.SIZEOF_DOUBLE);
            System.arraycopy(Bytes.toBytes(statisticInfo.get(i).getSecond().getFirst()), 0, result, i * (Bytes.SIZEOF_DOUBLE + 2 * Bytes.SIZEOF_INT) + Bytes.SIZEOF_DOUBLE, Bytes.SIZEOF_INT);
            System.arraycopy(Bytes.toBytes(statisticInfo.get(i).getSecond().getSecond()), 0, result, i * (Bytes.SIZEOF_DOUBLE + 2 * Bytes.SIZEOF_INT) + Bytes.SIZEOF_DOUBLE + Bytes.SIZEOF_INT, Bytes.SIZEOF_INT);
        }
        Insert insert = tableHandler.getTable().newInsert();
        PartialRow row = insert.getRow();
        row.addLong(0, META_TABLE_ROW_KEY);
        row.addBinary(1, result);
        tableHandler.apply(insert);
    }

    @Override
    public void close() throws IOException {
        tableHandler.close();
    }
}
