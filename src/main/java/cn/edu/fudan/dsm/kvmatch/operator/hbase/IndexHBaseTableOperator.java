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

import cn.edu.fudan.dsm.kvmatch.common.Pair;
import cn.edu.fudan.dsm.kvmatch.common.entity.IndexNode;
import cn.edu.fudan.dsm.kvmatch.operator.IndexOperator;
import cn.edu.fudan.dsm.kvmatch.utils.MeanIntervalUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * A wrapper class to interact with the HBase index table.
 * <p>
 * Created by Jiaye Wu on 16-3-16.
 */
public class IndexHBaseTableOperator implements IndexOperator {

    private static final Logger logger = LoggerFactory.getLogger(IndexHBaseTableOperator.class.getName());

    private HBaseTableHandler tableHandler;

    public IndexHBaseTableOperator(String type, int N, int Wu, boolean rebuild) throws IOException {
        TableName tableName = TableName.valueOf("HTSI:index-" + (type.equals("standard") ? "" : type) + N + "-1-" + Wu);
        HTableDescriptor tableDescriptors = new HTableDescriptor(tableName);
        HColumnDescriptor columnDescriptor = new HColumnDescriptor("std");
        tableDescriptors.addFamily(columnDescriptor);

        tableHandler = new HBaseTableHandler(tableName, tableDescriptors, null, rebuild);
    }

    @Override
    public Map<Double, IndexNode> readIndexes(double keyFrom, double keyTo) throws IOException {
        Map<Double, IndexNode> ret = new TreeMap<>();
        Scan scan = new Scan(MeanIntervalUtils.toBytes(keyFrom), MeanIntervalUtils.toBytes(keyTo));
        try (ResultScanner results = tableHandler.getTable().getScanner(scan)) {
            for (Result result : results) {
                IndexNode indexNode = new IndexNode();
                indexNode.parseBytesCompact(result.getValue(Bytes.toBytes("std"), Bytes.toBytes("p")));
                ret.put(MeanIntervalUtils.toDouble(result.getRow()), indexNode);
            }
        }
        return ret;
    }

    @Override
    public List<Pair<Double, Pair<Integer, Integer>>> readStatisticInfo() throws IOException {
        byte[] statisticData = tableHandler.getTable().get(new Get(Bytes.toBytes("statistic"))).getValue(Bytes.toBytes("std"), Bytes.toBytes("p"));
        List<Pair<Double, Pair<Integer, Integer>>> statisticInfo = new ArrayList<>(statisticData.length / (Bytes.SIZEOF_DOUBLE + 2 * Bytes.SIZEOF_INT));
        byte[] tmp = new byte[Bytes.SIZEOF_DOUBLE];
        for (int j = 0; j < statisticData.length; j += Bytes.SIZEOF_DOUBLE + 2 * Bytes.SIZEOF_INT) {
            System.arraycopy(statisticData, j, tmp, 0, Bytes.SIZEOF_DOUBLE);
            Double key = MeanIntervalUtils.toDouble(tmp);
            System.arraycopy(statisticData, j + Bytes.SIZEOF_DOUBLE, tmp, 0, Bytes.SIZEOF_INT);
            int value1 = Bytes.toInt(tmp);
            System.arraycopy(statisticData, j + Bytes.SIZEOF_DOUBLE + Bytes.SIZEOF_INT, tmp, 0, Bytes.SIZEOF_INT);
            int value2 = Bytes.toInt(tmp);
            statisticInfo.add(new Pair<>(key, new Pair<>(value1, value2)));
        }
        return statisticInfo;
    }

    @Override
    public void writeAll(Map<Double, IndexNode> sortedIndexes, List<Pair<Double, Pair<Integer, Integer>>> statisticInfo) throws IOException {
        List<Put> puts = new ArrayList<>(sortedIndexes.size());
        for (Map.Entry<Double, IndexNode> entry : sortedIndexes.entrySet()) {
            Put put = new Put(MeanIntervalUtils.toBytes(entry.getKey()));
            put.addColumn(Bytes.toBytes("std"), Bytes.toBytes("p"), entry.getValue().toBytesCompact());
            puts.add(put);
        }
        tableHandler.put(puts);

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
        Put put = new Put(Bytes.toBytes("statistic"));
        put.addColumn(Bytes.toBytes("std"), Bytes.toBytes("p"), result);
        tableHandler.put(put);
    }

    @Override
    public void close() throws IOException {
        tableHandler.flushWriteBuffer();
        tableHandler.close();
    }
}
