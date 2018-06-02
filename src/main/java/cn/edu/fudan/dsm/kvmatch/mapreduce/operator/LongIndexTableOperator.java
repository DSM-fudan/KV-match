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
package cn.edu.fudan.dsm.kvmatch.mapreduce.operator;

import cn.edu.fudan.dsm.kvmatch.common.Pair;
import cn.edu.fudan.dsm.kvmatch.mapreduce.common.LongIndexNode;
import cn.edu.fudan.dsm.kvmatch.operator.hbase.HBaseTableHandler;
import cn.edu.fudan.dsm.kvmatch.utils.MeanIntervalUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * A wrapper class to interact with the HBase index table.
 * <p>
 * Created by Jiaye Wu on 16-3-16.
 */
public class LongIndexTableOperator {

    private static final Logger logger = LoggerFactory.getLogger(LongIndexTableOperator.class.getName());

    private HBaseTableHandler tableHandler;

    public LongIndexTableOperator(long N, int Wu, boolean rebuild) throws IOException {
        TableName tableName = TableName.valueOf("KVmatch:index-" + N + "-mr7-" + Wu);
        HTableDescriptor tableDescriptors = new HTableDescriptor(tableName);
        HColumnDescriptor columnDescriptor = new HColumnDescriptor("std");
        tableDescriptors.addFamily(columnDescriptor);

        tableHandler = new HBaseTableHandler(tableName, tableDescriptors, null, rebuild);
    }

    public Map<Double, LongIndexNode> readIndexes(double keyFrom, double keyTo) throws IOException {
        Map<Double, LongIndexNode> ret = new TreeMap<>();
        Scan scan = new Scan(MeanIntervalUtils.toBytes(keyFrom), MeanIntervalUtils.toBytes(keyTo));
        try (ResultScanner results = tableHandler.getTable().getScanner(scan)) {
            for (Result result : results) {
                LongIndexNode indexNode = new LongIndexNode();
                indexNode.parseBytesCompact(result.getValue(Bytes.toBytes("std"), Bytes.toBytes("p")));
                ret.put(MeanIntervalUtils.toDouble(result.getRow()), indexNode);
            }
        }
        return ret;
    }

    public List<Pair<Double, Pair<Long, Long>>> readStatisticInfo() throws IOException {
        Scan scan = new Scan(Bytes.toBytes("statistic-"), Bytes.toBytes("statistic."));  // include all rows begin with 'statistic-'
        ResultScanner results = tableHandler.getTable().getScanner(scan);

        List<Pair<Double, Pair<Long, Long>>> statisticInfo = new ArrayList<>();
        long lastValue1 = 0, lastValue2 = 0;
        for (Result result : results) {
            logger.info("{} - lastValue1: {}, lastValue2: {}", Bytes.toString(result.getRow()), lastValue1, lastValue2);
            byte[] statisticData = result.getValue(Bytes.toBytes("std"), Bytes.toBytes("p"));
            long value1 = 0, value2 = 0;
            byte[] tmp = new byte[Bytes.SIZEOF_DOUBLE];
            for (int j = 0; j < statisticData.length; j += Bytes.SIZEOF_DOUBLE + 2 * Bytes.SIZEOF_LONG) {
                System.arraycopy(statisticData, j, tmp, 0, Bytes.SIZEOF_DOUBLE);
                Double key = MeanIntervalUtils.toDouble(tmp);
                System.arraycopy(statisticData, j + Bytes.SIZEOF_DOUBLE, tmp, 0, Bytes.SIZEOF_LONG);
                value1 = Bytes.toLong(tmp) + lastValue1;
                System.arraycopy(statisticData, j + Bytes.SIZEOF_DOUBLE + Bytes.SIZEOF_LONG, tmp, 0, Bytes.SIZEOF_LONG);
                value2 = Bytes.toLong(tmp) + lastValue2;
                statisticInfo.add(new Pair<>(key, new Pair<>(value1, value2)));
            }
            lastValue1 = value1;
            lastValue2 = value2;
        }
        return statisticInfo;
    }

    public void close() throws IOException {
        tableHandler.flushWriteBuffer();
        tableHandler.close();
    }
}
