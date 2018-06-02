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

import cn.edu.fudan.dsm.kvmatch.common.entity.TimeSeriesNode;
import cn.edu.fudan.dsm.kvmatch.common.entity.rowkey.TimeSeriesRowKey;
import cn.edu.fudan.dsm.kvmatch.operator.TimeSeriesOperator;
import cn.edu.fudan.dsm.kvmatch.utils.StringUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A wrapper class to interact with the HBase data table.
 * <p>
 * Created by Jiaye Wu on 16-2-16.
 */
public class TimeSeriesHBaseTableOperator implements TimeSeriesOperator {

    private static final Logger logger = LoggerFactory.getLogger(TimeSeriesHBaseTableOperator.class.getName());

    private HBaseTableHandler tableHandler;

    public TimeSeriesHBaseTableOperator(int N, int numRegions, boolean rebuild) throws IOException {
        TableName tableName = TableName.valueOf("KVmatch:data-" + N + "-" + numRegions);
        HTableDescriptor tableDescriptors = new HTableDescriptor(tableName);
        HColumnDescriptor columnDescriptor = new HColumnDescriptor("info");
        tableDescriptors.addFamily(columnDescriptor);

        byte[][] splitKeys = new byte[numRegions - 1][];
        for (int i = 0; i < splitKeys.length; i++) {
            splitKeys[i] = Bytes.toBytes(StringUtils.toStringFixedWidth((i + 1) * (N / numRegions), TimeSeriesRowKey.ROWKEY_FIXED_WIDTH));
        }

        // avoid auto split
        tableDescriptors.setRegionSplitPolicyClassName(ConstantSizeRegionSplitPolicy.class.getName());

        tableHandler = new HBaseTableHandler(tableName, tableDescriptors, splitKeys, rebuild);
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
            Scan scan = new Scan(Bytes.toBytes(new TimeSeriesRowKey(begin).toString()), Bytes.toBytes(new TimeSeriesRowKey(end + 1).toString()));
            scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("d"));
            ResultScanner results = tableHandler.getTable().getScanner(scan);
            for (Result result : results) {
                TimeSeriesNode node = new TimeSeriesNode();
                node.parseBytes(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("d")));
                int first = Integer.parseInt(Bytes.toString(result.getRow()));
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
        return ret;
    }

    private TimeSeriesNode getTimeSeriesNode(TimeSeriesRowKey rowKey) throws IOException {
        logger.debug("Getting from HBase table: {}", rowKey);

        Get get = new Get(Bytes.toBytes(rowKey.toString()));
        get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("d"));
        Result result = tableHandler.get(get);

        TimeSeriesNode node = null;
        if (result != null) {
            node = new TimeSeriesNode();
            node.parseBytes(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("d")));
        }
        return node;
    }

    @Override
    public Iterator readAllTimeSeries() throws IOException {
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("d"));
        scan.setCaching(500);
        return tableHandler.getTable().getScanner(scan).iterator();
    }

    @Override
    public void writeTimeSeriesNode(TimeSeriesRowKey rowKey, TimeSeriesNode node) throws IOException {
        logger.debug("Putting into HBase table: {}-{}", rowKey, node);

        Put put = new Put(Bytes.toBytes(rowKey.toString()));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("d"), node.toBytes());
        tableHandler.put(put);
    }

    @Override
    public void close() throws IOException {
        tableHandler.flushWriteBuffer();
        tableHandler.close();
    }
}
