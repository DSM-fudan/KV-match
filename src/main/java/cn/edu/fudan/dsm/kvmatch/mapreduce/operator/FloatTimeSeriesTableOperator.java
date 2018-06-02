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

import cn.edu.fudan.dsm.kvmatch.common.entity.rowkey.TimeSeriesRowKey;
import cn.edu.fudan.dsm.kvmatch.mapreduce.common.FloatTimeSeriesNode;
import cn.edu.fudan.dsm.kvmatch.operator.hbase.HBaseTableHandler;
import cn.edu.fudan.dsm.kvmatch.utils.StringUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
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
 * Created by Jiaye Wu on 17-8-16.
 */
public class FloatTimeSeriesTableOperator {

    private static final Logger logger = LoggerFactory.getLogger(FloatTimeSeriesTableOperator.class.getName());

    private HBaseTableHandler tableHandler;

    public FloatTimeSeriesTableOperator(long N, int numRegions, boolean rebuild) throws IOException {
//        TableName tableName = TableName.valueOf("KVmatch:data-" + N + "-" + numRegions);
        TableName tableName = TableName.valueOf("HTSI:data-" + N + "-" + numRegions);  // TODO: rename for publication
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

    public List<Float> readTimeSeries(long left, int length) throws IOException {
        List<Float> ret = new ArrayList<>(length);

        long begin = (left - 1) / FloatTimeSeriesNode.ROW_LENGTH * FloatTimeSeriesNode.ROW_LENGTH + 1;
        long end = (left + length - 2) / FloatTimeSeriesNode.ROW_LENGTH * FloatTimeSeriesNode.ROW_LENGTH + 1;

        if (begin == end) {  // `get` is faster
            FloatTimeSeriesNode node = getTimeSeriesNode(new TimeSeriesRowKey(begin));
            int offset = (int) ((left - 1) % FloatTimeSeriesNode.ROW_LENGTH);
            for (int i = offset; i < Math.min(offset + length, FloatTimeSeriesNode.ROW_LENGTH); i++) {
                ret.add(node.getData().get(i));
            }
        } else {  // use `scan` instead
            Scan scan = new Scan(Bytes.toBytes(new TimeSeriesRowKey(begin).toString()), Bytes.toBytes(new TimeSeriesRowKey(end + 1).toString()));
            scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("d"));
            ResultScanner results = tableHandler.getTable().getScanner(scan);
            for (Result result : results) {
                FloatTimeSeriesNode node = new FloatTimeSeriesNode();
                node.parseBytes(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("d")));
                long first = Long.parseLong(Bytes.toString(result.getRow()));
                if (first == begin) {
                    int offset = (int) ((left - 1) % FloatTimeSeriesNode.ROW_LENGTH);
                    for (int i = offset; i < Math.min(offset + length, FloatTimeSeriesNode.ROW_LENGTH); i++) {
                        ret.add(node.getData().get(i));
                    }
                } else if (first == end) {
                    int offset = (int) ((left + length - 2) % FloatTimeSeriesNode.ROW_LENGTH);
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

    private FloatTimeSeriesNode getTimeSeriesNode(TimeSeriesRowKey rowKey) throws IOException {
        logger.debug("Getting from HBase table: {}", rowKey);

        Get get = new Get(Bytes.toBytes(rowKey.toString()));
        get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("d"));
        Result result = tableHandler.get(get);

        FloatTimeSeriesNode node = null;
        if (result != null) {
            node = new FloatTimeSeriesNode();
            node.parseBytes(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("d")));
        }
        return node;
    }

    public Iterator readAllTimeSeries() throws IOException {
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("d"));
        scan.setCaching(500);
        return tableHandler.getTable().getScanner(scan).iterator();
    }

    public void close() throws IOException {
        tableHandler.flushWriteBuffer();
        tableHandler.close();
    }
}
