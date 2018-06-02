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
package cn.edu.fudan.dsm.kvmatch.mapreduce;

import cn.edu.fudan.dsm.kvmatch.common.Pair;
import cn.edu.fudan.dsm.kvmatch.mapreduce.common.FloatTimeSeriesNode;
import cn.edu.fudan.dsm.kvmatch.mapreduce.common.LongIndexNode;
import cn.edu.fudan.dsm.kvmatch.mapreduce.operator.FloatTimeSeriesTableOperator;
import cn.edu.fudan.dsm.kvmatch.utils.IndexNodeUtils;
import cn.edu.fudan.dsm.kvmatch.utils.MeanIntervalUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by Jiaye Wu on 17-8-31.
 */
public class BuildIndexMapReduce {

    private static final Logger logger = LoggerFactory.getLogger(BuildIndexMapReduce.class.getName());

    // \Sigma = {25, 50, 100, 200, 400}
    private static final int[] WuList = {25, 50, 75, 100, 125, 150, 175, 200, 225, 250, 275, 300, 325, 350, 375, 400};
    private static final boolean[] WuEnabled = {true, true, false, true, false, false, false, true, false, false, false, false, false, false, false, true};

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            logger.info("Usage: BuildIndexMapReduce <dataLength> [rebuild?([false]/true)] [numSampleRows(1000)]");
            System.exit(2);
        }
        long N = Long.parseLong(args[0]);
        boolean rebuild = false;
        int numSampleRows = 1000;
        if (args.length >= 2) {
            rebuild = Boolean.parseBoolean(args[1]);
        }
        if (args.length >= 3) {
            numSampleRows = Integer.parseInt(args[2]);
        }

        TableName dataTableName = TableName.valueOf("HTSI:data-" + N + "-1");
//        TableName dataTableName = TableName.valueOf("KVmatch:data-" + N + "-1");
        Connection connection = ConnectionFactory.createConnection();
        Admin admin = connection.getAdmin();
        if (!admin.tableExists(dataTableName)) {
            logger.warn("Please generate data first.");
            System.exit(1);
        }

        List<Job> jobs = new ArrayList<>();
        for (int i = 0; i < WuList.length; i++) {
            if (!WuEnabled[i]) continue;

            TableName indexTableName = TableName.valueOf("KVmatch:index-" + N + "-mr7-" + WuList[i]);
            HTableDescriptor tableDescriptors = new HTableDescriptor(indexTableName);
            HColumnDescriptor columnDescriptor = new HColumnDescriptor("std");
            tableDescriptors.addFamily(columnDescriptor);

            if (admin.tableExists(indexTableName)) {
                if (!rebuild) {
                    logger.warn("Please manually delete the index table {} before index building, or set the rebuild option to true to enforce overwrite the index table.", indexTableName);
                    System.exit(1);
                } else {
                    admin.disableTable(indexTableName);
                    admin.deleteTable(indexTableName);
                }
            }
            admin.createTable(tableDescriptors);

            Scan scan = new Scan();
            scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("d"));

            Job job = Job.getInstance();
            job.setJobName("KV-match Index Building - " + N + " (w=" + WuList[i] + ")");
            job.getConfiguration().setLong("N", N);
            job.getConfiguration().setInt("w", WuList[i]);
            job.setJarByClass(BuildIndexMapReduce.class);
            TableMapReduceUtil.initTableMapperJob(dataTableName.getNameAsString(), scan, BuildIndexMapper.class, DoubleWritable.class, LongIndexNode.class, job);
            job.setCombinerClass(BuildIndexCombiner.class);
            TableMapReduceUtil.initTableReducerJob(indexTableName.getNameAsString(), BuildIndexReducer.class, job);
            job.setNumReduceTasks(7);

            // approximately sample data rows to determine the mean value range
            Path partitionOutputPath = new Path("hdfs://hadoopCluster/wujy/_partition.lst");
            TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), partitionOutputPath);
            if (i == 0) {  // execute sampling only once
                sampleData(job, numSampleRows);
            }
            job.setPartitionerClass(TotalOrderPartitioner.class);

            job.getConfiguration().set("mapreduce.map.java.opts", "-Xmx4g -verbose:gc");
            job.getConfiguration().set("mapreduce.map.memory.mb", "5120");
            job.getConfiguration().set("mapreduce.reduce.shuffle.input.buffer.percent", "0.2");
            job.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx9g -verbose:gc");
            job.getConfiguration().set("mapreduce.reduce.memory.mb", "10240");

            jobs.add(job);
        }

        for (Job job : jobs) {
//            job.submit();
            new Thread(() -> {
                try {
                    job.waitForCompletion(true);
                } catch (IOException | InterruptedException | ClassNotFoundException e) {
                    logger.error(e.getMessage(), e.getCause());
                }
            }).start();
        }
    }

    private static void sampleData(Job job, int numSampleRows) throws IOException {
        Configuration conf = job.getConfiguration();
        long N = conf.getLong("N", 1);
        FloatTimeSeriesTableOperator timeSeriesOperator = new FloatTimeSeriesTableOperator(N, 1, false);
        int numSamples = numSampleRows * FloatTimeSeriesNode.ROW_LENGTH;

        logger.info("Sampling {} data points ...", numSamples);
        List<Float> samples = new ArrayList<>(numSamples);
        for (int i = 1; i <= numSampleRows; i++) {
            long left = ThreadLocalRandom.current().nextLong(1, N - FloatTimeSeriesNode.ROW_LENGTH);
            samples.addAll(timeSeriesOperator.readTimeSeries(left, FloatTimeSeriesNode.ROW_LENGTH));
            if (i % (numSampleRows / 10) == 0) {
                logger.info("Sampled {} data points.", samples.size());
            }
        }

        Collections.sort(samples);
        Path dst = new Path(TotalOrderPartitioner.getPartitionFile(conf));
        FileSystem fs = dst.getFileSystem(conf);
        if (fs.exists(dst)) {
            fs.delete(dst, false);
        }
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, dst, job.getMapOutputKeyClass(), NullWritable.class);
        NullWritable nullValue = NullWritable.get();
        int numPartitions = job.getNumReduceTasks();
        float stepSize = samples.size() / (float) numPartitions;
        int last = -1;
        for (int i = 1; i < numPartitions; i++) {
            int k = Math.round(stepSize * i);
            while (last >= k && Float.compare(samples.get(last), samples.get(k)) == 0) {
                k++;
            }
            logger.info("Partition split #{}: {}", i, samples.get(k));
            writer.append(new DoubleWritable(samples.get(k)), nullValue);
            last = k;
        }
        writer.close();
    }

    static class BuildIndexMapper extends TableMapper<DoubleWritable, LongIndexNode> {

        private Map<Double, LongIndexNode> indexNodeMap;
        private Double lastMeanRound = null;
        private LongIndexNode indexNode = null;
        private long N;
        private int w;

        private long regionBegin = -1;
        private FloatTimeSeriesNode lastTimeSeriesNode = null;
        private FloatTimeSeriesTableOperator timeSeriesOperator;

        @Override
        public void setup(Context context) throws IOException {
            indexNodeMap = new HashMap<>();
            N = context.getConfiguration().getLong("N", 0);
            w = context.getConfiguration().getInt("w", 50);
            timeSeriesOperator = new FloatTimeSeriesTableOperator(N, 1, false);
        }

        @Override
        public void map(ImmutableBytesWritable row, Result columns, Context context) throws IOException, InterruptedException {
            // parse data
            long first = Long.parseLong(Bytes.toString(row.get()));
            FloatTimeSeriesNode node = new FloatTimeSeriesNode();
            node.parseBytes(columns.getValue(Bytes.toBytes("info"), Bytes.toBytes("d")));

            // construct complete data
            List<Float> data = new ArrayList<>(node.getData().size());
            if (regionBegin == -1) {  // the first row of this mapper
                regionBegin = first;
                if (regionBegin > 1) {  // not the first region
                    // fetch front length-(w-1) data to avoid missing sliding windows
                    data.addAll(timeSeriesOperator.readTimeSeries(regionBegin - w + 1, w - 1));
                    first = regionBegin - w + 1;
                }
                logger.info("Region begin [{}), row length: {}", first, data.size());
            } else {
                data.addAll(lastTimeSeriesNode.getData().subList(lastTimeSeriesNode.getData().size() - w + 1, lastTimeSeriesNode.getData().size()));
                first = first - w + 1;
            }
            data.addAll(node.getData());
            lastTimeSeriesNode = node;  // leave data for next iteration of this mapper

            // calculate mean value of each sliding window
            long loc = first - 1;
            double ex = 0;
            for (int i = 0; i < w - 1; i++) {
                ex += data.get(i);
            }
            for (int i = w - 1; i < data.size(); i++) {
                ex += data.get(i);
                loc++;
                if (i - w >= 0) {
                    ex -= data.get(i - w);
                }
                double mean = ex / w;

                double curMeanRound = MeanIntervalUtils.toRound(mean);
                logger.debug("key:{}, mean:{}, loc:{}", curMeanRound, mean, loc);
                if (lastMeanRound == null || !lastMeanRound.equals(curMeanRound) || loc - indexNode.getPositions().get(indexNode.getPositions().size() - 1).getFirst() == LongIndexNode.MAXIMUM_DIFF - 1) {
                    // put the last row
                    if (lastMeanRound != null) {
                        indexNodeMap.put(lastMeanRound, indexNode);
                    }
                    // new row
                    logger.debug("new row, rowkey: {}", curMeanRound);
                    indexNode = indexNodeMap.getOrDefault(curMeanRound, new LongIndexNode());
                    indexNode.getPositions().add(new Pair<>(loc, loc));
                    lastMeanRound = curMeanRound;
                } else {
                    // use last row
                    logger.debug("use last row, rowkey: {}", lastMeanRound);
                    int index = indexNode.getPositions().size();
                    indexNode.getPositions().get(index - 1).setSecond(loc);
                }
            }

            // output to reducer and clear store
            for (Map.Entry<Double, LongIndexNode> entry : indexNodeMap.entrySet()) {
                context.write(new DoubleWritable(entry.getKey()), entry.getValue());
            }
            indexNodeMap = new HashMap<>();
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            // put the last node
            if (indexNode != null && !indexNode.getPositions().isEmpty()) {
                indexNodeMap.put(lastMeanRound, indexNode);
            }
            // output to reducer
            for (Map.Entry<Double, LongIndexNode> entry : indexNodeMap.entrySet()) {
                context.write(new DoubleWritable(entry.getKey()), entry.getValue());
            }
        }
    }

    static class BuildIndexCombiner extends Reducer<DoubleWritable, LongIndexNode, DoubleWritable, LongIndexNode> {

        @Override
        public void reduce(DoubleWritable key, Iterable<LongIndexNode> values, Context context) throws IOException, InterruptedException {
            // combine rows with the same key (mean value range)
            LongIndexNode node = new LongIndexNode();
            for (LongIndexNode node1 : values) {
                node = IndexNodeUtils.mergeIndexNode(node, node1);
            }
            context.write(key, node);
        }
    }

    static class BuildIndexReducer extends TableReducer<DoubleWritable, LongIndexNode, Writable> {

        private List<Pair<Double, Pair<Long, Long>>> statisticInfo;

        @Override
        public void setup(Context context) throws IOException {
            statisticInfo = new ArrayList<>();
        }

        @Override
        public void reduce(DoubleWritable key, Iterable<LongIndexNode> values, Context context) throws IOException, InterruptedException {
            // combine rows with the same key (mean value range)
            LongIndexNode node = new LongIndexNode();
            for (LongIndexNode node1 : values) {
                node = IndexNodeUtils.mergeIndexNode(node, node1);
            }

            Put put = new Put(MeanIntervalUtils.toBytes(key.get()));
            put.addColumn(Bytes.toBytes("std"), Bytes.toBytes("p"), node.toBytesCompact());
            context.write(new ImmutableBytesWritable(put.getRow()), put);
            statisticInfo.add(new Pair<>(key.get(), node.getStatisticInfoPair()));
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            // store statistic information for query order optimization
            if (statisticInfo.isEmpty()) {
                logger.warn("Empty reducer");
                return;
            }
            statisticInfo.sort(Comparator.comparing(Pair::getFirst));
            byte[] result = new byte[(Bytes.SIZEOF_DOUBLE + 2 * Bytes.SIZEOF_LONG) * statisticInfo.size()];
            System.arraycopy(MeanIntervalUtils.toBytes(statisticInfo.get(0).getFirst()), 0, result, 0, Bytes.SIZEOF_DOUBLE);
            System.arraycopy(Bytes.toBytes(statisticInfo.get(0).getSecond().getFirst()), 0, result, Bytes.SIZEOF_DOUBLE, Bytes.SIZEOF_LONG);
            System.arraycopy(Bytes.toBytes(statisticInfo.get(0).getSecond().getSecond()), 0, result, Bytes.SIZEOF_DOUBLE + Bytes.SIZEOF_LONG, Bytes.SIZEOF_LONG);
            for (int i = 1; i < statisticInfo.size(); i++) {
                statisticInfo.get(i).getSecond().setFirst(statisticInfo.get(i).getSecond().getFirst() + statisticInfo.get(i - 1).getSecond().getFirst());
                statisticInfo.get(i).getSecond().setSecond(statisticInfo.get(i).getSecond().getSecond() + statisticInfo.get(i - 1).getSecond().getSecond());
                System.arraycopy(MeanIntervalUtils.toBytes(statisticInfo.get(i).getFirst()), 0, result, i * (Bytes.SIZEOF_DOUBLE + 2 * Bytes.SIZEOF_LONG), Bytes.SIZEOF_DOUBLE);
                System.arraycopy(Bytes.toBytes(statisticInfo.get(i).getSecond().getFirst()), 0, result, i * (Bytes.SIZEOF_DOUBLE + 2 * Bytes.SIZEOF_LONG) + Bytes.SIZEOF_DOUBLE, Bytes.SIZEOF_LONG);
                System.arraycopy(Bytes.toBytes(statisticInfo.get(i).getSecond().getSecond()), 0, result, i * (Bytes.SIZEOF_DOUBLE + 2 * Bytes.SIZEOF_LONG) + Bytes.SIZEOF_DOUBLE + Bytes.SIZEOF_LONG, Bytes.SIZEOF_LONG);
            }
            Put put = new Put(Bytes.add(Bytes.toBytes("statistic-"), MeanIntervalUtils.toBytes(statisticInfo.get(0).getFirst())));
            put.addColumn(Bytes.toBytes("std"), Bytes.toBytes("p"), result);
            context.write(new ImmutableBytesWritable(put.getRow()), put);
        }
    }
}
