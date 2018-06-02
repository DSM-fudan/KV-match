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
package cn.edu.fudan.dsm.kvmatch;

import cn.edu.fudan.dsm.kvmatch.common.Pair;
import cn.edu.fudan.dsm.kvmatch.common.entity.IndexNode;
import cn.edu.fudan.dsm.kvmatch.common.entity.TimeSeriesNode;
import cn.edu.fudan.dsm.kvmatch.operator.IndexOperator;
import cn.edu.fudan.dsm.kvmatch.operator.TimeSeriesOperator;
import cn.edu.fudan.dsm.kvmatch.operator.file.IndexFileOperator;
import cn.edu.fudan.dsm.kvmatch.operator.file.TimeSeriesFileOperator;
import cn.edu.fudan.dsm.kvmatch.operator.hbase.IndexHBaseTableOperator;
import cn.edu.fudan.dsm.kvmatch.operator.hbase.TimeSeriesHBaseTableOperator;
import cn.edu.fudan.dsm.kvmatch.operator.kudu.IndexKuduTableOperator;
import cn.edu.fudan.dsm.kvmatch.operator.kudu.TimeSeriesKuduTableOperator;
import cn.edu.fudan.dsm.kvmatch.statistic.StatisticInfo;
import cn.edu.fudan.dsm.kvmatch.statistic.StatisticWriter;
import cn.edu.fudan.dsm.kvmatch.utils.IndexNodeUtils;
import cn.edu.fudan.dsm.kvmatch.utils.MeanIntervalUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

@SuppressWarnings("Duplicates")
public class DualIndexBuilder {

    private static final Logger logger = LoggerFactory.getLogger(DualIndexBuilder.class.getName());

    private static final int W = 25;

    private Iterator scanner;
    private TimeSeriesNode node = new TimeSeriesNode();
    private IndexOperator indexOperator = null;

    private int n;

    private long cnt = 0;
    private long first = -1;      // left offset, for output global position of time series
    private int dataIndex = 0;

    public static void main(String args[]) throws IOException {
        System.out.print("Data Length = ");
        Scanner scanner = new Scanner(System.in);
        int n = scanner.nextInt();
        scanner.close();

        DualIndexBuilder indexBuilder = new DualIndexBuilder(n, "file");
        long startTime = System.currentTimeMillis();
        indexBuilder.buildIndex();
        long endTime = System.currentTimeMillis();
        logger.info("Time usage: {} ms", endTime - startTime);
        StatisticWriter.print(String.valueOf(endTime - startTime) + ",");
    }

    public DualIndexBuilder(int n, String storageType) throws IOException {
        this.n = n;
        TimeSeriesOperator timeSeriesOperator = null;
        switch (storageType) {
            case "file":
                timeSeriesOperator = new TimeSeriesFileOperator(n, false);
                break;
            case "hdfs":

                break;
            case "hbase":
                timeSeriesOperator = new TimeSeriesHBaseTableOperator(n, 7, false);
                break;
            case "kudu":
                timeSeriesOperator = new TimeSeriesKuduTableOperator(n, false);
                break;
        }
        if (timeSeriesOperator != null) {
            this.scanner = timeSeriesOperator.readAllTimeSeries();
        }

        switch (storageType) {
            case "file":
                indexOperator = new IndexFileOperator("dual", n, W, true);
                break;
            case "hdfs":

                break;
            case "hbase":
                indexOperator = new IndexHBaseTableOperator("dual", n, W, true);
                break;
            case "kudu":
                indexOperator = new IndexKuduTableOperator("dual", n, W, true);
                break;
        }
    }

    private boolean nextData() throws IOException {
        if (dataIndex + 1 < node.getData().size()) {
            dataIndex++;
            return ++cnt <= n;
        } else {
            Object result = scanner.next();
            if (result != null) {
                if (result instanceof Result) {  // HBase table
                    Result result1 = (Result) result;
                    if (first == -1) {
                        first = Long.parseLong(Bytes.toString(result1.getRow()));
                    }
                    node = new TimeSeriesNode();
                    node.parseBytes(result1.getValue(Bytes.toBytes("info"), Bytes.toBytes("d")));
                    dataIndex = 0;
                } else {  // local file
                    Pair result1 = (Pair) result;
                    if (first == -1) {
                        first = (long) result1.getFirst();
                    }
                    node = (TimeSeriesNode) result1.getSecond();
                    dataIndex = 0;
                }
                return true;
            } else {
                return false;
            }
        }
    }

    private double getCurrentData() {
        return node.getData().get(dataIndex);
    }

    private void buildIndex() throws IOException {
        Double lastMeanRound = null;
        IndexNode indexNode = null;
        Map<Double, IndexNode> indexNodeMap = new HashMap<>();

        long loc = 0;
        double ex = 0;
        while (nextData()) {
            ex += getCurrentData();
            loc++;
            if (loc % W == 0) {  // a new disjoint window of data
                double mean = ex / W;
                ex = 0;

                if (loc % 1000000 == 0) {
                    logger.info("{}", loc);
                }

                double curMeanRound = MeanIntervalUtils.toRound(mean);
                logger.debug("key:{}, mean:{}, loc:{}", curMeanRound, mean, loc);

                long pos = loc / W;
                if (lastMeanRound == null || !lastMeanRound.equals(curMeanRound) || pos - indexNode.getPositions().get(indexNode.getPositions().size() - 1).getFirst() == IndexNode.MAXIMUM_DIFF-1) {
                    // put the last row
                    if (lastMeanRound != null) {
                        indexNodeMap.put(lastMeanRound, indexNode);
                    }
                    // new row
                    logger.debug("new row, rowkey: {}", curMeanRound);
                    indexNode = indexNodeMap.get(curMeanRound);
                    if (indexNode == null) {
                        indexNode = new IndexNode();
                    }
                    indexNode.getPositions().add(new Pair<>((int) pos, (int) pos));
                    lastMeanRound = curMeanRound;
                } else {
                    // use last row
                    logger.debug("use last row, rowkey: {}", lastMeanRound);
                    int index = indexNode.getPositions().size();
                    indexNode.getPositions().get(index - 1).setSecond((int) pos);
                }
            }
        }

        // put the last node
        if (indexNode != null && !indexNode.getPositions().isEmpty()) {
            indexNodeMap.put(lastMeanRound, indexNode);
        }

        // step 2: merge consecutive rows to variable-width index rows
        // get ordered statistic list and average number of disjoint window intervals
        List<Pair<Double, Pair<Integer, Integer>>> rawStatisticInfo = new ArrayList<>(indexNodeMap.size());
        StatisticInfo average = new StatisticInfo();
        for (Map.Entry entry : indexNodeMap.entrySet()) {
            IndexNode indexNode1 = (IndexNode) entry.getValue();
            rawStatisticInfo.add(new Pair<>((Double) entry.getKey(), new Pair<>(indexNode1.getPositions().size(), 0)));
            average.append(indexNode1.getPositions().size());
        }
        rawStatisticInfo.sort((o1, o2) -> -o1.getFirst().compareTo(o2.getFirst()));
        logger.debug("number of disjoint window intervals: average: {}, minimum: {}, maximum: {}", average.getAverage(), average.getMinimum(), average.getMaximum());

        // merge adjacent index nodes satisfied criterion, and put to HBase
        Map<Double, IndexNode> indexStore = new TreeMap<>();
        List<Pair<Double, Pair<Integer, Integer>>> statisticInfo = new ArrayList<>(indexNodeMap.size());
        IndexNode last = indexNodeMap.get(rawStatisticInfo.get(0).getFirst());
        for (int i = 1; i < rawStatisticInfo.size(); i++) {
            IndexNode current = indexNodeMap.get(rawStatisticInfo.get(i).getFirst());
            boolean isMerged = false;
            if (rawStatisticInfo.get(i).getSecond().getFirst() < average.getAverage() * 1.2) {
                IndexNode merged = IndexNodeUtils.mergeIndexNode(last, current);
                if (merged.getPositions().size() < (last.getPositions().size() + current.getPositions().size()) * 0.8) {
                    logger.debug("[MERGE] {} - last: {}, current: {}, merged: {}", rawStatisticInfo.get(i-1).getFirst(), last.getPositions().size(), current.getPositions().size(), merged.getPositions().size());
                    last = merged;
                    isMerged = true;
                }
            }
            if (!isMerged) {
                double key = rawStatisticInfo.get(i - 1).getFirst();
                indexStore.put(key, last);
                statisticInfo.add(new Pair<>(key, last.getStatisticInfoPair()));
                logger.info("{} - #intervals: {}, #offsets: {}", key, last.getStatisticInfoPair().getFirst(), last.getStatisticInfoPair().getSecond());

                last = current;
            }
        }
        double key = rawStatisticInfo.get(rawStatisticInfo.size() - 1).getFirst();
        indexStore.put(key, last);
        statisticInfo.add(new Pair<>(key, last.getStatisticInfoPair()));
        logger.info("{} - #intervals: {}, #offsets: {}", key, last.getStatisticInfoPair().getFirst(), last.getStatisticInfoPair().getSecond());

        indexOperator.writeAll(indexStore, statisticInfo);
    }
}
