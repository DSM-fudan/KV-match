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

/**
 * Index builder for KV-index
 * <p>
 * Created by Jiaye Wu on 16-8-9.
 */
@SuppressWarnings("Duplicates")
public class IndexBuilder {

    private static final Logger logger = LoggerFactory.getLogger(IndexBuilder.class.getName());

    // \Sigma = {25, 50, 100, 200, 400}
    private static final int[] WuList = {25, 50, 75, 100, 125, 150, 175, 200, 225, 250, 275, 300, 325, 350, 375, 400};
    private static final boolean[] WuEnabled = {true, true, false, true, false, false, false, true, false, false, false, false, false, false, false, true};

    private TimeSeriesOperator timeSeriesOperator = null;
    private IndexOperator[] indexOperators = new IndexOperator[WuList.length];
    private int n;

    public IndexBuilder(int n, String storageType) throws IOException {
        this.n = n;
        switch (storageType) {
            case "file":
                timeSeriesOperator = new TimeSeriesFileOperator(n, false);
                break;
            case "hbase":
                timeSeriesOperator = new TimeSeriesHBaseTableOperator(n, 7, false);
                break;
            case "kudu":
                timeSeriesOperator = new TimeSeriesKuduTableOperator(n, false);
                break;
        }
        for (int i = 0; i < WuList.length; i++) {
            if (!WuEnabled[i]) continue;
            switch (storageType) {
                case "file":
                    indexOperators[i] = new IndexFileOperator("standard", n, WuList[i], true);
                    break;
                case "hbase":
                    indexOperators[i] = new IndexHBaseTableOperator("standard", n, WuList[i], true);
                    break;
                case "kudu":
                    indexOperators[i] = new IndexKuduTableOperator("standard", n, WuList[i], true);
                    break;
            }
        }
    }

    public static void main(String[] args) throws IOException {
        System.out.print("Data Length = ");
        Scanner scanner = new Scanner(System.in);
        int n = scanner.nextInt();
        scanner.close();

        IndexBuilder indexBuilder = new IndexBuilder(n, "file");
        indexBuilder.buildIndexes();
    }

    private void buildIndexes() {
        long startTime = System.currentTimeMillis();

        // TODO: naive -> generate tables together
        for (int i = 0; i < WuList.length; i++) {
            if (!WuEnabled[i]) continue;
            try {
                Iterator scanner = timeSeriesOperator.readAllTimeSeries();
                SingleIndexBuilder builder = new SingleIndexBuilder(n, WuList[i], scanner, indexOperators[i]);
                long startTime1 = System.currentTimeMillis();
                builder.run();
                long endTime1 = System.currentTimeMillis();
                logger.info("w = {}, time usage: {} ms", WuList[i], endTime1 - startTime1);
                StatisticWriter.print((endTime1 - startTime1) + ",");
            } catch (IOException e) {
                logger.error(e.getMessage(), e.getCause());
            }
        }

        long endTime = System.currentTimeMillis();
        logger.info("Total time usage: {} ms", endTime - startTime);
        StatisticWriter.print((endTime - startTime) + ",");
    }

    private class SingleIndexBuilder {

        long first = -1;  // left offset, for output global position of time series
        Iterator scanner;
        IndexOperator indexOperator;

        double[] t;  // data array and query array

        double d;
        double ex, ex2, mean, std;
        int n, w, cnt = 0;
        double[] buffer;

        // For every EPOCH points, all cumulative values, such as ex (sum), ex2 (sum square), will be restarted for reducing the floating point error.
        int EPOCH = 100000;

        TimeSeriesNode node = new TimeSeriesNode();
        int dataIndex = 0;

        SingleIndexBuilder(int n, int w, Iterator scanner, IndexOperator indexOperator) {
            this.scanner = scanner;
            this.w = w;
            this.n = n;

            t = new double[w * 2];
            buffer = new double[EPOCH];

            this.indexOperator = indexOperator;
        }

        boolean nextData() {
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

        double getCurrentData() {
            return node.getData().get(dataIndex);
        }

        void run() throws IOException {
            boolean done = false;
            int it = 0, ep;

            Double lastMeanRound = null;
            IndexNode indexNode = null;
            Map<Double, IndexNode> indexNodeMap = new HashMap<>();

            // step 1: fixed-width index rows
            while (!done) {
                // Read first w-1 points
                if (it == 0) {
                    for (int k = 0; k < w - 1; k++) {
                        if (nextData()) {
                            d = getCurrentData();
                            buffer[k] = d;
                        }
                    }
                } else {
                    for (int k = 0; k < w - 1; k++) {
                        buffer[k] = buffer[EPOCH - w + 1 + k];
                    }
                }

                // Read buffer of size EPOCH or when all data has been read.
                ep = w - 1;
                while (ep < EPOCH) {
                    if (nextData()) {
                        d = getCurrentData();
                        buffer[ep] = d;
                        ep++;
                    } else {
                        break;
                    }
                }

                // Data are read in chunk of size EPOCH.
                // When there is nothing to read, the loop is end.
                if (ep <= w - 1) {
                    done = true;
                } else {
                    // Just for printing a dot for approximate a million point. Not much accurate.
                    if (it % (1000000 / (EPOCH - w + 1)) == 0) {
                        System.out.print(".");
                    }

                    // Do main task here..
                    ex = 0;
                    ex2 = 0;
                    for (int i = 0; i < ep; i++) {
                        // A bunch of data has been read and pick one of them at a time to use
                        d = buffer[i];

                        // Calculate sum and sum square
                        ex += d;
                        ex2 += d * d;

                        // t is a circular array for keeping current data
                        t[i % w] = d;

                        // Double the size for avoiding using modulo "%" operator
                        t[(i % w) + w] = d;

                        // Start the task when there are more than m-1 points in the current chunk
                        if (i >= w - 1) {
                            mean = ex / w;
                            std = ex2 / w;
                            std = Math.sqrt(std - mean * mean);

                            // compute the start location of the data in the current circular array, t
                            int j = (i + 1) % w;

                            // store the mean and std for current chunk
                            long loc = (it) * (EPOCH - w + 1) + i - w + 1 + 1;
                            if (loc > n) {
                                done = true;
                                break;
                            }

                            double curMeanRound = MeanIntervalUtils.toRound(mean);
                            logger.debug("mean:{}({}), std:{}, loc:{}", mean, curMeanRound, std, loc);

                            if (lastMeanRound == null || !lastMeanRound.equals(curMeanRound) || loc - indexNode.getPositions().get(indexNode.getPositions().size() - 1).getFirst() == IndexNode.MAXIMUM_DIFF - 1) {
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
                                indexNode.getPositions().add(new Pair<>((int) loc, (int) loc));
                                lastMeanRound = curMeanRound;
                            } else {
                                // use last row
                                logger.debug("use last row, rowkey: {}", lastMeanRound);
                                int index = indexNode.getPositions().size();
                                indexNode.getPositions().get(index - 1).setSecond((int) loc);
                            }

                            // Reduce obsolete points from sum and sum square
                            ex -= t[j];
                            ex2 -= t[j] * t[j];
                        }
                    }

                    // If the size of last chunk is less then EPOCH, then no more data and terminate.
                    if (ep < EPOCH) {
                        done = true;
                    } else {
                        it++;
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
                        logger.debug("[MERGE] {} - last: {}, current: {}, merged: {}", rawStatisticInfo.get(i - 1).getFirst(), last.getPositions().size(), current.getPositions().size(), merged.getPositions().size());
                        last = merged;
                        isMerged = true;
                    }
                }
                if (!isMerged) {
                    double key = rawStatisticInfo.get(i - 1).getFirst();
                    indexStore.put(key, last);
                    statisticInfo.add(new Pair<>(key, last.getStatisticInfoPair()));

                    last = current;
                }
            }
            double key = rawStatisticInfo.get(rawStatisticInfo.size() - 1).getFirst();
            indexStore.put(key, last);
            statisticInfo.add(new Pair<>(key, last.getStatisticInfoPair()));

            indexOperator.writeAll(indexStore, statisticInfo);
        }
    }
}
