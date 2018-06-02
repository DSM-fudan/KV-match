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

import cn.edu.fudan.dsm.kvmatch.common.IndexCache;
import cn.edu.fudan.dsm.kvmatch.common.NormInterval;
import cn.edu.fudan.dsm.kvmatch.common.RangeQuerySegment;
import cn.edu.fudan.dsm.kvmatch.common.Pair;
import cn.edu.fudan.dsm.kvmatch.common.entity.IndexNode;
import cn.edu.fudan.dsm.kvmatch.common.Index;
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
import cn.edu.fudan.dsm.kvmatch.utils.MeanIntervalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

@SuppressWarnings("Duplicates")
public class DualQueryEngine {

    private static final Logger logger = LoggerFactory.getLogger(DualQueryEngine.class.getName());

    private static final boolean ENABLE_EARLY_TERMINATION = true;
    private static final double PHASE_2_TIME_ESTIMATE_COEFFICIENT_A = 4.0707589132278;
    private static final double PHASE_2_TIME_ESTIMATE_COEFFICIENT_B = 0.269833135638498;
    private static final double PHASE_2_TIME_ESTIMATE_INTERCEPT = 0.0;
    private static final boolean ENABLE_QUERY_REORDERING = true;
    private static final boolean ENABLE_INCREMENTAL_VISITING = true;
    private static final boolean ENABLE_STD_FILTER = true;
    private static final int MAX_SCAN_DATA_LENGTH = 40000;
    private static final int W = 25;

    private TimeSeriesOperator timeSeriesOperator = null;
    private IndexOperator indexOperator = null;
    private List<Pair<Double, Pair<Integer, Integer>>> statisticInfo;
    private List<IndexCache> indexCache;
    private int n, cntScans;

    public static void main(String args[]) throws IOException {
        Scanner scanner = new Scanner(System.in);
        System.out.print("Data Length = ");
        int n = scanner.nextInt();
        DualQueryEngine queryEngine = new DualQueryEngine(n, "file");
        StatisticWriter.println("Offset,Length,Epsilon,Alpha,Beta,T,T_1,T_2,#candidates,#answers");

        do {
            int offset, length;
            double epsilon, alpha, beta;
            do {
                System.out.print("Offset = ");
                offset = scanner.nextInt();
                if (offset > n) {
                    System.out.println("Invalid! Offset shouldn't be larger than " + n + ".");
                }
            } while (offset > n);
            if (offset <= 0) break;  // exit
            do {
                System.out.print("Length = ");
                length = scanner.nextInt();
                if (length < 2 * W - 1) {
                    System.out.println("Invalid! Length shouldn't be smaller than " + (2 * W - 1) + ".");
                }
                if (offset + length - 1 > n) {
                    System.out.println("Invalid! Offset+Length-1 shouldn't be larger than " + n + ".");
                }
            } while (length < 2 * W - 1 || offset + length - 1 > n);
            do {
                System.out.print("Epsilon = ");
                epsilon = scanner.nextDouble();
                if (epsilon < 0.0) {
                    System.out.println("Invalid! Epsilon shouldn't be smaller than 0.0.");
                }
            } while (epsilon < 0.0);
            do {
                System.out.print("Alpha = ");
                alpha = scanner.nextDouble();
                if (alpha <= 0.0) {
                    System.out.println("Invalid! Alpha should be greater than 0.0.");
                }
            } while (alpha <= 0.0);
            do {
                System.out.print("Beta = ");
                beta = scanner.nextDouble();
                if (beta == 0.0) {
                    System.out.println("Invalid! Beta should not equal to 0.0.");
                }
            } while (beta == 0.0);

            // initialization: 0-T, 1-T_1, 2-T_2, 3-#candidates, 4-#answers
            List<StatisticInfo> statisticInfos = new ArrayList<>(6);
            for (int i = 0; i < 6; i++) {
                statisticInfos.add(new StatisticInfo());
            }

            // execute the query request
            queryEngine.query(statisticInfos, offset, length, epsilon, alpha, beta);

            // output statistic information
            StatisticWriter.print(offset + "," + length + "," + epsilon + "," + alpha + "," + beta);
            for (int i = 0; i < 6; i++) {
                StatisticWriter.print(statisticInfos.get(i).getAverage() + ",");
            }
            StatisticWriter.println("");
        } while (true);
    }

    public DualQueryEngine(int n, String storageType) throws IOException {
        this.n = n;
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
        switch (storageType) {
            case "file":
                indexOperator = new IndexFileOperator("dual", n, W, false);
                break;
            case "hdfs":

                break;
            case "hbase":
                indexOperator = new IndexHBaseTableOperator("dual", n, W, false);
                break;
            case "kudu":
                indexOperator = new IndexKuduTableOperator("dual", n, W, false);
                break;
        }
        statisticInfo = indexOperator.readStatisticInfo();  // load meta table
    }

    @SuppressWarnings("unchecked")
    public boolean query(List<StatisticInfo> statistics, int offset, int length, double epsilon, double alpha, double beta) throws IOException {
        // fetch corresponding subsequence from data series
        logger.info("Query offset: {}, length: {}, epsilon: {}, alpha: {}, beta: {}", offset, length, epsilon, alpha, beta);
        List<Double> queryData = timeSeriesOperator.readTimeSeries(offset, length);
        return query(statistics, queryData, epsilon, alpha, beta);
    }

    public boolean query(List<StatisticInfo> statistics, List<Double> queryData, double epsilon, double alpha, double beta) throws IOException {
        // initialization: clear cache
        if (ENABLE_INCREMENTAL_VISITING) {
            indexCache = new ArrayList<>();
        }
        cntScans = 0;
        int queryLength = queryData.size();

        long startTime = System.currentTimeMillis();

        // Phase 0: calculate statistics for the query series
        List<RangeQuerySegment> queries = new ArrayList<>();

        // calculate mean and std of whole query series
        double ex = 0, ex2 = 0;
        for (Double value : queryData) {
            ex += value;
            ex2 += value * value;
        }
        double meanQ = ex / queryLength;
        double stdQ = Math.sqrt(ex2 / queryLength - meanQ * meanQ);
        logger.info("meanQ: {}, stdQ: {}", meanQ, stdQ);

        // sliding windows
        ex = 0;
        for (int i = 0; i < W - 1; i++) {
            ex += queryData.get(i);
        }
        for (int i = 0; i + 2 * W - 1 <= queryLength; i += W) {
            double meanMin = 1000000, meanMax = -1000000;
            for (int j = i + W - 1; j < i + 2 * W - 1; j++) {
                ex += queryData.get(j);
                if (j - W >= 0) {
                    ex -= queryData.get(j - W);
                }
                double mean = ex / W;
                meanMin = Math.min(meanMin, mean);
                meanMax = Math.max(meanMax, mean);
            }
            queries.add(new RangeQuerySegment(meanMin, meanMax, i / W + 1, getCountsFromStatisticInfo(meanMin, meanMax, epsilon, alpha, beta, meanQ, stdQ).getFirst(), W));
        }

        // optimize query order
        if (ENABLE_QUERY_REORDERING) {
            queries.sort(Comparator.comparingInt(RangeQuerySegment::getCount));
        }
        logger.info("Query order: {}", queries);

        // Phase 1: index-probing
        long startTime1 = System.currentTimeMillis();

        List<NormInterval> validPositions = new ArrayList<>();

        int lastSegment = queries.get(queries.size() - 1).getOrder();
        double lastTotalTimeUsageEstimated = Double.MAX_VALUE;
        for (int i = 0; i < queries.size(); i++) {
            RangeQuerySegment query = queries.get(i);

            logger.info("Disjoint window #{} - {} - meanMin:{} - meanMax:{}", i + 1, query.getOrder(), query.getMeanMin(), query.getMeanMax());

            int deltaW = (i == queries.size() - 1) ? 0 : (queries.get(i + 1).getOrder() - query.getOrder());// * W;

            List<NormInterval> nextValidPositions = new ArrayList<>();

            // store possible current segment
            List<NormInterval> positions = new ArrayList<>();

            // query possible rows which mean is in possible distance range of i th disjoint window
            double beginRound = 1.0 / alpha * query.getMeanMin() + (1 - 1.0 / alpha) * meanQ - beta - Math.sqrt(1.0 / (alpha * alpha) * stdQ * stdQ * epsilon * epsilon / query.getWu());
            double beginRound1 = alpha * query.getMeanMin() + (1 - alpha) * meanQ - beta - Math.sqrt(alpha * alpha * stdQ * stdQ * epsilon * epsilon / query.getWu());
            beginRound = MeanIntervalUtils.toRound(Math.min(beginRound, beginRound1), statisticInfo);

            double endRound = alpha * query.getMeanMax() + (1 - alpha) * meanQ + beta + Math.sqrt(alpha * alpha * stdQ * stdQ * epsilon * epsilon / query.getWu());
            double endRound1 = 1.0 / alpha * query.getMeanMax() + (1 - 1.0 / alpha) * meanQ + beta + Math.sqrt(1.0 / (alpha * alpha) * stdQ * stdQ * epsilon * epsilon / query.getWu());
            endRound = MeanIntervalUtils.toRound(Math.max(endRound, endRound1));

            logger.info("Scan index from {} to {}", beginRound, endRound);
            if (ENABLE_INCREMENTAL_VISITING) {
                int index_l = findCache(beginRound);
                int index_r = findCache(endRound, index_l);

                if (index_l == index_r && index_l >= 0) {
                    /*
                     * Current:          l|===|r
                     * Cache  : index_l_l|_____|index_l_r
                     * Future : index_l_l|_____|index_l_r
                     */
                    scanCache(index_l, beginRound, true, endRound, true, positions);
                } else if (index_l < 0 && index_r >= 0) {
                    /*
                     * Current:         l|_==|r
                     * Cache  :   index_r_l|_____|index_r_r
                     * Future : index_r_l|_______|index_r_r
                     */
                    scanCache(index_r, indexCache.get(index_r).getBeginRound(), true, endRound, true, positions);
                    scanIndexAndAddCache(beginRound, true, indexCache.get(index_r).getBeginRound(), false, index_r, positions);
                    indexCache.get(index_r).setBeginRound(beginRound);
                } else if (index_l >= 0 && index_r < 0) {
                    /*
                     * Current:             l|==_|r
                     * Cache  : index_l_l|_____|index_l_r
                     * Future : index_l_l|_______|index_l_r
                     */
                    scanCache(index_l, beginRound, true, indexCache.get(index_l).getEndRound(), true, positions);
                    scanIndexAndAddCache(indexCache.get(index_l).getEndRound(), false, endRound, true, index_l, positions);
                    indexCache.get(index_l).setEndRound(endRound);
                } else if (index_l == index_r && index_l < 0) {
                    /*
                     * Current:        l|___|r
                     * Cache  : |_____|       |_____|
                     * Future : |_____|l|___|r|_____|
                     */
                    scanIndexAndAddCache(beginRound, true, endRound, true, index_r, positions);  // insert a new cache node
                } else if (index_l >= 0 && index_r >= 0 && index_l + 1 == index_r) {
                    /*
                      Current:     l|=___=|r
                      Cache  : |_____|s  |_____|
                      Future : |_______________|
                     */
                    double s = indexCache.get(index_l).getEndRound();
                    scanCache(index_l, beginRound, true, s, true, positions);
                    scanIndexAndAddCache(s, false, indexCache.get(index_r).getBeginRound(), false, index_r, positions);
                    scanCache(index_r, indexCache.get(index_r).getBeginRound(), true, endRound, true, positions);
                    indexCache.get(index_r).setBeginRound(s + 0.01);
                }
            } else {
                scanIndex(beginRound, true, endRound, true, positions);
            }
            positions = sortButNotMergeIntervals(positions);
//            logger.info("position: {}", positions.toString());

            if (i == 0) {
                for (NormInterval position : positions) {
                    nextValidPositions.add(new NormInterval(position.getLeft() + deltaW, position.getRight() + deltaW, position.getEx(), position.getEx2()));
                }
            } else {
                int index1 = 0, index2 = 0;  // 1 - validPositions, 2-positions
                while (index1 < validPositions.size() && index2 < positions.size()) {
                    if (validPositions.get(index1).getRight() < positions.get(index2).getLeft()) {
                        index1++;
                    } else if (positions.get(index2).getRight() < validPositions.get(index1).getLeft()) {
                        index2++;
                    } else {
                        if (ENABLE_STD_FILTER) {
                            double sumEx = validPositions.get(index1).getEx() + positions.get(index2).getEx();
                            double sumEx2 = validPositions.get(index1).getEx2() + positions.get(index2).getEx2();
                            double mean = sumEx / (i + 1);  // w_i are identical, so they are omitted to avoid exceeding type limit

                            double std2 = 0;
                            if (mean > meanQ + beta) {
                                double newValue = meanQ + beta - (mean - meanQ - beta) * (i + 1) * W / (queryLength - (i + 1) * 1.0 * W);
                                mean = meanQ + beta;
                                std2 = (sumEx2 * 1.0 * W + (queryLength - (i + 1) * 1.0 * W) * newValue * newValue) / queryLength - mean * mean;
                            }

                            if (validPositions.get(index1).getRight() < positions.get(index2).getRight()) {
                                if (Double.compare(std2, alpha * alpha * stdQ * stdQ) <= 0) {
                                    nextValidPositions.add(new NormInterval(Math.max(validPositions.get(index1).getLeft(), positions.get(index2).getLeft()) + deltaW, validPositions.get(index1).getRight() + deltaW, sumEx, sumEx2));
                                }
                                index1++;
                            } else {
                                if (Double.compare(std2, alpha * alpha * stdQ * stdQ) <= 0) {
                                    nextValidPositions.add(new NormInterval(Math.max(validPositions.get(index1).getLeft(), positions.get(index2).getLeft()) + deltaW, positions.get(index2).getRight() + deltaW, sumEx, sumEx2));
                                }
                                index2++;
                            }
                        } else {
                            if (validPositions.get(index1).getRight() < positions.get(index2).getRight()) {
                                nextValidPositions.add(new NormInterval(Math.max(validPositions.get(index1).getLeft(), positions.get(index2).getLeft()) + deltaW, validPositions.get(index1).getRight() + deltaW, 0, 0));
                                index1++;
                            } else {
                                nextValidPositions.add(new NormInterval(Math.max(validPositions.get(index1).getLeft(), positions.get(index2).getLeft()) + deltaW, positions.get(index2).getRight() + deltaW, 0, 0));
                                index2++;
                            }
                        }
                    }
                }
            }

            Pair<List<NormInterval>, Pair<Integer, Integer>> candidates = sortButNotMergeIntervalsAndCount(nextValidPositions);
            validPositions = candidates.getFirst();
//            logger.info("next valid: {}", validPositions.toString());

            int cntCurrentDisjointCandidateWindows = candidates.getSecond().getFirst();
            long cntCurrentCandidateOffsets = candidates.getSecond().getSecond() * W;
            logger.info("Disjoint candidate windows: {}, candidate offsets: {}", cntCurrentDisjointCandidateWindows, cntCurrentCandidateOffsets);

            int step1TimeUsageUntilNow = (int) (System.currentTimeMillis() - startTime);
            double step2TimeUsageEstimated = PHASE_2_TIME_ESTIMATE_COEFFICIENT_A * cntCurrentDisjointCandidateWindows + PHASE_2_TIME_ESTIMATE_COEFFICIENT_B * cntCurrentCandidateOffsets / 100000 * queryLength + PHASE_2_TIME_ESTIMATE_INTERCEPT;
            double totalTimeUsageEstimated = step1TimeUsageUntilNow + step2TimeUsageEstimated;
            logger.info("Time usage: step 1 until now: {}, step 2 estimated: {}, total estimated: {}", step1TimeUsageUntilNow, step2TimeUsageEstimated, totalTimeUsageEstimated);

            if (i >= 5 && ENABLE_EARLY_TERMINATION && totalTimeUsageEstimated > lastTotalTimeUsageEstimated) {
                lastSegment = (i == queries.size() - 1) ? query.getOrder() : queries.get(i + 1).getOrder();
                break;
            }
            lastTotalTimeUsageEstimated = totalTimeUsageEstimated;
        }
        if (!ENABLE_EARLY_TERMINATION) {
            lastSegment = queries.get(queries.size() - 1).getOrder();
        }

        // merge consecutive intervals to shrink data size and alleviate scan times
        validPositions = sortAndMergeIntervals(validPositions);

        long endTime1 = System.currentTimeMillis();

        // Phase 2: Post Processing
        long startTime2 = System.currentTimeMillis();

        List<Pair<Integer, Double>> answers = new ArrayList<>();
        int cntCandidate = 0;

        // do z-normalization on query data
        double[] zQ = new double[queryLength];
        for (int i = 0; i < queryLength; i++) {
            zQ[i] = (queryData.get(i) - meanQ) / stdQ;
        }
        // sort the query data
        int[] order = new int[queryLength];
        Index[] Q_tmp = new Index[queryLength];
        for (int i = 0; i < queryLength; i++) {
            Q_tmp[i] = new Index(zQ[i], i);
        }
        Arrays.sort(Q_tmp, (o1, o2) -> Double.compare(Math.abs(o2.value), Math.abs(o1.value)));
        for (int i = 0; i < queryLength; i++) {
            zQ[i] = Q_tmp[i].value;
            order[i] = Q_tmp[i].index;
        }

        int idx = 0;
        while (idx < validPositions.size()) {
            int beginIdx = idx, endIdx = idx;

            int begin = (validPositions.get(idx).getLeft() - (lastSegment - 1) - 1) * W + 1 - W + 1;
            int end = (validPositions.get(idx).getRight() - (lastSegment - 1) - 1) * W + 1 + queryLength - 1;
            if (begin < 1) begin = 1;
            int length = end - begin + 1;
            idx++;

            while (idx < validPositions.size()) {
                begin = (validPositions.get(idx).getLeft() - (lastSegment - 1) - 1) * W + 1 - W + 1;
                int newLength = length + begin - end - 1;
                end = (validPositions.get(idx).getRight() - (lastSegment - 1) - 1) * W + 1 + queryLength - 1;
                if (end > n) end = n;
                newLength += end - begin + 1;
                if (newLength > MAX_SCAN_DATA_LENGTH) break;
                endIdx = idx;
                length = newLength;
                idx++;
            }

            begin = (validPositions.get(beginIdx).getLeft() - (lastSegment - 1) - 1) * W + 1 - W + 1;
            end = (validPositions.get(endIdx).getRight() - (lastSegment - 1) - 1) * W + 1 + queryLength - 1;
            if (begin < 1) begin = 1;
            if (end > n) end = n;
            logger.debug("Scan data [{}, {}]", begin, end);
            List<Double> data = timeSeriesOperator.readTimeSeries(begin, end - begin + 1);

            for (int idx1 = beginIdx; idx1 <= endIdx; idx1++) {
                cntCandidate += (validPositions.get(idx1).getRight() - validPositions.get(idx1).getLeft() + 1) * W;

                ex = ex2 = 0;
                double[] T = new double[2 * queryLength];

                int begin1 = (validPositions.get(idx1).getLeft() - (lastSegment - 1) - 1) * W + 1 - W + 1 - begin;
                int end1 = (validPositions.get(idx1).getRight() - (lastSegment - 1) - 1) * W + 1 + queryLength - 1 - begin;
                if (begin1 < 0) begin1 = 0;
                if (end1 > data.size() - 1) end1 = data.size() - 1;
//                logger.info("{} - {}", begin1 + begin, end1 + begin);
                for (int i = begin1; i <= end1; i++) {
                    double d = data.get(i);
                    ex += d;
                    ex2 += d * d;
                    T[i % queryLength] = d;
                    T[(i % queryLength) + queryLength] = d;

                    if (i >= queryLength - 1) {
                        // the current starting location of T
                        int j = (i + 1) % queryLength;

                        // z-normalization of T will be calculated on the fly
                        double mean = ex / queryLength;
                        double std = Math.sqrt(ex2 / queryLength - mean * mean);

                        if (Math.abs(mean - meanQ) <= beta && (std / stdQ) <= alpha && (std / stdQ) >= 1.0 / alpha) {
                            // calculate ED distance & test single point range criterion
                            double dist = 0;
                            for (int k = 0; k < queryLength && dist <= epsilon * epsilon; k++) {
                                double x = (T[(order[k] + j)] - mean) / std;
                                dist += (x - zQ[k]) * (x - zQ[k]);
                            }
                            if (dist <= epsilon * epsilon) {
                                answers.add(new Pair<>(begin + i - queryLength + 1, Math.sqrt(dist)));
                            }
                        }

                        ex -= T[j];
                        ex2 -= T[j] * T[j];
                    }
                }
            }
        }

        long endTime2 = System.currentTimeMillis();
        statistics.get(0).append(endTime2 - startTime);   // total time usage
        statistics.get(1).append(endTime1 - startTime1);  // phase 1 time usage
        statistics.get(2).append(endTime2 - startTime2);  // phase 2 time usage
        statistics.get(3).append(cntCandidate);           // number of candidates
        statistics.get(4).append(answers.size());         // number of answers
        statistics.get(5).append(cntScans);               // number of scans executed

        answers.sort(Comparator.comparing(Pair::getSecond));

        if (!answers.isEmpty()) {
            logger.info("Best: {}, distance: {}", answers.get(0).getFirst(), answers.get(0).getSecond());
        }
        logger.info("T: {} ms, T_1: {} ms, T_2: {} ms, #candidates: {}, #answers: {}", endTime2 - startTime, endTime1 - startTime1, endTime2 - startTime2, cntCandidate, answers.size());
        return !answers.isEmpty();
    }

    private Pair<Integer, Integer> getCountsFromStatisticInfo(double meanMin, double meanMax, double epsilon, double alpha, double beta, double meanQ, double stdQ) {
        double beginRound = 1.0/alpha * meanMin + (1 - 1.0/alpha) * meanQ - beta - Math.sqrt(1.0/(alpha*alpha) * stdQ*stdQ * epsilon*epsilon / W);
        double beginRound1 = alpha * meanMin + (1 - alpha) * meanQ - beta - Math.sqrt(alpha*alpha * stdQ*stdQ * epsilon*epsilon / W);
        beginRound = MeanIntervalUtils.toRound(Math.min(beginRound, beginRound1));

        double endRound = alpha * meanMax + (1 - alpha) * meanQ + beta + Math.sqrt(alpha*alpha * stdQ*stdQ * epsilon*epsilon / W);
        double endRound1 = 1.0/alpha * meanMax + (1 - 1.0/alpha) * meanQ + beta + Math.sqrt(1.0/(alpha*alpha) * stdQ*stdQ * epsilon*epsilon / W);
        endRound = MeanIntervalUtils.toRound(Math.max(endRound, endRound1));

        int index = Collections.binarySearch(statisticInfo, new Pair<>(beginRound, 0), Comparator.comparing(Pair::getFirst));
        index = index < 0 ? -(index + 1) : index;
        if (index >= statisticInfo.size()) index = statisticInfo.size() - 1;
        int lower1 = index > 0 ? statisticInfo.get(index - 1).getSecond().getFirst() : 0;
        int lower2 = index > 0 ? statisticInfo.get(index - 1).getSecond().getSecond() : 0;

        index = Collections.binarySearch(statisticInfo, new Pair<>(endRound, 0), Comparator.comparing(Pair::getFirst));
        index = index < 0 ? -(index + 1) : index;
        if (index >= statisticInfo.size()) index = statisticInfo.size() - 1;
        int upper1 = index > 0 ? statisticInfo.get(index).getSecond().getFirst() : 0;
        int upper2 = index > 0 ? statisticInfo.get(index).getSecond().getSecond() : 0;

        return new Pair<>(upper1 - lower1, upper2 - lower2);
    }

    private void scanIndex(double begin, boolean beginInclusive, double end, boolean endInclusive, List<NormInterval> positions) throws IOException {
        if (!beginInclusive) begin = begin + 0.01;
        if (endInclusive) end = end + 0.01;

        Map<Double, IndexNode> indexes = indexOperator.readIndexes(begin, end);
        cntScans++;
        for (Map.Entry<Double, IndexNode> entry : indexes.entrySet()) {
            double meanRound = entry.getKey();
            double meanRound2 = meanRound < 0 ? MeanIntervalUtils.toUpper(meanRound, statisticInfo) : meanRound;
            for (Pair<Integer, Integer> position : entry.getValue().getPositions()) {
                positions.add(new NormInterval(position.getFirst(), position.getSecond(), meanRound, meanRound2 * meanRound2));
            }
        }
    }

    private void scanIndexAndAddCache(double begin, boolean beginInclusive, double end, boolean endInclusive, int index, List<NormInterval> positions) throws IOException {
        if (index < 0) {
            index = -index - 1;
            indexCache.add(index, new IndexCache(begin, end));
        }

        if (!beginInclusive) begin = begin + 0.01;
        if (endInclusive) end = end + 0.01;

        Map<Double, IndexNode> indexes = indexOperator.readIndexes(begin, end);
        cntScans++;
        for (Map.Entry<Double, IndexNode> entry : indexes.entrySet()) {
            double meanRound = entry.getKey();
            double meanRound2 = meanRound < 0 ? MeanIntervalUtils.toUpper(meanRound, statisticInfo) : meanRound;
            for (Pair<Integer, Integer> position : entry.getValue().getPositions()) {
                positions.add(new NormInterval(position.getFirst(), position.getSecond(), meanRound, meanRound2 * meanRound2));
            }

            indexCache.get(index).addCache(meanRound, entry.getValue());
        }
    }

    private void scanCache(int index, double begin, boolean beginInclusive, double end, boolean endInclusive, List<NormInterval> positions) {
        for (Map.Entry<Double, IndexNode> entry : indexCache.get(index).getCaches().subMap(begin, beginInclusive, end, endInclusive).entrySet()) {
            double meanRound = entry.getKey();
            double meanRound2 = meanRound < 0 ? MeanIntervalUtils.toUpper(meanRound, statisticInfo) : meanRound;
            IndexNode indexNode = entry.getValue();
            for (Pair<Integer, Integer> position : indexNode.getPositions()) {
                positions.add(new NormInterval(position.getFirst(), position.getSecond(), meanRound, meanRound2 * meanRound2));
            }
        }
    }

    private int findCache(double round) {
        return findCache(round, 0);
    }

    private int findCache(double round, int first) {
        if (first < 0) {
            first = -first - 1;
        }

        for (int i = first; i < indexCache.size(); i++) {
            IndexCache cache = indexCache.get(i);
            if (cache.getBeginRound() > round) {
                return -i - 1;
            }
            if (cache.getBeginRound() <= round && cache.getEndRound() >= round) {
                return i;
            }
        }

        return -1;
    }

    private List<NormInterval> sortButNotMergeIntervals(List<NormInterval> intervals) {
        if (intervals.size() <= 1) {
            return intervals;
        }

        intervals.sort(Comparator.comparingInt(NormInterval::getLeft));

        NormInterval first = intervals.get(0);
        int start = first.getLeft();
        int end = first.getRight();
        double ex = first.getEx();
        double ex2 = first.getEx2();

        List<NormInterval> result = new ArrayList<>();

        for (int i = 1; i < intervals.size(); i++) {
            NormInterval current = intervals.get(i);
            if (current.getLeft() - 1 < end || (current.getLeft() - 1 == end && Double.compare(current.getEx(), ex) == 0 && Double.compare(current.getEx2(), ex2) == 0)) {
                end = Math.max(current.getRight(), end);
                ex = Math.min(current.getEx(), ex);
                ex2 = Math.min(current.getEx2(), ex2);
            } else {
                result.add(new NormInterval(start, end, ex, ex2));
                start = current.getLeft();
                end = current.getRight();
                ex = current.getEx();
                ex2 = current.getEx2();
            }
        }
        result.add(new NormInterval(start, end, ex, ex2));

        return result;
    }

    private Pair<List<NormInterval>, Pair<Integer, Integer>> sortButNotMergeIntervalsAndCount(List<NormInterval> intervals) {
        if (intervals.size() <= 1) {
            return new Pair<>(intervals, new Pair<>(intervals.size(), intervals.isEmpty() ? 0 : (intervals.get(0).getRight() - intervals.get(0).getLeft() + 1)));
        }

        intervals.sort(Comparator.comparingInt(NormInterval::getLeft));

        NormInterval first = intervals.get(0);
        int start = first.getLeft();
        int end = first.getRight();
        double ex = first.getEx();
        double ex2 = first.getEx2();

        List<NormInterval> result = new ArrayList<>();

        int cntDisjointIntervals = intervals.size();
        int cntOffsets = 0;
        for (int i = 1; i < intervals.size(); i++) {
            NormInterval current = intervals.get(i);

            if (current.getLeft() - 1 <= end) {  // count for disjoint intervals to estimate time usage of phase 2
                cntDisjointIntervals--;
            }

            if (current.getLeft() - 1 < end || (current.getLeft() - 1 == end && Double.compare(current.getEx(), ex) == 0 && Double.compare(current.getEx2(), ex2) == 0)) {
                end = Math.max(current.getRight(), end);
                ex = Math.min(current.getEx(), ex);
                ex2 = Math.min(current.getEx2(), ex2);
            } else {
                result.add(new NormInterval(start, end, ex, ex2));
                cntOffsets += end - start + 1;
                start = current.getLeft();
                end = current.getRight();
                ex = current.getEx();
                ex2 = current.getEx2();
            }
        }
        result.add(new NormInterval(start, end, ex, ex2));
        cntOffsets += end - start + 1;

        return new Pair<>(result, new Pair<>(cntDisjointIntervals, cntOffsets));
    }

    private List<NormInterval> sortAndMergeIntervals(List<NormInterval> intervals) {
        if (intervals.size() <= 1) {
            return intervals;
        }

        intervals.sort(Comparator.comparingInt(NormInterval::getLeft));

        NormInterval first = intervals.get(0);
        int start = first.getLeft();
        int end = first.getRight();
        double ex = first.getEx();
        double ex2 = first.getEx2();

        List<NormInterval> result = new ArrayList<>();

        for (int i = 1; i < intervals.size(); i++) {
            NormInterval current = intervals.get(i);
            if (current.getLeft() - 1 <= end) {
                end = Math.max(current.getRight(), end);
                ex = Math.min(current.getEx(), ex);
                ex2 = Math.min(current.getEx2(), ex2);
            } else {
                result.add(new NormInterval(start, end, ex, ex2));
                start = current.getLeft();
                end = current.getRight();
                ex = current.getEx();
                ex2 = current.getEx2();
            }
        }
        result.add(new NormInterval(start, end, ex, ex2));

        return result;
    }
}
