/*
 * Copyright 2018 Jiaye Wu
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
import cn.edu.fudan.dsm.kvmatch.common.Pair;
import cn.edu.fudan.dsm.kvmatch.common.RangeQuerySegment;
import cn.edu.fudan.dsm.kvmatch.common.entity.IndexNode;
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
import cn.edu.fudan.dsm.kvmatch.utils.DtwUtils;
import cn.edu.fudan.dsm.kvmatch.common.Index;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Query engine for KV-index_{DP} with normalization under DTW
 * <p>
 * Created by Jiaye Wu on 18-1-10.
 */
@SuppressWarnings("Duplicates")
public class NormQueryEngineDtw {

    private static final Logger logger = LoggerFactory.getLogger(NormQueryEngineDtw.class.getName());

    // \Sigma = {25, 50, 100, 200, 400}
    private static final int[] WuList = {25, 50, 75, 100, 125, 150, 175, 200, 225, 250, 275, 300, 325, 350, 375, 400};
    private static final boolean[] WuEnabled = {true, true, false, true, false, false, false, true, false, false, false, false, false, false, false, true};

    private static final boolean ENABLE_EARLY_TERMINATION = true;
    private static final double PHASE_2_TIME_ESTIMATE_COEFFICIENT_A = 9.72276547123376;
    private static final double PHASE_2_TIME_ESTIMATE_COEFFICIENT_B = 0.0106737255022236;
    private static final double PHASE_2_TIME_ESTIMATE_INTERCEPT = 0.0;
    private static final boolean ENABLE_QUERY_REORDERING = true;
    private static final boolean ENABLE_INCREMENTAL_VISITING = true;
    private static final boolean ENABLE_STD_FILTER = true;
    private static final int MAX_SCAN_DATA_LENGTH = 40000;

    private TimeSeriesOperator timeSeriesOperator = null;
    private IndexOperator[] indexOperators = new IndexOperator[WuList.length];
    private List<List<Pair<Double, Pair<Integer, Integer>>>> statisticInfos = new ArrayList<>(WuList.length);
    private List<List<IndexCache>> indexCaches = new ArrayList<>(WuList.length);
    private int n, cntScans;
    private double[] prefixSumsU, prefixSumsL;
    private double[][] cost;
    private int[][] cost2;

    public static void main(String args[]) throws IOException {
        Scanner scanner = new Scanner(System.in);
        System.out.print("Data Length = ");
        int n = scanner.nextInt();
        NormQueryEngineDtw queryEngine = new NormQueryEngineDtw(n, "file");
        StatisticWriter.println("Offset,Length,Epsilon,Alpha,Beta,T,T_1,T_2,#candidates,#answers");

        do {
            int offset, length, rho;
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
                if (length < WuList[0]) {
                    System.out.println("Invalid! Length shouldn't be smaller than " + WuList[0] + ".");
                }
                if (offset + length - 1 > n) {
                    System.out.println("Invalid! Offset+Length-1 shouldn't be larger than " + n + ".");
                }
            } while (length < WuList[0] || offset + length - 1 > n);
            do {
                System.out.print("Epsilon = ");
                epsilon = scanner.nextDouble();
                if (epsilon < 0.0) {
                    System.out.println("Invalid! Epsilon shouldn't be smaller than 0.0.");
                }
            } while (epsilon < 0.0);
            do {
                System.out.print("Rho (|Q|%) = ");
                double r = scanner.nextDouble();
                if (r <= 1) {
                    rho = (int) Math.floor(r * length);
                } else {
                    rho = (int) Math.floor(r);
                }
                if (rho < 0.0) {
                    System.out.println("Invalid! Rho shouldn't be smaller than 0.0.");
                }
            } while (rho < 0.0);
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
            queryEngine.query(statisticInfos, offset, length, epsilon, rho, alpha, beta);

            // output statistic information
            StatisticWriter.print(offset + "," + length + "," + epsilon + "," + rho + "," + alpha + "," + beta);
            for (int i = 0; i < 6; i++) {
                StatisticWriter.print(statisticInfos.get(i).getAverage() + ",");
            }
            StatisticWriter.println("");
        } while (true);
    }

    public NormQueryEngineDtw(int n, String storageType) throws IOException {
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
        for (int i = 0; i < WuList.length; i++) {
            if (!WuEnabled[i]) continue;
            switch (storageType) {
                case "file":
                    indexOperators[i] = new IndexFileOperator("standard", n, WuList[i], false);
                    break;
                case "hdfs":

                    break;
                case "hbase":
                    indexOperators[i] = new IndexHBaseTableOperator("standard", n, WuList[i], false);
                    break;
                case "kudu":
                    indexOperators[i] = new IndexKuduTableOperator("standard", n, WuList[i], false);
                    break;
            }

        }
        loadMetaTable();
    }

    @SuppressWarnings("unchecked")
    public boolean query(List<StatisticInfo> statistics, int offset, int length, double epsilon, int rho, double alpha, double beta) throws IOException {
        // fetch corresponding subsequence from data series
        logger.info("Query offset: {}, length: {}, epsilon: {}, rho: {}, alpha: {}, beta: {}", offset, length, epsilon, rho, alpha, beta);
        List<Double> queryData = timeSeriesOperator.readTimeSeries(offset, length);
        return query(statistics, queryData, epsilon, rho, alpha, beta);
    }

    public boolean query(List<StatisticInfo> statistics, List<Double> queryData, double epsilon, int rho, double alpha, double beta) throws IOException {
        // initialization: clear cache
        if (ENABLE_INCREMENTAL_VISITING) {
            indexCaches.clear();
            for (int ignored : WuList) {
                indexCaches.add(new ArrayList<>());
            }
        }
        cntScans = 0;
        int queryLength = queryData.size();

        long startTime = System.currentTimeMillis();

        // Phase 0: calculate statistics for the query series
        // calculate mean and std of whole query series
        double ex = 0, ex2 = 0;
        for (Double value : queryData) {
            ex += value;
            ex2 += value * value;
        }
        double meanQ = ex / queryLength;
        double stdQ = Math.sqrt(ex2 / queryLength - meanQ * meanQ);
        logger.info("meanQ: {}, stdQ: {}", meanQ, stdQ);
        // dynamic programming
        List<RangeQuerySegment> queries = determineQueryPlan(queryData, epsilon, rho, alpha, beta, meanQ, stdQ);
        logger.info("Query order: {}", queries);

        // Phase 1: index-probing
        long startTime1 = System.currentTimeMillis();

        List<NormInterval> validPositions = new ArrayList<>();  // CS

        int lastSegment = queries.get(queries.size() - 1).getOrder();
        double lastTotalTimeUsageEstimated = Double.MAX_VALUE;
        int preLength = 0;
        for (int i = 0; i < queries.size(); i++) {
            RangeQuerySegment query = queries.get(i);
            logger.info("Disjoint window #{} - {} - meanL: {}, meanU: {}", i + 1, query.getOrder(), query.getMeanMin(), query.getMeanMax());

            int deltaW = (i == queries.size() - 1) ? 0 : (queries.get(i + 1).getOrder() - query.getOrder()) * WuList[0];
            preLength += query.getWu() / WuList[0];

            List<NormInterval> nextValidPositions = new ArrayList<>();  // CS

            // store possible current segment
            List<NormInterval> positions = new ArrayList<>();  // CS_i

            // query possible rows which mean is in distance range of i-th disjoint window
            double beginRound = 1.0 / alpha * query.getMeanMin() + (1 - 1.0 / alpha) * meanQ - beta - 1.0 / alpha * epsilon * stdQ / Math.sqrt(query.getWu());
            double beginRound1 = alpha * query.getMeanMin() + (1 - alpha) * meanQ - beta - alpha * epsilon * stdQ / Math.sqrt(query.getWu());
            beginRound = MeanIntervalUtils.toRound(Math.min(beginRound, beginRound1), statisticInfos.get(query.getWu() / WuList[0] - 1));

            double endRound = alpha * query.getMeanMax() + (1 - alpha) * meanQ + beta + alpha * epsilon * stdQ / Math.sqrt(query.getWu());
            double endRound1 = 1.0 / alpha * query.getMeanMax() + (1 - 1.0 / alpha) * meanQ + beta + 1.0 / alpha * epsilon * stdQ / Math.sqrt(query.getWu());
            endRound = MeanIntervalUtils.toRound(Math.max(endRound, endRound1));

            logger.info("Scan index from {} to {}", beginRound, endRound);
            if (ENABLE_INCREMENTAL_VISITING) {
                int cacheIndex = query.getWu() / WuList[0] - 1;
                int index_l = findCache(cacheIndex, beginRound);
                int index_r = findCache(cacheIndex, endRound, index_l);

                if (index_l == index_r && index_l >= 0) {
                    /*
                     * Current:          l|===|r
                     * Cache  : index_l_l|_____|index_l_r
                     * Future : index_l_l|_____|index_l_r
                     */
                    scanCache(index_l, beginRound, true, endRound, true, query, positions);
                } else if (index_l < 0 && index_r >= 0) {
                    /*
                     * Current:         l|_==|r
                     * Cache  :   index_r_l|_____|index_r_r
                     * Future : index_r_l|_______|index_r_r
                     */
                    scanCache(index_r, indexCaches.get(cacheIndex).get(index_r).getBeginRound(), true, endRound, true, query, positions);
                    scanIndexAndAddCache(beginRound, true, indexCaches.get(cacheIndex).get(index_r).getBeginRound(), false, index_r, query, positions);
                    indexCaches.get(cacheIndex).get(index_r).setBeginRound(beginRound);
                } else if (index_l >= 0 && index_r < 0) {
                    /*
                     * Current:             l|==_|r
                     * Cache  : index_l_l|_____|index_l_r
                     * Future : index_l_l|_______|index_l_r
                     */
                    scanCache(index_l, beginRound, true, indexCaches.get(cacheIndex).get(index_l).getEndRound(), true, query, positions);
                    scanIndexAndAddCache(indexCaches.get(cacheIndex).get(index_l).getEndRound(), false, endRound, true, index_l, query, positions);
                    indexCaches.get(cacheIndex).get(index_l).setEndRound(endRound);
                } else if (index_l == index_r && index_l < 0) {
                    /*
                     * Current:        l|___|r
                     * Cache  : |_____|       |_____|
                     * Future : |_____|l|___|r|_____|
                     */
                    scanIndexAndAddCache(beginRound, true, endRound, true, index_r, query, positions);  // insert a new cache node
                } else if (index_l >= 0 && index_r >= 0 && index_l + 1 == index_r) {
                    /*
                      Current:     l|=___=|r
                      Cache  : |_____|s  |_____|
                      Future : |_______________|
                     */
                    double s = indexCaches.get(cacheIndex).get(index_l).getEndRound();
                    scanCache(index_l, beginRound, true, s, true, query, positions);
                    scanIndexAndAddCache(s, false, indexCaches.get(cacheIndex).get(index_r).getBeginRound(), false, index_r, query, positions);
                    scanCache(index_r, indexCaches.get(cacheIndex).get(index_r).getBeginRound(), true, endRound, true, query, positions);
                    indexCaches.get(cacheIndex).get(index_r).setBeginRound(s + 0.01);
                }
            } else {
                scanIndex(beginRound, true, endRound, true, query, positions);
            }
            positions = sortButNotMergeIntervals(positions);
//            logger.info("position: {}", positions.toString());

            if (i == 0) {
                for (NormInterval position : positions) {
                    if (position.getRight() - (query.getOrder() - 1) * WuList[0] + queryLength - 1 > n) {
                        if (position.getLeft() - (query.getOrder() - 1) * WuList[0] + queryLength - 1 <= n) {
                            nextValidPositions.add(new NormInterval(position.getLeft() + deltaW, n - queryLength + 1 + (query.getOrder() - 1) * WuList[0] + deltaW, position.getExLower(), position.getEx2Lower(), position.getExUpper(), position.getEx2Upper()));
                        }
                    } else if (position.getLeft() - (query.getOrder() - 1) * WuList[0] < 1) {
                        if (position.getRight() - (query.getOrder() - 1) * WuList[0] >= 1) {
                            nextValidPositions.add(new NormInterval(1 + (query.getOrder() - 1) * WuList[0] + deltaW, position.getRight() + deltaW, position.getExLower(), position.getEx2Lower(), position.getExUpper(), position.getEx2Upper()));
                        }
                    } else {
                        nextValidPositions.add(new NormInterval(position.getLeft() + deltaW, position.getRight() + deltaW, position.getExLower(), position.getEx2Lower(), position.getExUpper(), position.getEx2Upper()));
                    }
                }
            } else {
                int index1 = 0, index2 = 0;  // 1 - CS, 2 - CS_i
                while (index1 < validPositions.size() && index2 < positions.size()) {
                    if (validPositions.get(index1).getRight() < positions.get(index2).getLeft()) {
                        index1++;
                    } else if (positions.get(index2).getRight() < validPositions.get(index1).getLeft()) {
                        index2++;
                    } else {
                        if (ENABLE_STD_FILTER) {
                            double std2 = 0;
                            // meanLower > meanQ + beta
                            double sumExLower = validPositions.get(index1).getExLower() + positions.get(index2).getExLower();
                            double sumEx2Lower = validPositions.get(index1).getEx2Lower() + positions.get(index2).getEx2Lower();
                            double meanLower = sumExLower / preLength;
                            if (meanLower > meanQ + beta) {
                                double newValue = meanQ + beta - (meanLower - meanQ - beta) * preLength * WuList[0] / (queryLength - preLength * 1.0 * WuList[0]);
                                meanLower = meanQ + beta;
                                std2 = (sumEx2Lower * WuList[0] + (queryLength - preLength * WuList[0]) * newValue * newValue) / queryLength - meanLower * meanLower;
                            }
                            // meanUpper < meanQ - beta
                            double sumExUpper = validPositions.get(index1).getExUpper() + positions.get(index2).getExUpper();
                            double sumEx2Upper = validPositions.get(index1).getEx2Upper() + positions.get(index2).getEx2Upper();
                            double meanUpper = sumExUpper / preLength;
                            if (meanUpper < meanQ - beta) {
                                double newValue = meanQ - beta - (meanQ - beta - meanUpper) * preLength * WuList[0] / (queryLength - preLength * 1.0 * WuList[0]);
                                meanUpper = meanQ - beta;
                                std2 = (sumEx2Upper * WuList[0] + (queryLength - preLength * WuList[0]) * newValue * newValue) / queryLength - meanUpper * meanUpper;
                            }

                            if (validPositions.get(index1).getRight() < positions.get(index2).getRight()) {
                                if (Double.compare(std2, alpha * alpha * stdQ * stdQ) <= 0) {
                                    nextValidPositions.add(new NormInterval(Math.max(validPositions.get(index1).getLeft(), positions.get(index2).getLeft()) + deltaW, validPositions.get(index1).getRight() + deltaW, sumExLower, sumEx2Lower, sumExUpper, sumEx2Upper));
                                }
                                index1++;
                            } else {
                                if (Double.compare(std2, alpha * alpha * stdQ * stdQ) <= 0) {
                                    nextValidPositions.add(new NormInterval(Math.max(validPositions.get(index1).getLeft(), positions.get(index2).getLeft()) + deltaW, positions.get(index2).getRight() + deltaW, sumExLower, sumEx2Lower, sumExUpper, sumEx2Upper));
                                }
                                index2++;
                            }
                        } else {
                            if (validPositions.get(index1).getRight() < positions.get(index2).getRight()) {
                                nextValidPositions.add(new NormInterval(Math.max(validPositions.get(index1).getLeft(), positions.get(index2).getLeft()) + deltaW, validPositions.get(index1).getRight() + deltaW, 0, 0, 0, 0));
                                index1++;
                            } else {
                                nextValidPositions.add(new NormInterval(Math.max(validPositions.get(index1).getLeft(), positions.get(index2).getLeft()) + deltaW, positions.get(index2).getRight() + deltaW, 0, 0, 0, 0));
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
            int cntCurrentCandidateOffsets = candidates.getSecond().getSecond();
            logger.info("Disjoint candidate windows: {}, candidate offsets: {}", cntCurrentDisjointCandidateWindows, cntCurrentCandidateOffsets);

            if (ENABLE_EARLY_TERMINATION) {
                int phase1TimeUsageUntilNow = (int) (System.currentTimeMillis() - startTime1);
                double phase2TimeUsageEstimated = PHASE_2_TIME_ESTIMATE_COEFFICIENT_A * cntCurrentDisjointCandidateWindows + PHASE_2_TIME_ESTIMATE_COEFFICIENT_B * cntCurrentCandidateOffsets / 100000 * queryLength + PHASE_2_TIME_ESTIMATE_INTERCEPT;
                double totalTimeUsageEstimated = phase1TimeUsageUntilNow + phase2TimeUsageEstimated;
                logger.info("Time usage: phase 1 until now: {}, phase 2 estimated: {}, total estimated: {}", phase1TimeUsageUntilNow, phase2TimeUsageEstimated, totalTimeUsageEstimated);

                if (i >= 5 && totalTimeUsageEstimated > lastTotalTimeUsageEstimated) {
                    lastSegment = (i == queries.size() - 1) ? query.getOrder() : queries.get(i + 1).getOrder();
                    break;
                }
                lastTotalTimeUsageEstimated = totalTimeUsageEstimated;
            }
        }
        if (!ENABLE_EARLY_TERMINATION) {
            lastSegment = queries.get(queries.size() - 1).getOrder();
        }

        // merge consecutive intervals to shrink data size and alleviate scan times
        validPositions = sortAndMergeIntervals(validPositions);

        long endTime1 = System.currentTimeMillis();

        // Phase 2: post-processing
        long startTime2 = System.currentTimeMillis();

        List<Pair<Integer, Double>> answers = new ArrayList<>();
        int cntCandidate = 0;
        // do z-normalization on query data
        double[] zQ = new double[queryLength];
        for (int i = 0; i < queryLength; i++) {
            zQ[i] = (queryData.get(i) - meanQ) / stdQ;
        }
        // Create envelop of the query: lower envelop, l, and upper envelop, u
        double[] u = new double[queryLength], l = new double[queryLength];
        DtwUtils.lowerUpperLemire(zQ, queryLength, rho, l, u);
        // Sort the query one time by abs(z-norm(q[i]))
        Index[] tmpQ = new Index[queryLength];
        for (int i = 0; i < queryLength; i++) {
            tmpQ[i] = new Index(zQ[i], i);
        }
        Arrays.sort(tmpQ, (o1, o2) -> {
            // Sorting function for the query, sort by abs(z_norm(q[i])) from high to low
            return (int) (Math.abs(o2.value) - Math.abs(o1.value));
        });
        // also create another arrays for keeping sorted envelop
        int[] order = new int[queryLength];
        double[] qo = new double[queryLength];
        double[] uo = new double[queryLength];
        double[] lo = new double[queryLength];
        for (int i = 0; i < queryLength; i++) {
            int o = tmpQ[i].index;
            order[i] = o;
            qo[i] = zQ[o];
            uo[i] = u[o];
            lo[i] = l[o];
        }

        int idx = 0;
        while (idx < validPositions.size()) {
            int beginIdx = idx, endIdx = idx;

            int begin = validPositions.get(idx).getLeft() - (lastSegment - 1) * WuList[0];
            int end = validPositions.get(idx).getRight() - (lastSegment - 1) * WuList[0] + queryLength - 1;
            if (begin < 1) begin = 1;
            int length = end - begin + 1;
            idx++;

            while (idx < validPositions.size()) {
                begin = validPositions.get(idx).getLeft() - (lastSegment - 1) * WuList[0];
                int newLength = length + begin - end - 1;
                end = validPositions.get(idx).getRight() - (lastSegment - 1) * WuList[0] + queryLength - 1;
                if (end > n) end = n;
                newLength += end - begin + 1;
                if (newLength > MAX_SCAN_DATA_LENGTH) break;
                endIdx = idx;
                length = newLength;
                idx++;
            }

            begin = validPositions.get(beginIdx).getLeft() - (lastSegment - 1) * WuList[0];
            end = validPositions.get(endIdx).getRight() - (lastSegment - 1) * WuList[0] + queryLength - 1;
            if (begin < 1) begin = 1;
            if (end > n) end = n;
            logger.debug("Scan data [{}, {}]", begin, end);
            List<Double> data = timeSeriesOperator.readTimeSeries(begin, end - begin + 1);

            double[] uBuff = new double[data.size()];
            double[] lBuff = new double[data.size()];
            DtwUtils.lowerUpperLemire(data, rho, lBuff, uBuff);

            for (int idx1 = beginIdx; idx1 <= endIdx; idx1++) {
                cntCandidate += validPositions.get(idx1).getRight() - validPositions.get(idx1).getLeft() + 1;

                ex = ex2 = 0;
                double[] T = new double[2 * queryLength];

                int begin1 = validPositions.get(idx1).getLeft() - (lastSegment - 1) * WuList[0] - begin;
                int end1 = validPositions.get(idx1).getRight() - (lastSegment - 1) * WuList[0] + queryLength - 1 - begin;

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

                        if (Math.abs(mean - meanQ) <= beta && (std / stdQ) <= alpha && (std / stdQ) >= 1.0 / alpha) {  //  test single point range criterion
                            // Use a constant lower bound to prune the obvious sub-sequence
                            double lbKim = DtwUtils.lbKimHierarchy(T, zQ, j, queryLength, mean, std, epsilon * epsilon);
                            if (lbKim <= epsilon * epsilon) {
                                // Use a linear time lower bound to prune; z_normalization of t will be computed on the fly.
                                // uo, lo are envelop of the query.
                                double[] cb1 = new double[queryLength];
                                double lbK = DtwUtils.lbKeoghCumulative(order, T, uo, lo, cb1, j, queryLength, mean, std, epsilon * epsilon);
                                if (lbK <= epsilon * epsilon) {
                                    // Take another linear time to compute z_normalization of t.
                                    // Note that for better optimization, this can merge to the previous function.
                                    double[] zT = new double[queryLength];
                                    for (int k = 0; k < queryLength; k++) {
                                        zT[k] = (T[(k + j)] - mean) / std;
                                    }
                                    // Use another lb_keogh to prune
                                    // qo is the sorted query. tz is unsorted z_normalized data.
                                    // l_buff, u_buff are big envelop for all data in this chunk
                                    double[] cb2 = new double[queryLength];
                                    double lbK2 = DtwUtils.lbKeoghDataCumulative(order, qo, cb2, i-queryLength+1, lBuff, uBuff, queryLength, mean, std, epsilon * epsilon);
                                    if (lbK2 <= epsilon * epsilon) {
                                        // Choose better lower bound between lb_keogh and lb_keogh2 to be used in early abandoning DTW
                                        // Note that cb and cb2 will be cumulative summed here.
                                        double[] cb = new double[queryLength];
                                        if (lbK > lbK2) {
                                            cb[queryLength - 1] = cb1[queryLength - 1];
                                            for (int k = queryLength - 2; k >= 0; k--) {
                                                cb[k] = cb[k + 1] + cb1[k];
                                            }
                                        } else {
                                            cb[queryLength - 1] = cb2[queryLength - 1];
                                            for (int k = queryLength - 2; k >= 0; k--) {
                                                cb[k] = cb[k + 1] + cb2[k];
                                            }
                                        }
                                        // Compute DTW and early abandoning if possible
                                        double dist = DtwUtils.dtw(zT, zQ, cb, queryLength, rho, epsilon * epsilon);
                                        if (dist <= epsilon * epsilon) {
                                            answers.add(new Pair<>(begin + i - queryLength + 1, Math.sqrt(dist)));
                                        }
                                    }
                                }
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

    private Pair<Integer, Integer> getCountsFromStatisticInfo(int Wu, double meanMin, double meanMax, double epsilon, double alpha, double beta, double meanQ, double stdQ) {
        List<Pair<Double, Pair<Integer, Integer>>> statisticInfo = statisticInfos.get(Wu / WuList[0] - 1);

        double beginRound = 1.0/alpha * meanMin + (1 - 1.0/alpha) * meanQ - beta - 1.0 / alpha * epsilon * stdQ / Math.sqrt(Wu);
        double beginRound1 = alpha * meanMin + (1 - alpha) * meanQ - beta - alpha * epsilon * stdQ / Math.sqrt(Wu);
        beginRound = MeanIntervalUtils.toRound(Math.min(beginRound, beginRound1));

        double endRound = alpha * meanMax + (1 - alpha) * meanQ + beta + alpha * epsilon * stdQ / Math.sqrt(Wu);
        double endRound1 = 1.0/alpha * meanMax + (1 - 1.0/alpha) * meanQ + beta + 1.0 / alpha * epsilon * stdQ / Math.sqrt(Wu);
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

    private double getCost(int l, int r, double epsilon, double alpha, double beta, double meanQ, double stdQ) {
        if (cost[l][r] != -1) return cost[l][r];
        int useWu = WuList[0] * (r - l + 1);
        double meanU = (prefixSumsU[r] - (l > 0 ? prefixSumsU[l - 1] : 0)) / useWu;
        double meanL = (prefixSumsL[r] - (l > 0 ? prefixSumsL[l - 1] : 0)) / useWu;
        Pair<Integer, Integer> counts = getCountsFromStatisticInfo(useWu, meanL, meanU, epsilon, alpha, beta, meanQ, stdQ);
        cost[l][r] = 1.0 * counts.getFirst() / statisticInfos.get(100 / 25 - 1).get(statisticInfos.get(100 / 25 - 1).size() - 1).getSecond().getFirst();
        cost[l][r] = Math.log(cost[l][r]);
        cost2[l][r] = counts.getFirst();
        return cost[l][r];
    }

    private int getCost2(int l, int r, double epsilon, double alpha, double beta, double meanQ, double stdQ) {
        if (cost2[l][r] != -1) return cost2[l][r];
        int useWu = WuList[0] * (r - l + 1);
        double meanU = (prefixSumsU[r] - (l > 0 ? prefixSumsU[l - 1] : 0)) / useWu;
        double meanL = (prefixSumsL[r] - (l > 0 ? prefixSumsL[l - 1] : 0)) / useWu;
        Pair<Integer, Integer> counts = getCountsFromStatisticInfo(useWu, meanL, meanU, epsilon, alpha, beta, meanQ, stdQ);
        cost2[l][r] = counts.getFirst();
        return cost2[l][r];
    }

    private List<RangeQuerySegment> determineQueryPlan(List<Double> queryData, double epsilon, int rho, double alpha, double beta, double meanQ, double stdQ) {
        int m = queryData.size() / WuList[0];

        // calculate L[i] and U[i]
        double[] value = new double[queryData.size() + 2 * rho];
        for (int i = 0; i < rho; i++) {  // add number ro first value in the front
            value[i] = queryData.get(0);
        }
        for (int i = rho; i < queryData.size() + rho; i++) {  // add number N values
            value[i] = queryData.get(i - rho);
        }
        for (int i = queryData.size() + rho; i < queryData.size() + 2 * rho; i++) {  // add number ro last value in the end
            value[i] = queryData.get(queryData.size() - 1);
        }
        // for maximum (U)
        double[] U = new double[queryData.size()];
        int[] aMax = new int[queryData.size() + 2 * rho];
        int fMax = 0, tMax = 0;
        for (int i = 0; i < queryData.size() + 2 * rho; i++) {
            while (fMax < tMax && aMax[fMax] <= i - (2 * rho + 1)) {
                fMax++;
            }
            while (fMax < tMax && Double.compare(value[aMax[tMax - 1]], value[i]) <= 0) {
                tMax--;
            }
            aMax[tMax++] = i;
            if (i >= 2 * rho) {
                U[i - 2 * rho] = value[aMax[fMax]];
            }
        }
        // for minimum (L)
        double[] L = new double[queryData.size()];
        int[] aMin = new int[queryData.size() + 2 * rho];
        int fMin = 0, tMin = 0;
        for (int i = 0; i < queryData.size() + 2 * rho; i++) {
            while (fMin < tMin && aMin[fMin] <= i - (2 * rho + 1)) {
                fMin++;
            }
            while (fMin < tMin && Double.compare(value[aMin[tMin - 1]], value[i]) >= 0) {
                tMin--;
            }
            aMin[tMin++] = i;
            if (i >= 2 * rho) {
                L[i - 2 * rho] = value[aMin[fMin]];
            }
        }

        // calculate average bounds and sum of each disjoint window
        List<Double> sumsU = new ArrayList<>(m);
        List<Double> sumsL = new ArrayList<>(m);
        double exU = 0, exL = 0;
        for (int i = 0; i < queryData.size(); i++) {
            exU += U[i];
            exL += L[i];
            if ((i + 1) % WuList[0] == 0) {
                sumsL.add(exL);
                sumsU.add(exU);
                exU = 0;
                exL = 0;
            }
        }

        // initialize for dynamic programming algorithm
        double[][] dp = new double[m + 1][];
        int[][] pre = new int[m + 1][];
        cost = new double[m][];
        cost2 = new int[m][];
        prefixSumsU = new double[m];
        prefixSumsL = new double[m];
        prefixSumsU[0] = sumsU.get(0);
        prefixSumsL[0] = sumsL.get(0);
        for (int i = 0; i <= m; i++) {
            dp[i] = new double[m + 1];
            Arrays.fill(dp[i], Double.MAX_VALUE);
            pre[i] = new int[m + 1];
            Arrays.fill(pre[i], -1);
        }
        for (int i = 0; i < m; i++) {
            cost[i] = new double[m];
            Arrays.fill(cost[i], -1);
            cost2[i] = new int[m];
            Arrays.fill(cost2[i], -1);
            if (i > 0) {
                prefixSumsU[i] = prefixSumsU[i - 1] + sumsU.get(i);
                prefixSumsL[i] = prefixSumsL[i - 1] + sumsL.get(i);
            }
        }

        dp[0][0] = 0;
        for (int i = 1; i <= m; i++) {
            for (int j = 1; j <= Math.min(i, 30); j++) {
                for (int k = 1; k <= WuList.length && i - k >= 0; k++) {
                    if (!WuEnabled[k - 1]) continue;
                    //double tmp = Math.pow(Math.pow(dp[i-k][j-1], j-1) * getCost(i-k, i-1), 1.0 / j);
                    double tmp = ((j - 1) * (dp[i - k][j - 1]) + getCost(i - k, i - 1, epsilon, alpha, beta, meanQ, stdQ)) / j;
                    if (tmp < dp[i][j]) {
                        dp[i][j] = tmp;
                        pre[i][j] = k;
                    }
                }
            }
        }

        // find out the optimal division strategy
        List<RangeQuerySegment> queries = new ArrayList<>();
        double min = Double.MAX_VALUE;
        int index = m, p = -1;
        for (int i = (31 - Integer.numberOfLeadingZeros(queryData.size()) - 1) / 2; i <= Math.min(m, 30); i++) {
            if (dp[m][i] <= min) {
                min = dp[m][i];
                p = i;
            }
        }
        for (int i = p; i >= 0; i--) {
            int l = index - pre[index][i], r = index - 1;
            int useWu = WuList[0] * (r - l + 1);
            if (useWu < 0) break;
            double meanU = (prefixSumsU[r] - (l > 0 ? prefixSumsU[l - 1] : 0)) / useWu;
            double meanL = (prefixSumsL[r] - (l > 0 ? prefixSumsL[l - 1] : 0)) / useWu;
            queries.add(new RangeQuerySegment(meanL, meanU, l + 1, getCost2(l, r, epsilon, alpha, beta, meanQ, stdQ), useWu));
            index -= pre[index][i];
        }

        if (ENABLE_QUERY_REORDERING) {
            // optimize query order
            queries.sort(Comparator.comparingInt(RangeQuerySegment::getCount));
        }

        return queries;
    }

    private void scanIndex(double begin, boolean beginInclusive, double end, boolean endInclusive, RangeQuerySegment query, List<NormInterval> positions) throws IOException {
        int useWu = query.getWu() / WuList[0];

        if (!beginInclusive) begin = begin + 0.01;
        if (endInclusive) end = end + 0.01;

        Map<Double, IndexNode> indexes = indexOperators[useWu - 1].readIndexes(begin, end);
        cntScans++;
        for (Map.Entry<Double, IndexNode> entry : indexes.entrySet()) {
            double meanRoundLower = entry.getKey();
            double meanRoundUpper = MeanIntervalUtils.toUpper(meanRoundLower, statisticInfos.get(useWu - 1));
            double meanRound2Lower = meanRoundLower < 0 ? meanRoundUpper * meanRoundUpper : meanRoundLower * meanRoundLower;
            double meanRound2Upper = meanRoundUpper < 0 ? meanRoundLower * meanRoundLower : meanRoundUpper * meanRoundUpper;
            for (Pair<Integer, Integer> position : entry.getValue().getPositions()) {
                positions.add(new NormInterval(position.getFirst(), position.getSecond(), meanRoundLower * useWu, meanRound2Lower * useWu, meanRoundUpper * useWu, meanRound2Upper * useWu));
            }
        }
    }

    private void scanIndexAndAddCache(double begin, boolean beginInclusive, double end, boolean endInclusive, int index, RangeQuerySegment query, List<NormInterval> positions) throws IOException {
        int useWu = query.getWu() / WuList[0];

        if (index < 0) {
            index = -index - 1;
            indexCaches.get(useWu - 1).add(index, new IndexCache(begin, end));
        }

        if (!beginInclusive) begin = begin + 0.01;
        if (endInclusive) end = end + 0.01;

        Map<Double, IndexNode> indexes = indexOperators[useWu - 1].readIndexes(begin, end);
        cntScans++;
        for (Map.Entry<Double, IndexNode> entry : indexes.entrySet()) {
            double meanRoundLower = entry.getKey();
            double meanRoundUpper = MeanIntervalUtils.toUpper(meanRoundLower, statisticInfos.get(useWu - 1));
            double meanRound2Lower = meanRoundLower < 0 ? meanRoundUpper * meanRoundUpper : meanRoundLower * meanRoundLower;
            double meanRound2Upper = meanRoundUpper < 0 ? meanRoundLower * meanRoundLower : meanRoundUpper * meanRoundUpper;
            for (Pair<Integer, Integer> position : entry.getValue().getPositions()) {
                positions.add(new NormInterval(position.getFirst(), position.getSecond(), meanRoundLower * useWu, meanRound2Lower * useWu, meanRoundUpper * useWu, meanRound2Upper * useWu));
            }

            indexCaches.get(useWu - 1).get(index).addCache(entry.getKey(), entry.getValue());
        }
    }

    private void scanCache(int index, double begin, boolean beginInclusive, double end, boolean endInclusive, RangeQuerySegment query, List<NormInterval> positions) {
        int useWu = query.getWu() / WuList[0];

        for (Map.Entry<Double, IndexNode> entry : indexCaches.get(useWu - 1).get(index).getCaches().subMap(begin, beginInclusive, end, endInclusive).entrySet()) {
            double meanRoundLower = entry.getKey();
            double meanRoundUpper = MeanIntervalUtils.toUpper(meanRoundLower, statisticInfos.get(useWu - 1));
            double meanRound2Lower = meanRoundLower < 0 ? meanRoundUpper * meanRoundUpper : meanRoundLower * meanRoundLower;
            double meanRound2Upper = meanRoundUpper < 0 ? meanRoundLower * meanRoundLower : meanRoundUpper * meanRoundUpper;
            IndexNode indexNode = entry.getValue();
            for (Pair<Integer, Integer> position : indexNode.getPositions()) {
                positions.add(new NormInterval(position.getFirst(), position.getSecond(), meanRoundLower * useWu, meanRound2Lower * useWu, meanRoundUpper * useWu, meanRound2Upper * useWu));
            }
        }
    }

    private int findCache(int index, double round) {
        return findCache(index, round, 0);
    }

    private int findCache(int index, double round, int first) {
        if (first < 0) {
            first = -first - 1;
        }

        for (int i = first; i < indexCaches.get(index).size(); i++) {
            IndexCache cache = indexCaches.get(index).get(i);
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
        double exLower = first.getExLower();
        double ex2Lower = first.getEx2Lower();
        double exUpper = first.getExUpper();
        double ex2Upper = first.getEx2Upper();

        List<NormInterval> result = new ArrayList<>();

        for (int i = 1; i < intervals.size(); i++) {
            NormInterval current = intervals.get(i);
            if (current.getLeft() - 1 < end || (current.getLeft() - 1 == end && Double.compare(current.getExLower(), exLower) == 0 && Double.compare(current.getEx2Lower(), ex2Lower) == 0)) {
                end = Math.max(current.getRight(), end);
                exLower = Math.min(current.getExLower(), exLower);
                ex2Lower = Math.min(current.getEx2Lower(), ex2Lower);
                exUpper = Math.min(current.getExUpper(), exUpper);
                ex2Upper = Math.min(current.getEx2Upper(), ex2Upper);
            } else {
                result.add(new NormInterval(start, end, exLower, ex2Lower, exUpper, ex2Upper));
                start = current.getLeft();
                end = current.getRight();
                exLower = current.getExLower();
                ex2Lower = current.getEx2Lower();
                exUpper = current.getExUpper();
                ex2Upper = current.getEx2Upper();
            }
        }
        result.add(new NormInterval(start, end, exLower, ex2Lower, exUpper, ex2Upper));

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
        double exLower = first.getExLower();
        double ex2Lower = first.getEx2Lower();
        double exUpper = first.getExUpper();
        double ex2Upper = first.getEx2Upper();

        List<NormInterval> result = new ArrayList<>();

        int cntDisjointIntervals = intervals.size();
        int cntOffsets = 0;
        for (int i = 1; i < intervals.size(); i++) {
            NormInterval current = intervals.get(i);

            if (current.getLeft() - 1 <= end) {  // count for disjoint intervals to estimate time usage of phase 2
                cntDisjointIntervals--;
            }

            if (current.getLeft() - 1 < end || (current.getLeft() - 1 == end && Double.compare(current.getExLower(), exLower) == 0 && Double.compare(current.getEx2Lower(), ex2Lower) == 0)) {
                end = Math.max(current.getRight(), end);
                exLower = Math.min(current.getExLower(), exLower);
                ex2Lower = Math.min(current.getEx2Lower(), ex2Lower);
                exUpper = Math.min(current.getExUpper(), exUpper);
                ex2Upper = Math.min(current.getEx2Upper(), ex2Upper);
            } else {
                result.add(new NormInterval(start, end, exLower, ex2Lower, exUpper, ex2Upper));
                cntOffsets += end - start + 1;
                start = current.getLeft();
                end = current.getRight();
                exLower = current.getExLower();
                ex2Lower = current.getEx2Lower();
                exUpper = current.getExUpper();
                ex2Upper = current.getEx2Upper();
            }
        }
        result.add(new NormInterval(start, end, exLower, ex2Lower, exUpper, ex2Upper));
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
        double exLower = first.getExLower();
        double ex2Lower = first.getEx2Lower();
        double exUpper = first.getExUpper();
        double ex2Upper = first.getEx2Upper();

        List<NormInterval> result = new ArrayList<>();

        for (int i = 1; i < intervals.size(); i++) {
            NormInterval current = intervals.get(i);
            if (current.getLeft() - 1 <= end) {
                end = Math.max(current.getRight(), end);
                exLower = Math.min(current.getExLower(), exLower);
                ex2Lower = Math.min(current.getEx2Lower(), ex2Lower);
                exUpper = Math.min(current.getExUpper(), exUpper);
                ex2Upper = Math.min(current.getEx2Upper(), ex2Upper);
            } else {
                result.add(new NormInterval(start, end, exLower, ex2Lower, exUpper, ex2Upper));
                start = current.getLeft();
                end = current.getRight();
                exLower = current.getExLower();
                ex2Lower = current.getEx2Lower();
                exUpper = current.getExUpper();
                ex2Upper = current.getEx2Upper();
            }
        }
        result.add(new NormInterval(start, end, exLower, ex2Lower, exUpper, ex2Upper));

        return result;
    }

    /**
     * Fetch the meta table of each KV-index
     */
    private void loadMetaTable() {
        for (int i = 0; i < WuList.length; i++) {
            if (!WuEnabled[i]) {
                statisticInfos.add(new ArrayList<>());
                continue;
            }
            try {
                statisticInfos.add(indexOperators[i].readStatisticInfo());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
