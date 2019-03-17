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

import cn.edu.fudan.dsm.kvmatch.common.*;
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
import cn.edu.fudan.dsm.kvmatch.utils.DtwUtils;
import cn.edu.fudan.dsm.kvmatch.utils.MeanIntervalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Query engine for KV-index_{DP} under DTW
 * <p>
 * Created by Jiaye Wu on 18-1-10.
 */
@SuppressWarnings("Duplicates")
public class QueryEngineDtw {

    private static final Logger logger = LoggerFactory.getLogger(QueryEngineDtw.class.getName());

    // \Sigma = {25, 50, 100, 200, 400}
    private static final int[] WuList = {25, 50, 75, 100, 125, 150, 175, 200, 225, 250, 275, 300, 325, 350, 375, 400};
    private static final boolean[] WuEnabled = {true, true, false, true, false, false, false, true, false, false, false, false, false, false, false, true};

    private static final boolean ENABLE_EARLY_TERMINATION = true;
    private static final double PHASE_2_TIME_ESTIMATE_COEFFICIENT_A = 9.72276547123376;
    private static final double PHASE_2_TIME_ESTIMATE_COEFFICIENT_B = 0.0106737255022236;
    private static final double PHASE_2_TIME_ESTIMATE_INTERCEPT = 0.0;
    private static final boolean ENABLE_QUERY_REORDERING = true;
    private static final boolean ENABLE_INCREMENTAL_VISITING = false;

    private TimeSeriesOperator timeSeriesOperator = null;
    private IndexOperator[] indexOperators = new IndexOperator[WuList.length];
    private List<List<Pair<Double, Pair<Integer, Integer>>>> statisticInfos = new ArrayList<>(WuList.length);
    private List<List<IndexCache>> indexCaches = new ArrayList<>(WuList.length);
    private int n, cntScans;
    private double[] prefixSumsU, prefixSumsL;
    private double[][] cost;
    private int[][] cost2;

    public QueryEngineDtw(int n, String storageType) throws IOException {
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
                    indexOperators[i] = new IndexFileOperator("standard", n, WuList[i], false);
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

    public static void main(String[] args) throws IOException {
        Scanner scanner = new Scanner(System.in);
        System.out.print("Data Length = ");
        int n = scanner.nextInt();
        QueryEngineDtw queryEngine = new QueryEngineDtw(n, "file");
        StatisticWriter.println("Offset,Length,Epsilon,T,T_1,T_2,#candidates,#answers");

        do {
            int offset, length, rho;
            double epsilon;
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

            // initialization: 0-T, 1-T_1, 2-T_2, 3-#candidates, 4-#answers
            List<StatisticInfo> statisticInfos = new ArrayList<>(6);
            for (int i = 0; i < 6; i++) {
                statisticInfos.add(new StatisticInfo());
            }

            // execute the query request
            queryEngine.query(statisticInfos, offset, length, epsilon, rho);

            // output statistic information
            StatisticWriter.print(offset + "," + length + "," + epsilon + "," + rho + ",");
            for (int i = 0; i < 6; i++) {
                StatisticWriter.print(statisticInfos.get(i).getAverage() + ",");
            }
            StatisticWriter.println("");
        } while (true);
    }

    @SuppressWarnings("unchecked")
    public boolean query(List<StatisticInfo> statistics, int offset, int length, double epsilon, int rho) throws IOException {
        // fetch corresponding subsequence from data series
        logger.info("Query offset: {}, length: {}, epsilon: {}, rho: {}", offset, length, epsilon, rho);
        List<Double> queryData = timeSeriesOperator.readTimeSeries(offset, length);
        return query(statistics, queryData, epsilon, rho);
    }

    public boolean query(List<StatisticInfo> statistics, List<Double> queryData, double epsilon, int rho) throws IOException {
        // initialization: clear cache
        if (ENABLE_INCREMENTAL_VISITING) {
            indexCaches.clear();
            for (int ignored : WuList) {
                indexCaches.add(new ArrayList<>());
            }
        }
        cntScans = 0;
        int length = queryData.size();

        long startTime = System.currentTimeMillis();

        // Phase 0: segmentation (DP)
        List<RangeQuerySegment> queries = determineQueryPlan(queryData, epsilon, rho);
        logger.info("Query order: {}", queries);

        // Phase 1: index-probing
        long startTime1 = System.currentTimeMillis();

        List<Interval> validPositions = new ArrayList<>();  // CS

        int lastSegment = queries.get(queries.size() - 1).getOrder();
        double range0 = epsilon * epsilon;
        double lastMinimumEpsilon = 0;
        double lastTotalTimeUsageEstimated = Double.MAX_VALUE;
        for (int i = 0; i < queries.size(); i++) {
            RangeQuerySegment query = queries.get(i);
            logger.info("Disjoint window #{} - {} - meanL: {}, meanU: {}", i + 1, query.getOrder(), query.getMeanMin(), query.getMeanMax());

            int deltaW = (i == queries.size() - 1) ? 0 : (queries.get(i + 1).getOrder() - query.getOrder()) * WuList[0];

            List<Interval> nextValidPositions = new ArrayList<>();  // CS

            // store possible current segment
            List<Interval> positions = new ArrayList<>();  // CS_i

            // query possible rows which mean is in distance range of i-th disjoint window
            if (lastMinimumEpsilon > range0) lastMinimumEpsilon = 0;
            double range = Math.sqrt((range0 - lastMinimumEpsilon) / query.getWu());
            double beginRound = MeanIntervalUtils.toRound(query.getMeanMin() - range, statisticInfos.get(query.getWu() / WuList[0] - 1));
            double endRound = MeanIntervalUtils.toRound(query.getMeanMax() + range);

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

            lastMinimumEpsilon = Double.MAX_VALUE;

            if (i == 0) {
                for (Interval position : positions) {
                    if (position.getRight() - (query.getOrder() - 1) * WuList[0] + length - 1 > n) {
                        if (position.getLeft() - (query.getOrder() - 1) * WuList[0] + length - 1 <= n) {
                            nextValidPositions.add(new Interval(position.getLeft() + deltaW, n - length + 1 + (query.getOrder() - 1) * WuList[0] + deltaW, position.getEpsilon()));
                        }
                    } else if (position.getLeft() - (query.getOrder() - 1) * WuList[0] < 1) {
                        if (position.getRight() - (query.getOrder() - 1) * WuList[0] >= 1) {
                            nextValidPositions.add(new Interval(1 + (query.getOrder() - 1) * WuList[0] + deltaW, position.getRight() + deltaW, position.getEpsilon()));
                        }
                    } else {
                        nextValidPositions.add(new Interval(position.getLeft() + deltaW, position.getRight() + deltaW, position.getEpsilon()));
                    }
                    if (position.getEpsilon() < lastMinimumEpsilon) {
                        lastMinimumEpsilon = position.getEpsilon();
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
                        double sumEpsilon = validPositions.get(index1).getEpsilon() + positions.get(index2).getEpsilon();
                        if (validPositions.get(index1).getRight() < positions.get(index2).getRight()) {
                            if (sumEpsilon <= epsilon * epsilon) {
                                nextValidPositions.add(new Interval(Math.max(validPositions.get(index1).getLeft(), positions.get(index2).getLeft()) + deltaW, validPositions.get(index1).getRight() + deltaW, sumEpsilon));
                                if (sumEpsilon < lastMinimumEpsilon) {
                                    lastMinimumEpsilon = sumEpsilon;
                                }
                            }
                            index1++;
                        } else {
                            if (sumEpsilon <= epsilon * epsilon) {
                                nextValidPositions.add(new Interval(Math.max(validPositions.get(index1).getLeft(), positions.get(index2).getLeft()) + deltaW, positions.get(index2).getRight() + deltaW, sumEpsilon));
                                if (sumEpsilon < lastMinimumEpsilon) {
                                    lastMinimumEpsilon = sumEpsilon;
                                }
                            }
                            index2++;
                        }
                    }
                }
            }

            Pair<List<Interval>, Pair<Integer, Integer>> candidates = sortButNotMergeIntervalsAndCount(nextValidPositions);
            validPositions = candidates.getFirst();
//            logger.info("next valid: {}", validPositions.toString());

            int cntCurrentDisjointCandidateWindows = candidates.getSecond().getFirst();
            int cntCurrentCandidateOffsets = candidates.getSecond().getSecond();
            logger.info("Disjoint candidate windows: {}, candidate offsets: {}", cntCurrentDisjointCandidateWindows, cntCurrentCandidateOffsets);

            if (ENABLE_EARLY_TERMINATION) {
                int phase1TimeUsageUntilNow = (int) (System.currentTimeMillis() - startTime1);
                double phase2TimeUsageEstimated = PHASE_2_TIME_ESTIMATE_COEFFICIENT_A * cntCurrentDisjointCandidateWindows + PHASE_2_TIME_ESTIMATE_COEFFICIENT_B * cntCurrentCandidateOffsets / 100000 * length + PHASE_2_TIME_ESTIMATE_INTERCEPT;
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

        // copy query data (without normalization)
        double[] Q = new double[length];
        for (int i = 0; i < length; i++) {
            Q[i] = queryData.get(i);
        }
        // Create envelop of the query: lower envelop, l, and upper envelop, u
        double[] u = new double[length], l = new double[length];
        DtwUtils.lowerUpperLemire(Q, length, rho, l, u);
        // Sort the query one time by abs(q[i])
        Index[] tmpQ = new Index[length];
        for (int i = 0; i < length; i++) {
            tmpQ[i] = new Index(Q[i], i);
        }
        //Arrays.sort(tmpQ, (o1, o2) -> {
        //    // Sorting function for the query, sort by abs(q[i]) from high to low
        //    return (int) (Math.abs(o2.value) - Math.abs(o1.value));
        //});
        // also create another arrays for keeping sorted envelop
        int[] order = new int[length];
        double[] qo = new double[length];
        double[] uo = new double[length];
        double[] lo = new double[length];
        for (int i = 0; i < length; i++) {
            int o = tmpQ[i].index;
            order[i] = o;
            qo[i] = Q[o];
            uo[i] = u[o];
            lo[i] = l[o];
        }

        for (Interval position : validPositions) {
            cntCandidate += position.getRight() - position.getLeft() + 1;

            int begin = position.getLeft() - (lastSegment - 1) * WuList[0];
            int end = position.getRight() - (lastSegment - 1) * WuList[0] + length - 1;
            if (begin < 1) begin = 1;
            if (end > n) end = n;
            logger.debug("Scan data [{}, {}]", begin, end);
            @SuppressWarnings("unchecked")
            List<Double> data = timeSeriesOperator.readTimeSeries(begin, end - begin + 1);
            double[] T = new double[2 * length];

            double[] uBuff = new double[data.size()];
            double[] lBuff = new double[data.size()];
            DtwUtils.lowerUpperLemire(data, rho, lBuff, uBuff);

            for (int i = 0; i < data.size(); i++) {
                double d = data.get(i);
                T[i % length] = d;
                T[(i % length) + length] = d;

                if (i >= length - 1) {
                    // the current starting location of T
                    int j = (i + 1) % length;

                    // Use a constant lower bound to prune the obvious sub-sequence
                    double lbKim = DtwUtils.lbKimHierarchy(T, Q, j, length, 0, 1, epsilon * epsilon);
                    if (lbKim <= epsilon * epsilon) {
                        // Use a linear time lower bound to prune; z_normalization of t will be computed on the fly.
                        // uo, lo are envelop of the query.
                        double[] cb1 = new double[length];
                        double lbK = DtwUtils.lbKeoghCumulative(order, T, uo, lo, cb1, j, length, 0, 1, epsilon * epsilon);
                        if (lbK <= epsilon * epsilon) {
                            // Use another lb_keogh to prune
                            // qo is the sorted query.
                            // l_buff, u_buff are big envelop for all data in this chunk
                            double[] cb2 = new double[length];
                            double lbK2 = DtwUtils.lbKeoghDataCumulative(order, qo, cb2, i - length + 1, lBuff, uBuff, length, 0, 1, epsilon * epsilon);
                            if (lbK2 <= epsilon * epsilon) {
                                // Take another linear time to copy current t.
                                // Note that for better optimization, this can merge to the previous function.
                                double[] cT = new double[length];
                                System.arraycopy(T, j, cT, 0, length);
                                // Choose better lower bound between lb_keogh and lb_keogh2 to be used in early abandoning DTW
                                // Note that cb and cb2 will be cumulative summed here.
                                double[] cb = new double[length];
                                if (lbK > lbK2) {
                                    cb[length - 1] = cb1[length - 1];
                                    for (int k = length - 2; k >= 0; k--) {
                                        cb[k] = cb[k + 1] + cb1[k];
                                    }
                                } else {
                                    cb[length - 1] = cb2[length - 1];
                                    for (int k = length - 2; k >= 0; k--) {
                                        cb[k] = cb[k + 1] + cb2[k];
                                    }
                                }
                                // Compute DTW and early abandoning if possible
                                double dist = DtwUtils.dtw(cT, Q, cb, length, rho, epsilon * epsilon);
                                if (dist <= epsilon * epsilon) {
                                    answers.add(new Pair<>(begin + i - length + 1, Math.sqrt(dist)));
                                }
                            }
                        }
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

    private Pair<Integer, Integer> getCountsFromStatisticInfo(int Wu, double meanMin, double meanMax, double epsilon) {
        List<Pair<Double, Pair<Integer, Integer>>> statisticInfo = statisticInfos.get(Wu / WuList[0] - 1);

        double range = epsilon / Math.sqrt(Wu);
        double beginRound = MeanIntervalUtils.toRound(meanMin - range);
        double endRound = MeanIntervalUtils.toRound(meanMax + range);

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

    private double getCost(int l, int r, double epsilon) {
        if (cost[l][r] != -1) return cost[l][r];
        int useWu = WuList[0] * (r - l + 1);
        double meanU = (prefixSumsU[r] - (l > 0 ? prefixSumsU[l - 1] : 0)) / useWu;
        double meanL = (prefixSumsL[r] - (l > 0 ? prefixSumsL[l - 1] : 0)) / useWu;
        Pair<Integer, Integer> counts = getCountsFromStatisticInfo(useWu, meanL, meanU, epsilon);
        cost[l][r] = 1.0 * counts.getFirst() / statisticInfos.get(100 / 25 - 1).get(statisticInfos.get(100 / 25 - 1).size() - 1).getSecond().getFirst();
        cost[l][r] = Math.log(cost[l][r]);
        cost2[l][r] = counts.getFirst();
        return cost[l][r];
    }

    private int getCost2(int l, int r, double epsilon) {
        if (cost2[l][r] != -1) return cost2[l][r];
        int useWu = WuList[0] * (r - l + 1);
        double meanU = (prefixSumsU[r] - (l > 0 ? prefixSumsU[l - 1] : 0)) / useWu;
        double meanL = (prefixSumsL[r] - (l > 0 ? prefixSumsL[l - 1] : 0)) / useWu;
        Pair<Integer, Integer> counts = getCountsFromStatisticInfo(useWu, meanL, meanU, epsilon);
        cost2[l][r] = counts.getFirst();
        return cost2[l][r];
    }

    private List<RangeQuerySegment> determineQueryPlan(List<Double> queryData, double epsilon, int rho) {
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
                    double tmp = ((j - 1) * (dp[i - k][j - 1]) + getCost(i - k, i - 1, epsilon)) / j;
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
            queries.add(new RangeQuerySegment(meanL, meanU, l + 1, getCost2(l, r, epsilon), useWu));
            index -= pre[index][i];
        }

        if (ENABLE_QUERY_REORDERING) {
            // optimize query order
            queries.sort(Comparator.comparingInt(RangeQuerySegment::getCount));
        }

        return queries;
    }

    @SuppressWarnings("SameParameterValue")
    private void scanIndex(double begin, boolean beginInclusive, double end, boolean endInclusive,
                           RangeQuerySegment query, List<Interval> positions) throws IOException {
        if (!beginInclusive) begin = begin + 0.01;
        if (endInclusive) end = end + 0.01;

        Map<Double, IndexNode> indexes = indexOperators[query.getWu() / WuList[0] - 1].readIndexes(begin, end);
        cntScans++;
        for (Map.Entry<Double, IndexNode> entry : indexes.entrySet()) {
            double meanRound = entry.getKey();
            double lowerBound = getDistanceLowerBound(query, meanRound);
            for (Pair<Integer, Integer> position : entry.getValue().getPositions()) {
                positions.add(new Interval(position.getFirst(), position.getSecond(), query.getWu() * lowerBound));
            }
        }
    }

    private void scanIndexAndAddCache(double begin, boolean beginInclusive, double end, boolean endInclusive,
                                      int index, RangeQuerySegment query, List<Interval> positions) throws IOException {
        if (index < 0) {
            index = -index - 1;
            indexCaches.get(query.getWu() / WuList[0] - 1).add(index, new IndexCache(begin, end));
        }

        if (!beginInclusive) begin = begin + 0.01;
        if (endInclusive) end = end + 0.01;

        Map<Double, IndexNode> indexes = indexOperators[query.getWu() / WuList[0] - 1].readIndexes(begin, end);
        cntScans++;
        for (Map.Entry<Double, IndexNode> entry : indexes.entrySet()) {
            double meanRound = entry.getKey();
            double lowerBound = getDistanceLowerBound(query, meanRound);
            for (Pair<Integer, Integer> position : entry.getValue().getPositions()) {
                positions.add(new Interval(position.getFirst(), position.getSecond(), query.getWu() * lowerBound));
            }

            indexCaches.get(query.getWu() / WuList[0] - 1).get(index).addCache(meanRound, entry.getValue());
        }
    }

    @SuppressWarnings("SameParameterValue")
    private void scanCache(int index, double begin, boolean beginInclusive, double end, boolean endInclusive,
                           RangeQuerySegment query, List<Interval> positions) {
        for (Map.Entry<Double, IndexNode> entry : indexCaches.get(query.getWu() / WuList[0] - 1).get(index).getCaches().subMap(begin, beginInclusive, end, endInclusive).entrySet()) {
            double meanRound = entry.getKey();
            IndexNode indexNode = entry.getValue();
            double lowerBound = getDistanceLowerBound(query, meanRound);
            for (Pair<Integer, Integer> position : indexNode.getPositions()) {
                positions.add(new Interval(position.getFirst(), position.getSecond(), query.getWu() * lowerBound));
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

    private double getDistanceLowerBound(RangeQuerySegment query, double meanLower) {
        double meanUpper = MeanIntervalUtils.toUpper(meanLower, statisticInfos.get(query.getWu() / WuList[0] - 1));

        double delta;
        if (meanLower > query.getMeanMax()) {
            delta = (meanLower - query.getMeanMax()) * (meanLower - query.getMeanMax());
        } else if (meanUpper < query.getMeanMin()) {
            delta = (query.getMeanMin() - meanUpper) * (query.getMeanMin() - meanUpper);
        } else {
            delta = 0;
        }

        return delta;
    }

    private List<Interval> sortButNotMergeIntervals(List<Interval> intervals) {
        if (intervals.size() <= 1) {
            return intervals;
        }

        intervals.sort(Comparator.comparingInt(Interval::getLeft));

        Interval first = intervals.get(0);
        int start = first.getLeft();
        int end = first.getRight();
        double epsilon = first.getEpsilon();

        List<Interval> result = new ArrayList<>();

        for (int i = 1; i < intervals.size(); i++) {
            Interval current = intervals.get(i);
            if (current.getLeft() - 1 < end || (current.getLeft() - 1 == end && Math.abs(current.getEpsilon() - epsilon) < 1)) {
                end = Math.max(current.getRight(), end);
                epsilon = Math.min(current.getEpsilon(), epsilon);
            } else {
                result.add(new Interval(start, end, epsilon));
                start = current.getLeft();
                end = current.getRight();
                epsilon = current.getEpsilon();
            }
        }
        result.add(new Interval(start, end, epsilon));

        return result;
    }

    private Pair<List<Interval>, Pair<Integer, Integer>> sortButNotMergeIntervalsAndCount(List<Interval> intervals) {
        if (intervals.size() <= 1) {
            return new Pair<>(intervals, new Pair<>(intervals.size(), intervals.isEmpty() ? 0 : (intervals.get(0).getRight() - intervals.get(0).getLeft() + 1)));
        }

        intervals.sort(Comparator.comparingInt(Interval::getLeft));

        Interval first = intervals.get(0);
        int start = first.getLeft();
        int end = first.getRight();
        double epsilon = first.getEpsilon();

        List<Interval> result = new ArrayList<>();

        int cntDisjointIntervals = intervals.size();
        int cntOffsets = 0;
        for (int i = 1; i < intervals.size(); i++) {
            Interval current = intervals.get(i);

            if (current.getLeft() - 1 <= end) {  // count for disjoint intervals to estimate time usage of phase 2
                cntDisjointIntervals--;
            }

            if (current.getLeft() - 1 < end || (current.getLeft() - 1 == end && Math.abs(current.getEpsilon() - epsilon) < 1)) {
                end = Math.max(current.getRight(), end);
                epsilon = Math.min(current.getEpsilon(), epsilon);
            } else {
                result.add(new Interval(start, end, epsilon));
                cntOffsets += end - start + 1;
                start = current.getLeft();
                end = current.getRight();
                epsilon = current.getEpsilon();
            }
        }
        result.add(new Interval(start, end, epsilon));
        cntOffsets += end - start + 1;

        return new Pair<>(result, new Pair<>(cntDisjointIntervals, cntOffsets));
    }

    private List<Interval> sortAndMergeIntervals(List<Interval> intervals) {
        if (intervals.size() <= 1) {
            return intervals;
        }

        intervals.sort(Comparator.comparingInt(Interval::getLeft));

        Interval first = intervals.get(0);
        int start = first.getLeft();
        int end = first.getRight();
        double epsilon = first.getEpsilon();

        List<Interval> result = new ArrayList<>();

        for (int i = 1; i < intervals.size(); i++) {
            Interval current = intervals.get(i);
            if (current.getLeft() - 1 <= end) {
                end = Math.max(current.getRight(), end);
                epsilon = Math.min(current.getEpsilon(), epsilon);
            } else {
                result.add(new Interval(start, end, epsilon));
                start = current.getLeft();
                end = current.getRight();
                epsilon = current.getEpsilon();
            }
        }
        result.add(new Interval(start, end, epsilon));

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
