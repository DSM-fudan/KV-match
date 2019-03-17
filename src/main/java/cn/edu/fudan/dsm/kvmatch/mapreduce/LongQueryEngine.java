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
import cn.edu.fudan.dsm.kvmatch.mapreduce.common.LongIndexCache;
import cn.edu.fudan.dsm.kvmatch.mapreduce.common.LongIndexNode;
import cn.edu.fudan.dsm.kvmatch.mapreduce.common.LongInterval;
import cn.edu.fudan.dsm.kvmatch.mapreduce.common.LongQuerySegment;
import cn.edu.fudan.dsm.kvmatch.mapreduce.operator.FloatTimeSeriesTableOperator;
import cn.edu.fudan.dsm.kvmatch.mapreduce.operator.LongIndexTableOperator;
import cn.edu.fudan.dsm.kvmatch.mapreduce.utils.LongMeanIntervalUtils;
import cn.edu.fudan.dsm.kvmatch.statistic.StatisticInfo;
import cn.edu.fudan.dsm.kvmatch.statistic.StatisticWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

@SuppressWarnings("Duplicates")
public class LongQueryEngine {

    private static final Logger logger = LoggerFactory.getLogger(LongQueryEngine.class.getName());

    // \Sigma = {25, 50, 100, 200, 400}
    private static final int[] WuList = {25, 50, 75, 100, 125, 150, 175, 200, 225, 250, 275, 300, 325, 350, 375, 400};
    private static final boolean[] WuEnabled = {true, true, false, true, false, false, false, true, false, false, false, false, false, false, false, true};

    private static final boolean ENABLE_EARLY_TERMINATION = true;
    private static final double PHASE_2_TIME_ESTIMATE_COEFFICIENT_A = 4.0707589132278;
    private static final double PHASE_2_TIME_ESTIMATE_COEFFICIENT_B = 0.269833135638498;
    private static final double PHASE_2_TIME_ESTIMATE_INTERCEPT = 0.0;
    private static final boolean ENABLE_QUERY_REORDERING = true;
    private static final boolean ENABLE_INCREMENTAL_VISITING = true;

    private FloatTimeSeriesTableOperator timeSeriesOperator;
    private LongIndexTableOperator[] indexOperators = new LongIndexTableOperator[WuList.length];
    private List<List<Pair<Double, Pair<Long, Long>>>> statisticInfos = new ArrayList<>(WuList.length);
    private List<List<LongIndexCache>> indexCaches = new ArrayList<>(WuList.length);
    private long n;
    private double[] prefixSums;
    private double[][] cost;
    private long[][] cost2;

    public LongQueryEngine(long n) throws IOException {
        this.n = n;
        timeSeriesOperator = new FloatTimeSeriesTableOperator(n, 1, false);
        for (int i = 0; i < WuList.length; i++) {
            if (!WuEnabled[i]) continue;
            indexOperators[i] = new LongIndexTableOperator(n, WuList[i], false);
        }
        loadMetaTable();
    }

    public static void main(String[] args) throws IOException {
        Scanner scanner = new Scanner(System.in);
        System.out.print("Data Length = ");
        long n = scanner.nextLong();
        LongQueryEngine queryEngine = new LongQueryEngine(n);
        StatisticWriter.println("Offset,Length,Epsilon,T,T_1,T_2,#candidates,#answers");

        do {
            long offset;
            int length;
            double epsilon;
            do {
                System.out.print("Offset = ");
                offset = scanner.nextLong();
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

            // initialization: 0-T, 1-T_1, 2-T_2, 3-#candidates, 4-#answers
            List<StatisticInfo> statisticInfos = new ArrayList<>(5);
            for (int i = 0; i < 5; i++) {
                statisticInfos.add(new StatisticInfo());
            }

            // execute the query request
            queryEngine.query(statisticInfos, offset, length, epsilon);

            // output statistic information
            StatisticWriter.print(offset + "," + length + "," + epsilon + ",");
            for (int i = 0; i < 5; i++) {
                StatisticWriter.print(statisticInfos.get(i).getAverage() + ",");
            }
            StatisticWriter.println("");
        } while (true);
    }

    public boolean query(List<StatisticInfo> statistics, long offset, int length, double epsilon) throws IOException {
        // fetch corresponding subsequence from data series
        logger.info("Query offset: {}, length: {}, epsilon: {}", offset, length, epsilon);
        List<Float> queryData = timeSeriesOperator.readTimeSeries(offset, length);
        return query(statistics, queryData, epsilon);
    }

    public boolean query(List<StatisticInfo> statistics, List<Float> queryData, double epsilon) throws IOException {
        // initialization: clear cache
        if (ENABLE_INCREMENTAL_VISITING) {
            indexCaches.clear();
            for (int ignored : WuList) {
                indexCaches.add(new ArrayList<>());
            }
        }
        int length = queryData.size();

        long startTime = System.currentTimeMillis();

        // Phase 0: segmentation (DP)
        List<LongQuerySegment> queries = determineQueryPlan(queryData, epsilon);
        logger.debug("Query order: {}", queries);

        // Phase 1: index-probing
        long startTime1 = System.currentTimeMillis();

        List<LongInterval> validPositions = new ArrayList<>();  // CS

        int lastSegment = queries.get(queries.size() - 1).getOrder();
        double range0 = epsilon * epsilon;
        double lastMinimumEpsilon = 0;
        double lastTotalTimeUsageEstimated = Double.MAX_VALUE;
        for (int i = 0; i < queries.size(); i++) {
            LongQuerySegment query = queries.get(i);
            logger.debug("Disjoint window #{} - {} - mean:{}", i + 1, query.getOrder(), query.getMean());

            int deltaW = (i == queries.size() - 1) ? 0 : (queries.get(i + 1).getOrder() - query.getOrder()) * WuList[0];

            List<LongInterval> nextValidPositions = new ArrayList<>();  // CS

            // store possible current segment
            List<LongInterval> positions = new ArrayList<>();  // CS_i

            // query possible rows which mean is in distance range of i-th disjoint window
            double range = Math.sqrt((range0 - lastMinimumEpsilon) / query.getWu());
            double beginRound = LongMeanIntervalUtils.toRound(query.getMean() - range, statisticInfos.get(query.getWu() / WuList[0] - 1));
            double endRound = LongMeanIntervalUtils.toRound(query.getMean() + range);

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
                for (LongInterval position : positions) {
                    if (position.getRight() - (query.getOrder() - 1) * WuList[0] + length - 1 > n) {
                        if (position.getLeft() - (query.getOrder() - 1) * WuList[0] + length - 1 <= n) {
                            nextValidPositions.add(new LongInterval(position.getLeft() + deltaW, n - length + 1 + (query.getOrder() - 1) * WuList[0] + deltaW, position.getEpsilon()));
                        }
                    } else if (position.getLeft() - (query.getOrder() - 1) * WuList[0] < 1) {
                        if (position.getRight() - (query.getOrder() - 1) * WuList[0] >= 1) {
                            nextValidPositions.add(new LongInterval(1 + (query.getOrder() - 1) * WuList[0] + deltaW, position.getRight() + deltaW, position.getEpsilon()));
                        }
                    } else {
                        nextValidPositions.add(new LongInterval(position.getLeft() + deltaW, position.getRight() + deltaW, position.getEpsilon()));
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
                                nextValidPositions.add(new LongInterval(Math.max(validPositions.get(index1).getLeft(), positions.get(index2).getLeft()) + deltaW, validPositions.get(index1).getRight() + deltaW, sumEpsilon));
                                if (sumEpsilon < lastMinimumEpsilon) {
                                    lastMinimumEpsilon = sumEpsilon;
                                }
                            }
                            index1++;
                        } else {
                            if (sumEpsilon <= epsilon * epsilon) {
                                nextValidPositions.add(new LongInterval(Math.max(validPositions.get(index1).getLeft(), positions.get(index2).getLeft()) + deltaW, positions.get(index2).getRight() + deltaW, sumEpsilon));
                                if (sumEpsilon < lastMinimumEpsilon) {
                                    lastMinimumEpsilon = sumEpsilon;
                                }
                            }
                            index2++;
                        }
                    }
                }
            }

            Pair<List<LongInterval>, Pair<Integer, Integer>> candidates = sortButNotMergeIntervalsAndCount(nextValidPositions);
            validPositions = candidates.getFirst();
//            logger.info("next valid: {}", validPositions.toString());

            int cntCurrentDisjointCandidateWindows = candidates.getSecond().getFirst();
            int cntCurrentCandidateOffsets = candidates.getSecond().getSecond();
            logger.debug("Disjoint candidate windows: {}, candidate offsets: {}", cntCurrentDisjointCandidateWindows, cntCurrentCandidateOffsets);

            if (ENABLE_EARLY_TERMINATION) {
                int phase1TimeUsageUntilNow = (int) (System.currentTimeMillis() - startTime1);
                double phase2TimeUsageEstimated = PHASE_2_TIME_ESTIMATE_COEFFICIENT_A * cntCurrentDisjointCandidateWindows + PHASE_2_TIME_ESTIMATE_COEFFICIENT_B * cntCurrentCandidateOffsets / 100000 * length + PHASE_2_TIME_ESTIMATE_INTERCEPT;
                double totalTimeUsageEstimated = phase1TimeUsageUntilNow + phase2TimeUsageEstimated;
                logger.debug("Time usage: phase 1 until now: {}, phase 2 estimated: {}, total estimated: {}", phase1TimeUsageUntilNow, phase2TimeUsageEstimated, totalTimeUsageEstimated);

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

        List<Pair<Long, Double>> answers = new ArrayList<>();
        int cntCandidate = 0;
        for (LongInterval position : validPositions) {
            cntCandidate += position.getRight() - position.getLeft() + 1;

            long begin = position.getLeft() - (lastSegment - 1) * WuList[0];
            long end = position.getRight() - (lastSegment - 1) * WuList[0] + length - 1;
            logger.debug("Scan data [{}, {}]", begin, end);
            List<Float> data = timeSeriesOperator.readTimeSeries(begin, (int) (end - begin + 1));

            for (int i = 0; i + length - 1 < data.size(); i++) {
                double distance = 0;
                for (int j = 0; j < length && distance <= epsilon * epsilon; j++) {
                    distance += (data.get(i + j) - queryData.get(j)) * (data.get(i + j) - queryData.get(j));
                }
                if (distance <= epsilon * epsilon) {
                    answers.add(new Pair<>(begin + i, Math.sqrt(distance)));
                }
            }
        }

        long endTime2 = System.currentTimeMillis();
        statistics.get(0).append(endTime2 - startTime);   // total time usage
        statistics.get(1).append(endTime1 - startTime1);  // phase 1 time usage
        statistics.get(2).append(endTime2 - startTime2);  // phase 2 time usage
        statistics.get(3).append(cntCandidate);           // number of candidates
        statistics.get(4).append(answers.size());         // number of answers

//        answers.sort(Comparator.comparing(Pair::getSecond));

//        logger.info("Best: {}, distance: {}", answers.get(0).getFirst(), answers.get(0).getSecond());
        logger.info("T: {} ms, T_1: {} ms, T_2: {} ms, #candidates: {}, #answers: {}", endTime2 - startTime, endTime1 - startTime1, endTime2 - startTime2, cntCandidate, answers.size());
        return !answers.isEmpty();
    }

    private Pair<Long, Long> getCountsFromStatisticInfo(int Wu, double mean, double epsilon) {
        List<Pair<Double, Pair<Long, Long>>> statisticInfo = statisticInfos.get(Wu / WuList[0] - 1);

        double range = epsilon / Math.sqrt(Wu);
        double beginRound = LongMeanIntervalUtils.toRound(mean - range);
        double endRound = LongMeanIntervalUtils.toRound(mean + range);

        int index = Collections.binarySearch(statisticInfo, new Pair<>(beginRound, 0), Comparator.comparing(Pair::getFirst));
        index = index < 0 ? -(index + 1) : index;
        if (index >= statisticInfo.size()) index = statisticInfo.size() - 1;
        long lower1 = index > 0 ? statisticInfo.get(index - 1).getSecond().getFirst() : 0;
        long lower2 = index > 0 ? statisticInfo.get(index - 1).getSecond().getSecond() : 0;

        index = Collections.binarySearch(statisticInfo, new Pair<>(endRound, 0), Comparator.comparing(Pair::getFirst));
        index = index < 0 ? -(index + 1) : index;
        if (index >= statisticInfo.size()) index = statisticInfo.size() - 1;
        long upper1 = index > 0 ? statisticInfo.get(index).getSecond().getFirst() : 0;
        long upper2 = index > 0 ? statisticInfo.get(index).getSecond().getSecond() : 0;

        return new Pair<>(upper1 - lower1, upper2 - lower2);
    }

    private double getCost(int l, int r, double epsilon) {
        if (cost[l][r] != -1) return cost[l][r];
        int useWu = WuList[0] * (r - l + 1);
        double mean = (prefixSums[r] - (l > 0 ? prefixSums[l - 1] : 0)) / useWu;
        Pair<Long, Long> counts = getCountsFromStatisticInfo(useWu, mean, epsilon);
        cost[l][r] = 1.0 * counts.getFirst() / statisticInfos.get(100 / 25 - 1).get(statisticInfos.get(100 / 25 - 1).size() - 1).getSecond().getFirst();
        cost[l][r] = Math.log(cost[l][r]);
        cost2[l][r] = counts.getFirst();
        return cost[l][r];
    }

    private long getCost2(int l, int r, double epsilon) {
        if (cost2[l][r] != -1) return cost2[l][r];
        int useWu = WuList[0] * (r - l + 1);
        double mean = (prefixSums[r] - (l > 0 ? prefixSums[l - 1] : 0)) / useWu;
        Pair<Long, Long> counts = getCountsFromStatisticInfo(useWu, mean, epsilon);
        cost2[l][r] = counts.getFirst();
        return cost2[l][r];
    }

    private List<LongQuerySegment> determineQueryPlan(List<Float> queryData, double epsilon) {
        int m = queryData.size() / WuList[0];

        // calculate mean and sum of each disjoint window
        List<Double> sums = new ArrayList<>(m);
        double ex = 0;
        for (int i = 0; i < queryData.size(); i++) {
            ex += queryData.get(i);
            if ((i + 1) % WuList[0] == 0) {
                sums.add(ex);
                ex = 0;
            }
        }

        // initialize for dynamic programming algorithm
        double[][] dp = new double[m + 1][];
        int[][] pre = new int[m + 1][];
        cost = new double[m][];
        cost2 = new long[m][];
        prefixSums = new double[m];
        prefixSums[0] = sums.get(0);
        for (int i = 0; i <= m; i++) {
            dp[i] = new double[m + 1];
            Arrays.fill(dp[i], Double.MAX_VALUE);
            pre[i] = new int[m + 1];
            Arrays.fill(pre[i], -1);
        }
        for (int i = 0; i < m; i++) {
            cost[i] = new double[m];
            Arrays.fill(cost[i], -1);
            cost2[i] = new long[m];
            Arrays.fill(cost2[i], -1);
            if (i > 0) {
                prefixSums[i] = prefixSums[i - 1] + sums.get(i);
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
        List<LongQuerySegment> queries = new ArrayList<>();
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
            double mean = (prefixSums[r] - (l > 0 ? prefixSums[l - 1] : 0)) / useWu;
            queries.add(new LongQuerySegment(mean, l + 1, getCost2(l, r, epsilon), useWu));
            index -= pre[index][i];
        }

        if (ENABLE_QUERY_REORDERING) {
            // optimize query order
            queries.sort(Comparator.comparingLong(LongQuerySegment::getCount));
        }

        return queries;
    }

    @SuppressWarnings("SameParameterValue")
    private void scanIndex(double begin, boolean beginInclusive, double end, boolean endInclusive,
                           LongQuerySegment query, List<LongInterval> positions) throws IOException {
        if (!beginInclusive) begin = begin + 0.01;
        if (endInclusive) end = end + 0.01;

        Map<Double, LongIndexNode> indexes = indexOperators[query.getWu() / WuList[0] - 1].readIndexes(begin, end);
        for (Map.Entry<Double, LongIndexNode> entry : indexes.entrySet()) {
            double meanRound = entry.getKey();
            double lowerBound = getDistanceLowerBound(query, meanRound);
            for (Pair<Long, Long> position : entry.getValue().getPositions()) {
                positions.add(new LongInterval(position.getFirst(), position.getSecond(), query.getWu() * lowerBound));
            }
        }
    }

    private void scanIndexAndAddCache(double begin, boolean beginInclusive, double end, boolean endInclusive, int index, LongQuerySegment query, List<LongInterval> positions) throws IOException {
        if (index < 0) {
            index = -index - 1;
            indexCaches.get(query.getWu() / WuList[0] - 1).add(index, new LongIndexCache(begin, end));
        }

        if (!beginInclusive) begin = begin + 0.01;
        if (endInclusive) end = end + 0.01;

        Map<Double, LongIndexNode> indexes = indexOperators[query.getWu() / WuList[0] - 1].readIndexes(begin, end);
        for (Map.Entry<Double, LongIndexNode> entry : indexes.entrySet()) {
            double meanRound = entry.getKey();
            double lowerBound = getDistanceLowerBound(query, meanRound);
            for (Pair<Long, Long> position : entry.getValue().getPositions()) {
                positions.add(new LongInterval(position.getFirst(), position.getSecond(), query.getWu() * lowerBound));
            }

            indexCaches.get(query.getWu() / WuList[0] - 1).get(index).addCache(meanRound, entry.getValue());
        }
    }

    @SuppressWarnings("SameParameterValue")
    private void scanCache(int index, double begin, boolean beginInclusive, double end, boolean endInclusive,
                           LongQuerySegment query, List<LongInterval> positions) {
        for (Map.Entry<Double, LongIndexNode> entry : indexCaches.get(query.getWu() / WuList[0] - 1).get(index).getCaches().subMap(begin, beginInclusive, end, endInclusive).entrySet()) {
            double meanRound = entry.getKey();
            LongIndexNode indexNode = entry.getValue();
            double lowerBound = getDistanceLowerBound(query, meanRound);
            for (Pair<Long, Long> position : indexNode.getPositions()) {
                positions.add(new LongInterval(position.getFirst(), position.getSecond(), query.getWu() * lowerBound));
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
            LongIndexCache cache = indexCaches.get(index).get(i);
            if (cache.getBeginRound() > round) {
                return -i - 1;
            }
            if (cache.getBeginRound() <= round && cache.getEndRound() >= round) {
                return i;
            }
        }

        return -1;
    }

    private double getDistanceLowerBound(LongQuerySegment query, double meanLower) {
        double meanUpper = LongMeanIntervalUtils.toUpper(meanLower, statisticInfos.get(query.getWu() / WuList[0] - 1));

        double delta;
        if (meanLower > query.getMean()) {
            delta = (meanLower - query.getMean()) * (meanLower - query.getMean());
        } else if (meanUpper < query.getMean()) {
            delta = (query.getMean() - meanUpper) * (query.getMean() - meanUpper);
        } else {
            delta = 0;
        }

        return delta;
    }

    private List<LongInterval> sortButNotMergeIntervals(List<LongInterval> intervals) {
        if (intervals.size() <= 1) {
            return intervals;
        }

        intervals.sort(Comparator.comparingLong(LongInterval::getLeft));

        LongInterval first = intervals.get(0);
        long start = first.getLeft();
        long end = first.getRight();
        double epsilon = first.getEpsilon();

        List<LongInterval> result = new ArrayList<>();

        for (int i = 1; i < intervals.size(); i++) {
            LongInterval current = intervals.get(i);
            if (current.getLeft() - 1 < end || (current.getLeft() - 1 == end && Math.abs(current.getEpsilon() - epsilon) < 1)) {
                end = Math.max(current.getRight(), end);
                epsilon = Math.min(current.getEpsilon(), epsilon);
            } else {
                result.add(new LongInterval(start, end, epsilon));
                start = current.getLeft();
                end = current.getRight();
                epsilon = current.getEpsilon();
            }
        }
        result.add(new LongInterval(start, end, epsilon));

        return result;
    }

    private Pair<List<LongInterval>, Pair<Integer, Integer>> sortButNotMergeIntervalsAndCount(List<LongInterval> intervals) {
        if (intervals.size() <= 1) {
            return new Pair<>(intervals, new Pair<>(intervals.size(), intervals.isEmpty() ? 0 : (int) (intervals.get(0).getRight() - intervals.get(0).getLeft() + 1)));
        }

        intervals.sort(Comparator.comparingLong(LongInterval::getLeft));

        LongInterval first = intervals.get(0);
        long start = first.getLeft();
        long end = first.getRight();
        double epsilon = first.getEpsilon();

        List<LongInterval> result = new ArrayList<>();

        int cntDisjointIntervals = intervals.size();
        int cntOffsets = 0;
        for (int i = 1; i < intervals.size(); i++) {
            LongInterval current = intervals.get(i);

            if (current.getLeft() - 1 <= end) {  // count for disjoint intervals to estimate time usage of phase 2
                cntDisjointIntervals--;
            }

            if (current.getLeft() - 1 < end || (current.getLeft() - 1 == end && Math.abs(current.getEpsilon() - epsilon) < 1)) {
                end = Math.max(current.getRight(), end);
                epsilon = Math.min(current.getEpsilon(), epsilon);
            } else {
                result.add(new LongInterval(start, end, epsilon));
                cntOffsets += end - start + 1;
                start = current.getLeft();
                end = current.getRight();
                epsilon = current.getEpsilon();
            }
        }
        result.add(new LongInterval(start, end, epsilon));
        cntOffsets += end - start + 1;

        return new Pair<>(result, new Pair<>(cntDisjointIntervals, cntOffsets));
    }

    private List<LongInterval> sortAndMergeIntervals(List<LongInterval> intervals) {
        if (intervals.size() <= 1) {
            return intervals;
        }

        intervals.sort(Comparator.comparingLong(LongInterval::getLeft));

        LongInterval first = intervals.get(0);
        long start = first.getLeft();
        long end = first.getRight();
        double epsilon = first.getEpsilon();

        List<LongInterval> result = new ArrayList<>();

        for (int i = 1; i < intervals.size(); i++) {
            LongInterval current = intervals.get(i);
            if (current.getLeft() - 1 <= end) {
                end = Math.max(current.getRight(), end);
                epsilon = Math.min(current.getEpsilon(), epsilon);
            } else {
                result.add(new LongInterval(start, end, epsilon));
                start = current.getLeft();
                end = current.getRight();
                epsilon = current.getEpsilon();
            }
        }
        result.add(new LongInterval(start, end, epsilon));

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
                logger.info("Loading metadata for index {}-{}", n, WuList[i]);
                statisticInfos.add(indexOperators[i].readStatisticInfo());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
