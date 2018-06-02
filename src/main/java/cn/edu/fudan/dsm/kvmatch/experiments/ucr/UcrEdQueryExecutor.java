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
package cn.edu.fudan.dsm.kvmatch.experiments.ucr;

import cn.edu.fudan.dsm.kvmatch.common.Pair;
import cn.edu.fudan.dsm.kvmatch.common.entity.TimeSeriesNode;
import cn.edu.fudan.dsm.kvmatch.common.Index;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class UcrEdQueryExecutor {

    private static final Logger logger = LoggerFactory.getLogger(UcrEdQueryExecutor.class);

    private List<Double> queryData;
    private int dataIndex = 0, cnt = 0;
    private int N, M;
    private double Epsilon, Alpha, Beta;

    private double[] Q;             // query array

    private double ex = 0;
    private double ex2 = 0;

    private Iterator scanner;
    private TimeSeriesNode node = new TimeSeriesNode();

    @SuppressWarnings("Duplicates")
    private boolean nextData() {
        if (dataIndex + 1 < node.getData().size()) {
            dataIndex++;
            return ++cnt <= N;
        } else {
            Object result = scanner.next();
            if (result != null) {
                if (result instanceof Result) {  // HBase table
                    Result result1 = (Result) result;
                    node = new TimeSeriesNode();
                    node.parseBytes(result1.getValue(Bytes.toBytes("info"), Bytes.toBytes("d")));
                    dataIndex = 0;
                } else {  // local file
                    Pair result1 = (Pair) result;
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

    /**
     * Main function for calculating ED distance between the query, Q, and current data, T.
     * Note that Q is already sorted by absolute z-normalization value, |z_norm(Q[i])|
     */
    private double distance(double[] Q, double[] T, int j, int m, double mean, double std, int[] order, double bsf) {
        double sum = 0;
        for (int i = 0; i < m && sum < bsf; i++) {
            double x = (T[(order[i] + j)] - mean) / std;
            sum += (x - Q[i]) * (x - Q[i]);
        }
        return sum;
    }

    public UcrEdQueryExecutor(int N, int M, List<Double> queryData, double Epsilon, double Alpha, double Beta, Iterator scanner) {
        this.scanner = scanner;
        this.M = M;
        this.N = N;
        this.queryData = queryData;
        this.Epsilon = Epsilon;
        this.Alpha = Alpha;
        this.Beta = Beta;

        // Array for keeping the query data
        Q = new double[M];
    }

    @SuppressWarnings("Duplicates")
    public int run() {
        // Read the query data from HBase and calculate its statistic such as mean, std
        for (int i = 0; i < M; i++) {
            double d = queryData.get(i);
            ex += d;
            ex2 += d * d;
            Q[i] = d;
        }

        // Do z_normalization on query data
        double meanQ = ex / M;
        double stdQ = ex2 / M;
        stdQ = Math.sqrt(stdQ - meanQ * meanQ);
        for (int i = 0; i < M; i++) {
            Q[i] = (Q[i] - meanQ) / stdQ;
        }

        // Sort the query data
        int[] order = new int[M];
        Index[] Q_tmp = new Index[M];
        for (int i = 0; i < M; i++) {
            Q_tmp[i] = new Index(Q[i], i);
        }
        Arrays.sort(Q_tmp, (o1, o2) -> {
            // Comparison function for sorting the query.
            // The query will be sorted by absolute z-normalization value, |z_norm(Q[i])| from high to low.
            return Double.compare(Math.abs(o2.value), Math.abs(o1.value));
        });
        for (int i = 0; i < M; i++) {
            Q[i] = Q_tmp[i].value;
            order[i] = Q_tmp[i].index;
        }

        // Array for keeping the current data; Twice the size for removing modulo (circulation) in distance calculation
        double[] t = new double[2 * M];

        double dist;
        int i = 0, j;
        ex = ex2 = 0;

        List<Pair<Integer, Double>> answers = new ArrayList<>();

        // Read data from HBase, one value at a time
        while (nextData()) {
            double d = getCurrentData();
            ex += d;
            ex2 += d * d;
            t[i % M] = d;
            t[(i % M) + M] = d;

            // If there is enough data in T, the ED distance can be calculated
            if (i >= M - 1) {
                // the current starting location of T
                j = (i + 1) % M;

                // Z_norm(T[i]) will be calculated on the fly
                double mean = ex/M;
                double std = ex2/M;
                std = Math.sqrt(std - mean * mean);

                if (Math.abs(mean - meanQ) <= Beta && (std / stdQ) <= Alpha && (std / stdQ) >= 1.0 / Alpha) {  //  test single point range criterion

                    // Calculate ED distance
                    dist = distance(Q, t, j, M, mean, std, order, Epsilon * Epsilon);
                    if (dist <= Epsilon * Epsilon) {
                        answers.add(new Pair<>(i - M + 2, Math.sqrt(dist)));
                    }
                }
                ex -= t[j];
                ex2 -= t[j]* t[j];
            }
            i++;
        }

        answers.sort(Comparator.comparing(Pair::getSecond));

        if (!answers.isEmpty()) {
            logger.info("Best: {}, distance: {}", answers.get(0).getFirst(), answers.get(0).getSecond());
        } else {
            logger.warn("No sub-sequence within distance {}.", Epsilon);
        }
        return answers.size();
    }
}
