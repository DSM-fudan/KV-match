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
package cn.edu.fudan.dsm.kvmatch.experiments.ucr;

import cn.edu.fudan.dsm.kvmatch.common.Index;
import cn.edu.fudan.dsm.kvmatch.common.Pair;
import cn.edu.fudan.dsm.kvmatch.common.entity.TimeSeriesNode;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class PaaUcrEdQueryExecutor {

    private static final Logger logger = LoggerFactory.getLogger(PaaUcrEdQueryExecutor.class);

    private List<Double> queryData;
    private int dataIndex = 0, cnt = 0;
    private int N, M, Phi;
    private double Epsilon, Alpha, Beta;

    private double[] Q;   // query array
    private double[] eQ, eT;  // for EDBT PAA

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
    private double distance(double[] T, int j, double mean, double std, int[] order, double bsf) {
        double sum = 0;
        for (int i = 0; i < M && sum <= bsf; i++) {
            double x = (T[(order[i] + j + 1) % M] - mean) / std;
            sum += (x - Q[i]) * (x - Q[i]);
        }
        return sum;
    }

    private double lbPaaED(double[] Pre, int x, double tmpPre, double mean, double std) {
        int pSize = M / Phi;
        int st = x - M + 1;
        double tmp = tmpPre;

        for (int i = 0; i < Phi - 1; i++) {
            eT[i] = Pre[(st + (i+1)*pSize - 1) % M] - tmp;
            eT[i] = (eT[i] / pSize - mean) / std;
            tmp = Pre[(st + (i+1)*pSize - 1) % M];
        }
        eT[Phi - 1] = Pre[x % M] - tmp;
        eT[Phi - 1] = (eT[Phi - 1] / (M - (Phi-1)*pSize) - mean) / std;

        double lb = 0;
        for (int i = 0; i < Phi - 1; i++) {
            lb += (eT[i] - eQ[i]) * (eT[i] - eQ[i]) * pSize;
        }
        lb += (eT[Phi - 1] - eQ[Phi - 1]) * (eT[Phi - 1] - eQ[Phi - 1]) * (M - (Phi-1)*pSize);
        return Math.sqrt(lb);
    }

    private double ED(double S1, double S2, double S3, double S4, double S5) {
        double tmp = (M * S5 - S1 * S2) / (Math.sqrt(M * S3 - S1*S1) * Math.sqrt(M * S4 - S2*S2));
        return Math.sqrt(2 * M * (1.0 - tmp));
    }

    public PaaUcrEdQueryExecutor(int N, int M, List<Double> queryData, double Epsilon, double Alpha, double Beta, int Phi, Iterator scanner) {
        this.scanner = scanner;
        this.M = M;
        this.N = N;
        this.queryData = queryData;
        this.Epsilon = Epsilon;
        this.Alpha = Alpha;
        this.Beta = Beta;
        this.Phi = Phi;

        // Array for keeping the query data
        Q = new double[M];
        eQ = new double[Phi];
        eT = new double[Phi];
    }

    @SuppressWarnings("Duplicates")
    public int run() {
        List<Pair<Integer, Double>> answers = new ArrayList<>();

        // Read the query data from HBase and calculate its statistic such as mean, std
        for (int i = 0; i < M; i++) {
            double d = queryData.get(i);
            ex += d;
            ex2 += d * d;
            Q[i] = d;
        }

        // Calculate mean and std
        double meanQ = ex / M;
        double stdQ = ex2 / M;
        stdQ = Math.sqrt(stdQ - meanQ * meanQ);

        // Build q_PAA
        int pSize = M / Phi;
        for (int i = 0; i < Phi - 1; i++) {
            eQ[i] = 0;
            for (int j = i*pSize; j < (i+1)*pSize; j++) {
                eQ[i] = eQ[i] + Q[j];
            }
            eQ[i] = (eQ[i] * 1.0 / pSize - meanQ) / stdQ;
        }
        eQ[Phi - 1] = 0;
        for (int j = (Phi - 1)*pSize; j < M; j++) {
            eQ[Phi - 1] = eQ[Phi - 1] + Q[j];
        }
        eQ[Phi - 1] = (eQ[Phi - 1] * 1.0 / (M - (Phi - 1)*pSize) - meanQ) / stdQ;

        // Normalize Q
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
            // return Double.compare(Math.abs(o2.value), Math.abs(o1.value));
            return Double.compare(o2.value, o1.value);
        });
        for (int i = 0; i < M; i++) {
            Q[i] = Q_tmp[i].value;
            order[i] = Q_tmp[i].index;
        }

        // Array for keeping the current data; Twice the size for removing modulo (circulation) in distance calculation
        double[] t = new double[2 * M];
        double[] Pre = new double[M];

        nextData();
        t[0] = getCurrentData();
        ex = t[0];
        ex2 = t[0] * t[0];
        Pre[0] = t[0];
        double S1 = 0, S2 = t[0], S3 = 0, S4 = t[0]*t[0], S5 = 0;  // T(c-1), T(c), T(c-1)^2, T(c)^2, T(c-1)*T(c)
        double preTii = 0, preTi = 0, tmpPre = 0, lbED = 0;

        for (int i = 1; i < N; i++) {
            if (i % M == 0) {
                for (int j = M-1; j >= 0; j--) {
                    Pre[j] = Pre[j] - Pre[0];
                }
                tmpPre = 0;
            } else if (i > M) {
                tmpPre = tmpPre + t[i % M];
            }

            nextData();
            double d = getCurrentData();
            t[i % M] = d;
            Pre[i % M] = Pre[(i-1) % M] + d;
            ex += d;
            ex2 += d * d;

            S1 = S2;
            S2 = S2 - preTi + d;
            S3 = S4;
            S4 = S4 - preTi*preTi + d * d;
            S5 = S5 - preTi*preTii + t[(i-1) % M] * d;

            if (i >= M - 1) {
                double mean = ex / M;
                double std = ex2 / M;
                std = Math.sqrt(std - mean * mean);

                if (lbED > Epsilon * Epsilon) {
                    lbED -= ED(S1, S2, S3, S4, S5);
                } else {
                    if (Math.abs(mean - meanQ) <= Beta && (std / stdQ) <= Alpha && (std / stdQ) >= 1.0 / Alpha) {  //  test single point range criterion
                        // LB_PAA (EDBT 2017)
                        lbED = lbPaaED(Pre, i, tmpPre, mean, std);
                        if (lbED <= Epsilon) {
                            // Calculate ED distance
                            double dist = distance(t, i, mean, std, order, Epsilon * Epsilon);
                            if (dist <= Epsilon * Epsilon) {
                                answers.add(new Pair<>(i - M + 1, Math.sqrt(dist)));
                            }
                            lbED = Math.sqrt(dist);
                        }
                    }
                }

                preTii = preTi;
                preTi = t[(i - M + 1) % M];
                ex -= t[(i - M + 1) % M];
                ex2 -= t[(i - M + 1) % M] * t[(i - M + 1) % M];
            }
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
