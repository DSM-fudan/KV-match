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

import cn.edu.fudan.dsm.kvmatch.common.Pair;
import cn.edu.fudan.dsm.kvmatch.common.entity.TimeSeriesNode;
import cn.edu.fudan.dsm.kvmatch.utils.DtwUtils;
import cn.edu.fudan.dsm.kvmatch.common.Index;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.util.*;

public class UcrDtwQueryExecutor {

    private static final Logger logger = LoggerFactory.getLogger(UcrDtwQueryExecutor.class);

    private List<Double> queryData;
    private int dataIndex = 0, cnt = 0;
    private int N, M;
    private double Epsilon;
    private int Rho;
    private double Alpha, Beta;

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

    public UcrDtwQueryExecutor(int N, int M, List<Double> queryData, double Epsilon, int Rho, double Alpha, double Beta, Iterator scanner) {
        this.scanner = scanner;
        this.M = M;
        this.N = N;
        this.queryData = queryData;
        this.Epsilon = Epsilon;
        this.Rho = Rho;
        this.Alpha = Alpha;
        this.Beta = Beta;
    }

    public int run() {
        double[] t, q;  // data array and query array
        int[] order;    // new order of the query
        double[] u, l, qo, uo, lo, tz, cb, cb1, cb2;

        double d;
        double ex, ex2, meanQ, stdQ;
//        int kim = 0, keogh = 0, keogh2 = 0;
        double dist, lb_kim, lb_k, lb_k2;
        Index[] Q_tmp;
        double[] buffer, u_buff, l_buff;

        // For every EPOCH points, all cumulative values, such as ex (sum), ex2 (sum square), will be restarted for reducing the floating point error.
        int EPOCH = 100000;

        // malloc everything here
        q = new double[M];
        qo = new double[M];
        uo = new double[M];
        lo = new double[M];
        order = new int[M];
        Q_tmp = new Index[M];
        u = new double[M];
        l = new double[M];
        cb = new double[M];
        cb1 = new double[M];
        cb2 = new double[M];
        t = new double[M * 2];
        tz = new double[M];
        buffer = new double[EPOCH];
        u_buff = new double[EPOCH];
        l_buff = new double[EPOCH];

        // Read query file
        ex = ex2 = 0;

        for (int i = 0; i < M; i++) {
            d = queryData.get(i);
            ex += d;
            ex2 += d * d;
            q[i] = d;
        }

        // Do z-normalize the query, keep in same array, q
        meanQ = ex / M;
        stdQ = ex2 / M;
        stdQ = Math.sqrt(stdQ - meanQ * meanQ);
        for (int i = 0; i < M; i++) {
            q[i] = (q[i] - meanQ) / stdQ;
        }

        // Create envelop of the query: lower envelop, l, and upper envelop, u
        DtwUtils.lowerUpperLemire(q, M, Rho, l, u);

        // Sort the query one time by abs(z-norm(q[i]))
        for (int i = 0; i < M; i++) {
            Q_tmp[i] = new Index(q[i], i);
        }
        Arrays.sort(Q_tmp, (o1, o2) -> {
            // Sorting function for the query, sort by abs(z_norm(q[i])) from high to low
            return (int) (Math.abs(o2.value) - Math.abs(o1.value));
        });

        // also create another arrays for keeping sorted envelop
        for (int i = 0; i < M; i++) {
            int o = Q_tmp[i].index;
            order[i] = o;
            qo[i] = q[o];
            uo[i] = u[o];
            lo[i] = l[o];
        }

        // Initial the cumulative lower bound
        for (int i = 0; i < M; i++) {
            cb[i] = 0;
            cb1[i] = 0;
            cb2[i] = 0;
        }

        boolean done = false;
        int it = 0, ep;
        int I;    // the starting index of the data in current chunk of size EPOCH

        List<Pair<Integer, Double>> answers = new ArrayList<>();

        while (!done) {
            // Read first m-1 points
            if (it == 0) {
                for (int k = 0; k < M - 1; k++) {
                    try {
                        if (!nextData()) throw new EOFException();
                        d = getCurrentData();
                        buffer[k] = d;
                    } catch (EOFException e) {
                        // do nothing
                    }
                }
            } else {
                for (int k = 0; k < M - 1; k++) {
                    buffer[k] = buffer[EPOCH - M + 1 + k];
                }
            }

            // Read buffer of size EPOCH or when all data has been read.
            ep = M - 1;
            while (ep < EPOCH) {
                try {
                    if (!nextData()) throw new EOFException();
                    d = getCurrentData();
                    buffer[ep] = d;
                    ep++;
                } catch (EOFException e) {
                    break;
                }
            }

            // Data are read in chunk of size EPOCH.
            // When there is nothing to read, the loop is end.
            if (ep <= M - 1) {
                done = true;
            } else {
                DtwUtils.lowerUpperLemire(buffer, ep, Rho, l_buff, u_buff);

                // Just for printing a dot for approximate a million point. Not much accurate.
//                if (it % (1000000 / (EPOCH - M + 1)) == 0) {
//                    System.err.print(".");
//                }

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
                    t[i % M] = d;

                    // Double the size for avoiding using modulo "%" operator
                    t[(i % M) + M] = d;

                    // Start the task when there are more than m-1 points in the current chunk
                    if (i >= M - 1) {
                        double mean = ex / M;
                        double std = ex2 / M;
                        std = Math.sqrt(std - mean * mean);

                        // compute the start location of the data in the current circular array, t
                        int j = (i + 1) % M;
                        // the start location of the data in the current chunk
                        I = i - (M - 1);

                        if (Math.abs(mean - meanQ) <= Beta && (std / stdQ) <= Alpha && (std / stdQ) >= 1.0 / Alpha) {  //  test single point range criterion

                            // Use a constant lower bound to prune the obvious sub-sequence
                            lb_kim = DtwUtils.lbKimHierarchy(t, q, j, M, mean, std, Epsilon * Epsilon);

                            if (lb_kim <= Epsilon * Epsilon) {
                                // Use a linear time lower bound to prune; z_normalization of t will be computed on the fly.
                                // uo, lo are envelop of the query.
                                lb_k = DtwUtils.lbKeoghCumulative(order, t, uo, lo, cb1, j, M, mean, std, Epsilon * Epsilon);
                                if (lb_k <= Epsilon * Epsilon) {
                                    // Take another linear time to compute z_normalization of t.
                                    // Note that for better optimization, this can merge to the previous function.
                                    for (int k = 0; k < M; k++) {
                                        tz[k] = (t[(k + j)] - mean) / std;
                                    }

                                    // Use another lb_keogh to prune
                                    // qo is the sorted query. tz is unsorted z_normalized data.
                                    // l_buff, u_buff are big envelop for all data in this chunk
                                    lb_k2 = DtwUtils.lbKeoghDataCumulative(order, qo, cb2, I, l_buff, u_buff, M, mean, std, Epsilon * Epsilon);
                                    if (lb_k2 <= Epsilon * Epsilon) {
                                        // Choose better lower bound between lb_keogh and lb_keogh2 to be used in early abandoning DTW
                                        // Note that cb and cb2 will be cumulative summed here.
                                        if (lb_k > lb_k2) {
                                            cb[M - 1] = cb1[M - 1];
                                            for (int k = M - 2; k >= 0; k--) {
                                                cb[k] = cb[k + 1] + cb1[k];
                                            }
                                        } else {
                                            cb[M - 1] = cb2[M - 1];
                                            for (int k = M - 2; k >= 0; k--) {
                                                cb[k] = cb[k + 1] + cb2[k];
                                            }
                                        }

                                        // Compute DTW and early abandoning if possible
                                        dist = DtwUtils.dtw(tz, q, cb, M, Rho, Epsilon * Epsilon);
                                        if (dist <= Epsilon * Epsilon) {
                                            answers.add(new Pair<>((it) * (EPOCH - M + 1) + i - M + 1, Math.sqrt(dist)));
                                        }
//                                    } else {
//                                        keogh2++;
                                    }
//                                } else {
//                                    keogh++;
                                }
//                            } else {
//                                kim++;
                            }
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

        answers.sort(Comparator.comparing(Pair::getSecond));

        if (!answers.isEmpty()) {
            logger.info("Best: {}, distance: {}", answers.get(0).getFirst(), answers.get(0).getSecond());
        } else {
            logger.warn("No sub-sequence within distance {}.", Epsilon);
        }
        return answers.size();
    }
}
