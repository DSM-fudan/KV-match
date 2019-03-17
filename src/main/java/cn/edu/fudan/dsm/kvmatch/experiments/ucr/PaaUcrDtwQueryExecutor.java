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
import cn.edu.fudan.dsm.kvmatch.utils.DtwUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

@SuppressWarnings("Duplicates")
public class PaaUcrDtwQueryExecutor {

    private static final double INF = 1e20;

    private static final Logger logger = LoggerFactory.getLogger(PaaUcrDtwQueryExecutor.class);

    private List<Double> queryData;
    private int dataIndex = 0, cnt = 0;
    private int N, M, Phi;
    private double Epsilon;
    private int Rho;
    private double Alpha, Beta;

    private double[] Q, Qu, Ql;   // query array
    private Index[] QQ;
    private double[] eQ, eT, eQu, eQl;  // for EDBT PAA

    private Iterator scanner;
    private TimeSeriesNode node = new TimeSeriesNode();

    public PaaUcrDtwQueryExecutor(int N, int M, List<Double> queryData, double Epsilon, int Rho, double Alpha, double Beta, int Phi, Iterator scanner) {
        this.scanner = scanner;
        this.M = M;
        this.N = N;
        this.queryData = queryData;
        this.Epsilon = Epsilon;
        this.Rho = Rho;
        this.Alpha = Alpha;
        this.Beta = Beta;
        this.Phi = Phi;

        Q = new double[M];
        QQ = new Index[M];
        Qu = new double[M];
        Ql = new double[M];
        eQ = new double[Phi];
        eT = new double[Phi];
        eQu = new double[Phi];
        eQl = new double[Phi];
    }

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

    @SuppressWarnings("Duplicates")
    public int run() {
        double ex = 0, ex2 = 0;
        for (int i = 0; i < M; i++) {
            double d = queryData.get(i);
            ex += d;
            ex2 += d * d;
            Q[i] = d;
        }

        // Calculate mean and std
        double meanQ = ex / M;
        double stdQ = Math.sqrt(ex2 / M - meanQ * meanQ);

        // Build q_PAA
        int pSize = M / Phi;
        for (int i = 0; i < Phi - 1; i++) {
            eQ[i] = 0;
            for (int j = i * pSize; j < (i + 1) * pSize; j++) {
                eQ[i] = eQ[i] + Q[j];
            }
            eQ[i] = (eQ[i] * 1.0 / pSize - meanQ) / stdQ;
        }
        eQ[Phi - 1] = 0;
        for (int j = (Phi - 1) * pSize; j < M; j++) {
            eQ[Phi - 1] = eQ[Phi - 1] + Q[j];
        }
        eQ[Phi - 1] = (eQ[Phi - 1] * 1.0 / (M - (Phi - 1) * pSize) - meanQ) / stdQ;

        // Normalize q
        for (int i = 0; i < M; i++) {
            Q[i] = (Q[i] - meanQ) / stdQ;
        }

        // Create envelop of the query: lower envelop, l, and upper envelop, u
        DtwUtils.lowerUpperLemire(Q, M, Rho, Ql, Qu);

        // Get Qu/l_PAA
        for (int i = 0; i < Phi - 1; i++) {
            eQu[i] = 0;
            eQl[i] = 0;
            for (int j = i * pSize; j < (i + 1) * pSize; j++) {
                eQu[i] = eQu[i] + Qu[j];
                eQl[i] = eQl[i] + Ql[j];
            }
            eQu[i] = eQu[i] / pSize;
            eQl[i] = eQl[i] / pSize;
        }
        eQu[Phi - 1] = 0;
        eQl[Phi - 1] = 0;
        for (int j = (Phi - 1) * pSize; j < M; j++) {
            eQu[Phi - 1] = eQu[Phi - 1] + Qu[j];
            eQl[Phi - 1] = eQl[Phi - 1] + Ql[j];
        }
        eQu[Phi - 1] = eQu[Phi - 1] / (M - (Phi - 1) * pSize);
        eQl[Phi - 1] = eQl[Phi - 1] / (M - (Phi - 1) * pSize);

        // Sort the query one time by abs(z-norm(q[i]))
        for (int i = 0; i < M; i++) {
            QQ[i] = new Index(Q[i], i);
        }
        Arrays.sort(QQ, (o1, o2) -> {
            // Sorting function for the query, sort by abs(z_norm(q[i])) from high to low
            return Double.compare(o2.value, o1.value);
        });

        List<Pair<Integer, Double>> answers = new ArrayList<>();

        double[] t = new double[M];
        nextData();
        t[0] = getCurrentData();
        ex = t[0];
        ex2 = t[0] * t[0];
        double[] Pre = new double[M];
        Pre[0] = t[0];
        double S1, S2 = t[0], S3, S4 = t[0] * t[0], S5 = 0;  // T(c-1), T(c), T(c-1)^2, T(c)^2, T(c-1)*T(c)
        double preTii = 0, preTi = 0, tmpPre = 0, lbDTW = INF;

        for (int i = 1; i < N; i++) {
            if (i % M == 0) {
                for (int j = M - 1; j >= 0; j--) {
                    Pre[j] = Pre[j] - Pre[0];
                }
                tmpPre = 0;
            } else if (i > M) {
                tmpPre = tmpPre + t[i % M];
            }

            nextData();
            double d = getCurrentData();
            t[i % M] = d;
            Pre[i % M] = Pre[(i - 1) % M] + d;
            ex += d;
            ex2 += d * d;

            S1 = S2;
            S2 = S2 - preTi + d;
            S3 = S4;
            S4 = S4 - preTi * preTi + d * d;
            S5 = S5 - preTi * preTii + t[(i - 1) % M] * d;

            if (i >= M - 1) {
                double mean = ex / M;
                double std = ex2 / M;
                std = Math.sqrt(std - mean * mean);

                if (Math.abs(mean - meanQ) <= Beta && (std / stdQ) <= Alpha && (std / stdQ) >= 1.0 / Alpha) {  //  test single point range criterion
                    double lb_kim = lbKim(t, i, mean, std);
                    if (lb_kim <= Epsilon * Epsilon) {
                        if (lbDTW > Epsilon) {
                            lbDTW -= ED(S1, S2, S3, S4, S5);
                        } else {
                            lbDTW = lbPaaDTW(Pre, i, tmpPre, mean, std);
                            if (lbDTW <= Epsilon) {
                                double[] cb = new double[M];
                                double lb_EQ = lbDTW = lbEQ(t, i, mean, std, cb);
                                if (lb_EQ <= Epsilon) {
                                    double[] cbb = new double[M];
                                    double lb_ET = lbET(t, i, mean, std, cbb);
                                    if (lb_ET <= Epsilon) {
                                        double dist = getDTW(t, i, mean, std, cb, cbb);
                                        if (dist <= Epsilon * Epsilon) {
                                            lbDTW = Math.sqrt(dist);
                                            answers.add(new Pair<>(i - M + 1, lbDTW));
                                        }
                                    }
                                }
                            }
                        }
                    } else {
                        lbDTW = 0;
                    }
                } else {
                    lbDTW = 0;
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

    private double getDTW(double[] t, int x, double mean, double std, double[] cb, double[] cbb) {
        int st = x - M + 1;
        double[][] f = new double[2][];
        for (int i = 0; i < 2; i++) {
            f[i] = new double[M + 10];
        }

        for (int i = M - 2; i >= 0; i--) {
            cb[i] = cb[i] + cb[i + 1];
        }

        double[] tc = new double[M + 10];
        for (int i = 0; i < M; i++) {
            tc[i] = (t[(st + i) % M] - mean) / std;
        }
        f[0][0] = dist(tc[0], Q[0]);
        for (int i = 1; i <= Rho; i++) {
            f[0][i] = f[0][i - 1] + dist(Q[0], tc[i]);
        }
        f[0][Rho + 1] = INF;

        for (int i = 1; i < M; i++) {
            double minn = INF;
            for (int j = Math.max(0, i - Rho); j <= Math.min(M - 1, i + Rho); j++) {
                double min_cost = f[(i - 1) % 2][j];
                double a = INF, b = INF;
                if (j > 0) a = f[i % 2][j - 1];
                if (j > 0) b = f[(i - 1) % 2][j - 1];
                min_cost = Math.min(min_cost, Math.min(a, b));
                f[i % 2][j] = min_cost + dist(Q[i], tc[j]);
                if (minn > f[i % 2][j]) minn = f[i % 2][j];
            }
            if ((minn + cb[Math.min(M - 1, i + 1)]) >= Epsilon * Epsilon) {
                return minn + cb[Math.min(M - 1, i + 1)];
            }
            if ((minn + cbb[Math.min(M - 1, i + 1)]) >= Epsilon * Epsilon) {
                return minn + cbb[Math.min(M - 1, i + 1)];
            }
            f[i % 2][Math.min(i + Rho + 1, M)] = INF;
        }

        return f[(M - 1) % 2][M - 1];
    }

    private double lbET(double[] t, int x, double mean, double std, double[] cbb) {
        Index[] tt = new Index[M];
        double[] tu = new double[M], tl = new double[M];

        int st = x - M + 1;
        for (int i = 0; i < M; i++) {
            tt[i] = new Index((t[(st + i) % M] - mean) / std, st + i);
        }

        int duS = M, duT = M - 1, dlS = M, dlT = M - 1;
        int[] du = new int[2 * M + 10], dl = new int[2 * M + 10];
        for (int i = 0; i < M; i++) {
            while ((duT >= duS) && (tt[i].value > tt[du[duT]].value)) duT--;
            while ((dlT >= dlS) && (tt[i].value < tt[dl[dlT]].value)) dlT--;

            duT++;
            du[duT] = i;
            dlT++;
            dl[dlT] = i;

            tu[i] = tt[du[duS]].value;
            tl[i] = tt[dl[dlS]].value;

            if (i >= Rho) {
                tu[i - Rho] = Math.max(tu[i - Rho], tt[du[duS]].value);
                tl[i - Rho] = Math.min(tl[i - Rho], tt[dl[dlS]].value);
            }

            if ((i - Rho) == du[duS]) duS++;
            if ((i - Rho) == dl[dlS]) dlS++;
        }
        for (int i = M - Rho; i < M; i++) {
            tu[i] = Math.max(tu[i], tt[du[duS]].value);
            tl[i] = Math.min(tl[i], tt[dl[dlS]].value);
        }

        double LB = 0;
        for (int i = 0; i < M; i++) {
            int num = QQ[i].index;
            if (Q[num] > tu[num]) {
                double dis = dist(Q[num], tu[num]);
                LB = LB + dis;
                cbb[num] = dis;
            } else if (Q[num] < tl[num]) {
                double dis = dist(Q[num], tl[num]);
                LB = LB + dis;
                cbb[num] = dis;
            } else {
                LB = LB + 0;
                cbb[num] = 0;
            }
            if (LB > Epsilon * Epsilon) break;
        }
        return Math.sqrt(LB);
    }

    private double lbEQ(double[] t, int x, double mean, double std, double[] cb) {
        double LB = 0;
        for (int i = 0; i < M; i++) {
            double num = (t[(QQ[i].index + x + 1) % M] - mean) / std;
            if (num > Qu[QQ[i].index]) {
                double dis = dist(num, Qu[QQ[i].index]);
                LB = LB + dis;
                cb[QQ[i].index] = dis;
            } else if (num < Ql[QQ[i].index]) {
                double dis = dist(num, Ql[QQ[i].index]);
                LB = LB + dis;
                cb[QQ[i].index] = dis;
            } else {
                LB = LB + 0;
                cb[QQ[i].index] = 0;
            }
            if (LB > Epsilon * Epsilon) break;
        }
        return Math.sqrt(LB);
    }

    private double dist(double x, double y) {
        return (x - y) * (x - y);
    }

    private double lbKim(double[] t, int x, double mean, double std) {
        double LB, d;
        int st = x - M + 1;
        double en0 = (t[x % M] - mean) / std;
        double st0 = (t[(x - M + 1) % M] - mean) / std;
        LB = dist(st0, Q[0]) + dist(en0, Q[M - 1]);
        if (LB >= Epsilon * Epsilon) return LB;

        double st1 = (t[(st + 1) % M] - mean) / std;
        d = Math.min(dist(st1, Q[0]), dist(st0, Q[1]));
        d = Math.min(d, dist(st1, Q[1]));
        LB = LB + d;
        if (LB >= Epsilon * Epsilon) return LB;

        double en1 = (t[(st + M - 2) % M] - mean) / std;
        d = Math.min(dist(en1, Q[M - 1]), dist(en0, Q[M - 2]));
        d = Math.min(d, dist(en1, Q[M - 2]));
        LB = LB + d;
        if (LB >= Epsilon * Epsilon) return LB;

        double st2 = (t[(st + 2) % M] - mean) / std;
        d = Math.min(dist(st0, Q[2]), dist(st1, Q[2]));
        d = Math.min(d, dist(st2, Q[2]));
        d = Math.min(d, dist(st2, Q[1]));
        d = Math.min(d, dist(st2, Q[0]));
        LB = LB + d;

        double en2 = (t[(st + M - 3) % M] - mean) / std;
        d = Math.min(dist(en0, Q[M - 3]), dist(en1, Q[M - 3]));
        d = Math.min(d, dist(en2, Q[M - 3]));
        d = Math.min(d, dist(en2, Q[M - 2]));
        d = Math.min(d, dist(en2, Q[M - 1]));
        LB = LB + d;
        return LB;
    }

    private double lbPaaDTW(double[] Pre, int x, double tmpPre, double mean, double std) {
        int pSize = M / Phi;
        int st = x - M + 1;
        double tmp = tmpPre;

        for (int i = 0; i < Phi - 1; i++) {
            eT[i] = Pre[(st + (i + 1) * pSize - 1) % M] - tmp;
            eT[i] = (eT[i] / pSize - mean) / std;
            tmp = Pre[(st + (i + 1) * pSize - 1) % M];
        }
        eT[Phi - 1] = Pre[x % M] - tmp;
        eT[Phi - 1] = (eT[Phi - 1] / (M - (Phi - 1) * pSize) - mean) / std;

        double lb = 0;
        for (int i = 0; i < Phi - 1; i++) {
            if (eT[i] > eQu[i]) {
                lb = lb + (eT[i] - eQu[i]) * (eT[i] - eQu[i]) * pSize;
            }
            if (eT[i] < eQl[i]) {
                lb = lb + (eQl[i] - eT[i]) * (eQl[i] - eT[i]) * pSize;
            }
        }
        if (eT[Phi - 1] > eQu[Phi - 1]) {
            lb = lb + (eT[Phi - 1] - eQu[Phi - 1]) * (eT[Phi - 1] - eQu[Phi - 1]) * (M - (Phi - 1) * pSize);
        }
        if (eT[Phi - 1] < eQl[Phi - 1]) {
            lb = lb + (eQl[Phi - 1] - eT[Phi - 1]) * (eQl[Phi - 1] - eT[Phi - 1]) * (M - (Phi - 1) * pSize);
        }
        return Math.sqrt(lb);
    }

    private double ED(double S1, double S2, double S3, double S4, double S5) {
        double tmp = (M * S5 - S1 * S2) / (Math.sqrt(M * S3 - S1 * S1) * Math.sqrt(M * S4 - S2 * S2));
        return Math.sqrt(2 * M * (1.0 - tmp));
    }
}
