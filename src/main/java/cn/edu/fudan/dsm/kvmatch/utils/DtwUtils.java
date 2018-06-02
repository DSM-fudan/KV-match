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
package cn.edu.fudan.dsm.kvmatch.utils;

import cn.edu.fudan.dsm.kvmatch.common.CircularArray;

import java.util.List;

public class DtwUtils {

    private static final double INF = 1e20;

    private static int max(int a, int b) {
        return (a > b) ? a : b;
    }

    private static int min(int a, int b) {
        return (a < b) ? a : b;
    }

    private static double min(double a, double b) {
        return (a < b) ? a : b;
    }

    private static double dist(double v1, double v2) {
        return (v1 - v2) * (v1 - v2);
    }

    /**
     * Finding the envelop of min and max value for LB_Keogh
     * Implementation idea is introduced by Danial Lemire in his paper
     * "Faster Retrieval with a Two-Pass Dynamic-Time-Warping Lower Bound", Pattern Recognition 42(9), 2009.
     *
     * @param len ...
     * @param r   ...
     */
    public static void lowerUpperLemire(double[] t, int len, int r, double[] l, double[] u) {
        CircularArray du = new CircularArray(2 * r + 2);
        CircularArray dl = new CircularArray(2 * r + 2);

        du.pushBack(0);
        dl.pushBack(0);

        for (int i = 1; i < len; i++) {
            if (i > r) {
                u[i - r - 1] = t[du.front()];
                l[i - r - 1] = t[dl.front()];
            }
            if (t[i] > t[i - 1]) {
                du.popBack();
                while (!du.isEmpty() && t[i] > t[du.back()]) {
                    du.popBack();
                }
            } else {
                dl.popBack();
                while (!dl.isEmpty() && t[i] < t[dl.back()]) {
                    dl.popBack();
                }
            }
            du.pushBack(i);
            dl.pushBack(i);
            if (i == 2 * r + 1 + du.front()) {
                du.popFront();
            } else if (i == 2 * r + 1 + dl.front()) {
                dl.popFront();
            }
        }
        for (int i = len; i < len + r + 1; i++) {
            u[i - r - 1] = t[du.front()];
            l[i - r - 1] = t[dl.front()];
            if (i - du.front() >= 2 * r + 1) {
                du.popFront();
            }
            if (i - dl.front() >= 2 * r + 1) {
                dl.popFront();
            }
        }
    }

    public static void lowerUpperLemire(List<Double> t, int r, double[] l, double[] u) {
        CircularArray du = new CircularArray(2 * r + 2);
        CircularArray dl = new CircularArray(2 * r + 2);

        du.pushBack(0);
        dl.pushBack(0);

        for (int i = 1; i < t.size(); i++) {
            if (i > r) {
                u[i - r - 1] = t.get(du.front());
                l[i - r - 1] = t.get(dl.front());
            }
            if (t.get(i) > t.get(i - 1)) {
                du.popBack();
                while (!du.isEmpty() && t.get(i) > t.get(du.back())) {
                    du.popBack();
                }
            } else {
                dl.popBack();
                while (!dl.isEmpty() && t.get(i) < t.get(dl.back())) {
                    dl.popBack();
                }
            }
            du.pushBack(i);
            dl.pushBack(i);
            if (i == 2 * r + 1 + du.front()) {
                du.popFront();
            } else if (i == 2 * r + 1 + dl.front()) {
                dl.popFront();
            }
        }
        for (int i = t.size(); i < t.size() + r + 1; i++) {
            u[i - r - 1] = t.get(du.front());
            l[i - r - 1] = t.get(dl.front());
            if (i - du.front() >= 2 * r + 1) {
                du.popFront();
            }
            if (i - dl.front() >= 2 * r + 1) {
                dl.popFront();
            }
        }
    }

    /**
     * Calculate quick lower bound
     * Usually, LB_Kim take time O(m) for finding top,bottom,fist and last.
     * However, because of z-normalization the top and bottom cannot give significant benefits.
     * And using the first and last points can be computed in constant time.
     * The pruning power of LB_Kim is non-trivial, especially when the query is not long, say in length 128.
     *
     * @param j    ...
     * @param len  ...
     * @param mean ...
     * @param std  ...
     * @return ...
     */
    public static double lbKimHierarchy(double[] t, double[] q, int j, int len, double mean, double std, double bsf) {
        // 1 point at front and back
        double d, lb;
        double x0 = (t[j] - mean) / std;
        double y0 = (t[(len - 1 + j)] - mean) / std;
        lb = dist(x0, q[0]) + dist(y0, q[len - 1]);
        if (lb >= bsf) return lb;

        // 2 points at front
        double x1 = (t[(j + 1)] - mean) / std;
        d = min(dist(x1, q[0]), dist(x0, q[1]));
        d = min(d, dist(x1, q[1]));
        lb += d;
        if (lb >= bsf) return lb;

        // 2 points at back
        double y1 = (t[(len - 2 + j)] - mean) / std;
        d = min(dist(y1, q[len - 1]), dist(y0, q[len - 2]));
        d = min(d, dist(y1, q[len - 2]));
        lb += d;
        if (lb >= bsf) return lb;

        // 3 points at front
        double x2 = (t[(j + 2)] - mean) / std;
        d = min(dist(x0, q[2]), dist(x1, q[2]));
        d = min(d, dist(x2, q[2]));
        d = min(d, dist(x2, q[1]));
        d = min(d, dist(x2, q[0]));
        lb += d;
        if (lb >= bsf) return lb;

        // 3 points at back
        double y2 = (t[(len - 3 + j)] - mean) / std;
        d = min(dist(y0, q[len - 3]), dist(y1, q[len - 3]));
        d = min(d, dist(y2, q[len - 3]));
        d = min(d, dist(y2, q[len - 2]));
        d = min(d, dist(y2, q[len - 1]));
        lb += d;

        return lb;
    }

    /**
     * LB_Keogh 1: Create Envelop for the query
     * Note that because the query is known, envelop can be created once at the beginning.
     *
     * @param order sorted indices for the query.
     * @param t     a circular array keeping the current data.
     * @param uo    upper envelops for the query, which already sorted.
     * @param lo    lower envelops for the query, which already sorted.
     * @param cb    (output) current bound at each position. It will be used later for early abandoning in DTW.
     * @param j     index of the starting location in t
     * @param len   ...
     * @param mean  ...
     * @param std   ...
     * @return ...
     */
    public static double lbKeoghCumulative(int[] order, double[] t, double[] uo, double[] lo, double[] cb, int j, int len, double mean, double std, double best_so_far) {
        double lb = 0;
        double x, d;

        for (int i = 0; i < len && lb < best_so_far; i++) {
            x = (t[(order[i] + j)] - mean) / std;
            d = 0;
            if (x > uo[i]) {
                d = dist(x, uo[i]);
            } else if (x < lo[i]) {
                d = dist(x, lo[i]);
            }
            lb += d;
            cb[order[i]] = d;
        }
        return lb;
    }

    /**
     * LB_Keogh 2: Create Envelop for the data
     * Note that the envelops have been created (in main function) when each data point has been read.
     *
     * @param order ...
     * @param qo    sorted query
     * @param cb    (output) current bound at each position. Used later for early abandoning in DTW.
     * @param l     lower envelop of the current data
     * @param u     upper envelop of the current data
     * @param len   ...
     * @param mean  ...
     * @param std   ...
     * @return ...
     */
    public static double lbKeoghDataCumulative(int[] order, double[] qo, double[] cb, int I, double[] l, double[] u, int len, double mean, double std, double best_so_far) {
        double lb = 0;
        double uu, ll, d;

        for (int i = 0; i < len && lb < best_so_far; i++) {
            uu = (u[order[i] + I] - mean) / std;
            ll = (l[order[i] + I] - mean) / std;
            d = 0;
            if (qo[i] > uu) {
                d = dist(qo[i], uu);
            } else {
                if (qo[i] < ll) {
                    d = dist(qo[i], ll);
                }
            }
            lb += d;
            cb[order[i]] = d;
        }
        return lb;
    }

    /**
     * Calculate Dynamic Time Wrapping distance
     *
     * @param A  data
     * @param B  query
     * @param cb cumulative bound used for early abandoning
     * @param m  ...
     * @param r  size of Sakoe-Chiba warping band
     * @return ...
     */
    public static double dtw(double[] A, double[] B, double[] cb, int m, int r, double bsf) {
        double[] cost, cost_prev, cost_tmp;

        int i, j, k;
        double x, y, z, min_cost;

        /// Instead of using matrix of size O(m^2) or O(mr), we will reuse two array of size O(r).
        cost = new double[2 * r + 1];
        for (k = 0; k < 2 * r + 1; k++) {
            cost[k] = INF;
        }

        cost_prev = new double[2 * r + 1];
        for (k = 0; k < 2 * r + 1; k++) {
            cost_prev[k] = INF;
        }

        for (i = 0; i < m; i++) {
            k = max(0, r - i);
            min_cost = INF;

            for (j = max(0, i - r); j <= min(m - 1, i + r); j++, k++) {
                // Initialize all row and column
                if ((i == 0) && (j == 0)) {
                    cost[k] = dist(A[0], B[0]);
                    min_cost = cost[k];
                    continue;
                }

                if ((j - 1 < 0) || (k - 1 < 0)) {
                    y = INF;
                } else {
                    y = cost[k - 1];
                }
                if ((i - 1 < 0) || (k + 1 > 2 * r)) {
                    x = INF;
                } else {
                    x = cost_prev[k + 1];
                }
                if ((i - 1 < 0) || (j - 1 < 0)) {
                    z = INF;
                } else {
                    z = cost_prev[k];
                }

                // Classic DTW calculation
                cost[k] = min(min(x, y), z) + dist(A[i], B[j]);

                // Find minimum cost in row for early abandoning (possibly to use column instead of row).
                if (cost[k] < min_cost) {
                    min_cost = cost[k];
                }
            }

            // We can abandon early if the current cumulative distance with lower bound together are larger than bsf
            if (i + r < m - 1 && min_cost + cb[i + r + 1] >= bsf) {
                return min_cost + cb[i + r + 1];
            }

            // Move current array to previous array.
            cost_tmp = cost;
            cost = cost_prev;
            cost_prev = cost_tmp;
        }
        k--;

        // the DTW distance is in the last cell in the matrix of size O(m^2) or at the middle of our array.
        return cost_prev[k];
    }
}
