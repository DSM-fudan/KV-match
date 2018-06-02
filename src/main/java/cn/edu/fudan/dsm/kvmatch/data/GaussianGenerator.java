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
package cn.edu.fudan.dsm.kvmatch.data;

import cn.edu.fudan.dsm.kvmatch.utils.RandomUtils;

/**
 * Gaussian time series generator
 * <p>
 * Created by Jiaye Wu on 16-2-4.
 */
public class GaussianGenerator implements SeriesGenerator {

    private static double V1 = 0, V2 = 0, S = 0;
    private static int phase = 0;

    private double minMean;
    private double maxMean;

    private double minStd;
    private double maxStd;

    public GaussianGenerator(double minMean, double maxMean, double minStd, double maxStd) {
        this.minMean = minMean;
        this.maxMean = maxMean;
        this.minStd = minStd;
        this.maxStd = maxStd;
    }

    public static double[] generate(int length, double mean, double std) {
        double[] timeSeries = new double[length];

        for (int i = 0; i < timeSeries.length; i++) {
            timeSeries[i] = mean + std * gaussRand();
        }

        return timeSeries;
    }

    private static double gaussRand() {
        double X;

        if (phase == 0) {
            do {
                double U1 = Math.random() / 1.0;
                double U2 = Math.random() / 1.0;

                V1 = 2.0 * U1 - 1.0;
                V2 = 2.0 * U2 - 1.0;
                S = V1 * V1 + V2 * V2;
            } while (S >= 1.0 || S == 0.0);

            X = V1 * Math.sqrt(-2.0 * Math.log(S) / S);
        } else {
            X = V2 * Math.sqrt(-2.0 * Math.log(S) / S);
        }
        phase = 1 - phase;

        return X;
    }

    public double[] generate(int length) {
        double mean = RandomUtils.random(minMean, maxMean);
        double std = RandomUtils.random(minStd, maxStd);
        V1 = 0.0;
        V2 = 0.0;
        S = 0.0;
        phase = 0;

        return generate(length, mean, std);
    }
}
