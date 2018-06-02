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
 * Random walk time series generator
 * <p>
 * Created by Jiaye Wu on 16-2-4.
 */
public class RandomWalkGenerator implements SeriesGenerator {

    private double minStart;
    private double maxStart;

    private double minStep;
    private double maxStep;

    public RandomWalkGenerator(double minStart, double maxStart, double minStep, double maxStep) {
        this.minStart = minStart;
        this.maxStart = maxStart;
        this.minStep = minStep;
        this.maxStep = maxStep;
    }

    public double[] generate(int length) {
        double[] timeSeries = new double[length];
        timeSeries[0] = RandomUtils.random(minStart, maxStart);

        for (int i = 1; i < timeSeries.length; i++) {
            double sign = Math.random() < 0.5 ? -1 : 1;
            double step = RandomUtils.random(minStep, maxStep);
            timeSeries[i] = timeSeries[i - 1] + sign * step;
        }
        return timeSeries;
    }
}
