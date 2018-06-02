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
 * Mixed sine time series generator
 * <p>
 * Created by Jiaye Wu on 17-3-4.
 */
public class SineGenerator implements SeriesGenerator {

    private double minFrequency;
    private double maxFrequency;

    private double minAmplitude;
    private double maxAmplitude;

    private double minMean;
    private double maxMean;

    public SineGenerator(double minFrequency, double maxFrequency, double minAmplitude, double maxAmplitude, double minMean, double maxMean) {
        this.minFrequency = minFrequency;
        this.maxFrequency = maxFrequency;
        this.minAmplitude = minAmplitude;
        this.maxAmplitude = maxAmplitude;
        this.minMean = minMean;
        this.maxMean = maxMean;
    }

    public double[] generate(int length) {
        double[] timeSeries = new double[length];
        double frequency = RandomUtils.random(minFrequency, maxFrequency);
        double amplitude = RandomUtils.random(minAmplitude, maxAmplitude);
        double mean = RandomUtils.random(minMean, maxMean);
        double phase = RandomUtils.random(0, 2 * Math.PI);

        for (int i = 0; i < timeSeries.length; i++) {
            timeSeries[i] = mean + amplitude * Math.sin(2 * i * (Math.PI / timeSeries.length) * frequency + phase) + RandomUtils.random(amplitude * 0.05 * -1, amplitude * 0.05);  // add noise
        }
        return timeSeries;
    }
}
