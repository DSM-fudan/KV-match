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
package cn.edu.fudan.dsm.kvmatch.statistic;

/**
 * A class to maintain statistic information, e.g. average of execution time
 * <p>
 * Created by Jiaye Wu on 16-2-15.
 */
public class StatisticInfo {

    private double last;

    private double sum;

    private long n;

    private double maximum;

    private double minimum;

    public StatisticInfo() {
        last = -1;
        sum = 0;
        n = 0;
        maximum = Double.MIN_VALUE;
        minimum = Double.MAX_VALUE;
    }

    public void append(double value) {
        last = value;
        sum += value;
        n++;
        if (value > maximum) {
            maximum = value;
        }
        if (value < minimum) {
            minimum = value;
        }
    }

    public double getLast() {
        return last;
    }

    public double getSum() {
        return sum;
    }

    public long getN() {
        return n;
    }

    public double getAverage() {
        return sum / n;
    }

    public double getMaximum() {
        return maximum;
    }

    public double getMinimum() {
        return minimum;
    }
}
