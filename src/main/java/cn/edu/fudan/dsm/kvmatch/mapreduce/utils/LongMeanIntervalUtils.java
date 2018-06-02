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
package cn.edu.fudan.dsm.kvmatch.mapreduce.utils;

import cn.edu.fudan.dsm.kvmatch.common.Pair;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Utilities for operations on the Key of KV-index
 * <p>
 * Created by Jiaye Wu on 16-8-8.
 */
public class LongMeanIntervalUtils {

    /**
     * The minumum value of time series, which should be adapt to the real data.
     */
    private static double MINIMUM = 1000000;

    /**
     * Round float number to half integer. d = 0.5
     * For Example: 1.9 ->  1.5,  1.4 ->  1.0,  1.5 ->  1.5
     * -1.9 -> -2.0, -1.4 -> -1.5, -1.5 -> -1.5
     *
     * @param value should be rounded
     * @return rounded value
     */
    public static double toRound(double value) {
        double intValue = Math.floor(value);
        double diff = value - intValue;
        if (Double.compare(diff, 0.5) >= 0) {
            return intValue + 0.5;
        } else {
            return intValue;
        }
    }

    /**
     * toRound based on statistic information (lower bound)
     *
     * @param value         should be rounded
     * @param statisticInfo statistic information of index table
     * @return rounded value based on statistic information
     */
    public static double toRound(double value, List<Pair<Double, Pair<Long, Long>>> statisticInfo) {
        double rounded = toRound(value);
        int index = Collections.binarySearch(statisticInfo, new Pair<>(rounded, 0), Comparator.comparing(Pair::getFirst));
        if (index < 0) {
            index = -(index + 1) - 1;
            if (index < 0) return rounded - 10000;
            return statisticInfo.get(index).getFirst();
        } else {
            return rounded;
        }
    }

    /**
     * To upper bound of mean interval.
     * For example: 1.0 ->  1.5,  1.5 ->  2.0
     * -1.0 -> -0.5, -1.5 -> -1.0
     *
     * @param round mean interval round
     * @return upper bound
     */
    public static double toUpper(double round) {
        return round + 0.5;
    }

    /**
     * toUpper based on statistic information (upper bound)
     *
     * @param round         mean interval round
     * @param statisticInfo statistic information of index table
     * @return upper bound basedo on statistic information
     */
    public static double toUpper(double round, List<Pair<Double, Pair<Long, Long>>> statisticInfo) {
        double rounded = toUpper(round);
        int index = Collections.binarySearch(statisticInfo, new Pair<>(rounded, 0), Comparator.comparing(Pair::getFirst));
        if (index < 0) {
            index = -(index + 1);
            if (index >= statisticInfo.size()) return rounded + 10000;
            return statisticInfo.get(index).getFirst();
        } else {
            return rounded;
        }
    }

    /**
     * Convert mean value into HBase row key.
     * 1. Add the minimum value, and convert to positive number;
     * 2. Round to 0.0 or 0.5;
     * 3. Convert to byte array.
     *
     * @param value should be processed
     * @return row key in bytes
     */
    public static byte[] toRoundBytes(double value) {
        return Bytes.toBytes(toRound(value) + MINIMUM);
    }

    public static byte[] toBytes(double value) {
        return Bytes.toBytes(value + MINIMUM);
    }

    /**
     * Convert HBase row key into mean value.
     * 1. Convert to double value;
     * 2. Minus the minimum value.
     *
     * @param bytes
     * @return
     */
    public static double toDouble(byte[] bytes) {
        return Bytes.toDouble(bytes) - MINIMUM;
    }
}
