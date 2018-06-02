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
package cn.edu.fudan.dsm.kvmatch.utils;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Utilities for operations on random numbers
 * <p>
 * Created by Jiaye Wu on 16-2-4.
 */
public class RandomUtils {

    /**
     * Generate random double number in [min, max]
     *
     * @param min the lower bound
     * @param max the upper bound
     * @return the random number in [min, max]
     */
    public static double random(double min, double max) {
        return ThreadLocalRandom.current().nextDouble(min, max + 0.00001);
    }

    /**
     * Generate random integer number in [min, max]
     *
     * @param min the lower bound
     * @param max the upper bound
     * @return the random number in [min, max]
     */
    public static int random(int min, int max) {
        return ThreadLocalRandom.current().nextInt(min, max + 1);
    }
}
