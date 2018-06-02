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
package cn.edu.fudan.dsm.kvmatch.mapreduce.experiments;

import cn.edu.fudan.dsm.kvmatch.mapreduce.LongQueryEngine;
import cn.edu.fudan.dsm.kvmatch.statistic.StatisticInfo;
import cn.edu.fudan.dsm.kvmatch.statistic.StatisticWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class LongRandomQueryTest {

    private static final Logger logger = LoggerFactory.getLogger(LongRandomQueryTest.class);

    private static final double[] EPSILON_LIST_128 = {15, 18, 24};
    private static final double[] EPSILON_LIST_256 = {24, 30, 48};
    private static final double[] EPSILON_LIST_512 = {50, 75, 100};
    private static final double[] EPSILON_LIST_1024 = {100, 125, 150};
    private static final double[] EPSILON_LIST_2048 = {175, 200, 400};
    private static final double[] EPSILON_LIST_4096 = {400, 600, 800};
    private static final double[] EPSILON_LIST_8192 = {800, 1200, 1600};

//    private static final double[] EpsilonList = {5, 10, 15, 18, 24, 30, 50, 100, 150, 200, 400};

    public static void main(String args[]) {
        try {
            for (int n = 10; n <= 12; n++) {
                long N = (long) Math.pow(10, n);
                LongQueryEngine queryEngine = new LongQueryEngine(N);

                for (int k = 7; k <= 13; k++) {  // different query lengths
                    int queryLength = (int) Math.pow(2, k);  // 128, 256, 512, 1024, 2048, 4096, 8192

                    for (int o = 1; o <= 10; o++) {  // different query offsets
                        long queryOffset = ThreadLocalRandom.current().nextLong(1, N - queryLength);

                        for (int e = 0; e < 3; e++) {  // different query epsilons
                            double queryEpsilon = 0;  //= EpsilonList[e]
                            if (queryLength == 128) {
                                queryEpsilon = EPSILON_LIST_128[e];
                            } else if (queryLength == 256) {
                                queryEpsilon = EPSILON_LIST_256[e];
                            } else if (queryLength == 512) {
                                queryEpsilon = EPSILON_LIST_512[e];
                            } else if (queryLength == 1024) {
                                queryEpsilon = EPSILON_LIST_1024[e];
                            } else if (queryLength == 2048) {
                                queryEpsilon = EPSILON_LIST_2048[e];
                            } else if (queryLength == 4096) {
                                queryEpsilon = EPSILON_LIST_4096[e];
                            } else if (queryLength == 8192) {
                                queryEpsilon = EPSILON_LIST_8192[e];
                            }

                            // initialization: 0-T, 1-T_1, 2-T_2, 3-#candidates, 4-#answers
                            List<StatisticInfo> statisticInfos = new ArrayList<>(5);
                            for (int j = 0; j < 5; j++) {
                                statisticInfos.add(new StatisticInfo());
                            }

                            // execute the query requests
                            boolean ret = queryEngine.query(statisticInfos, queryOffset, queryLength, queryEpsilon);
                            if (!ret) {
                                StatisticWriter.println("No result for the query," + queryOffset + "," + queryLength + "," + queryEpsilon);
                            }

                            // output statistic information
                            StatisticWriter.print(N + "," + queryOffset + "," + queryLength + "," + queryEpsilon + ",");
                            for (int j = 0; j < 5; j++) {
                                StatisticWriter.print(statisticInfos.get(j).getAverage() + ",");
                            }
                            StatisticWriter.println("");

                            // avoid too large selectivity (#answers > N * 10^-6)
                            if (statisticInfos.get(4).getAverage() > N * Math.pow(10, -6)) break;
                        }
                    }
                }
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e.getCause());
        }
    }
}
