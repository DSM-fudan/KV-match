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
package cn.edu.fudan.dsm.kvmatch.experiments;

import cn.edu.fudan.dsm.kvmatch.NormQueryEngineDtw;
import cn.edu.fudan.dsm.kvmatch.experiments.ucr.UcrDtwQueryExecutor;
import cn.edu.fudan.dsm.kvmatch.operator.TimeSeriesOperator;
import cn.edu.fudan.dsm.kvmatch.operator.file.TimeSeriesFileOperator;
import cn.edu.fudan.dsm.kvmatch.statistic.StatisticInfo;
import cn.edu.fudan.dsm.kvmatch.statistic.StatisticWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created by Jiaye Wu on 17-8-28.
 */
public class NormQueryDtwSelectivityGenerate {

    private static final Logger logger = LoggerFactory.getLogger(NormQueryDtwSelectivityGenerate.class);

    private static final double[] BETA_BASE = {12.6630293829517, 19.6511100577873, 24.2890461295369, 44.973756278129, 56.5263112691118};

    public static void main(String args[]) {
        int N;
        String storageType;
        boolean runUcrDtw;
        if (args.length == 3) {
            N = Integer.parseInt(args[0]);
            storageType = args[1];
            runUcrDtw = Boolean.parseBoolean(args[2]);
        } else {
            Scanner scanner = new Scanner(System.in);
            System.out.print("Data Length = ");
            N = scanner.nextInt();
            System.out.print("Storage type [file/hdfs/hbase/kudu] = ");
            storageType = scanner.next();
            System.out.print("Run UCR-DTW? [true/false] = ");
            runUcrDtw = scanner.nextBoolean();
        }

        try {
            NormQueryEngineDtw queryEngine = new NormQueryEngineDtw(N, storageType);

            TimeSeriesOperator timeSeriesOperator = new TimeSeriesFileOperator(N, false);

            for (int k = 8; k <= 12; k++) {  // different query length
                int length = (int) Math.pow(2, k);  // 256, 512, 1024, 2048, 4096

                int aa = 0, bb = 10;
//                if (k == 8) { aa = 1; bb = 10; }
//                if (k == 9) { aa = 2; bb = 13; }
//                if (k == 10) { aa = 3; bb = 15;}
//                if (k == 11) { aa = 4; bb = 18; }
//                if (k == 12) { aa = 5; bb = 21; }
                for (int e = aa; e <= bb; e++) {  // different query delta
                    double epsilon = e == 0 ? 0.1 : e;

                    for (int a = 0; a < 3; a++) {
                        double alpha = 1.0;
                        if (a == 0) alpha = 1.1;
                        if (a == 1) alpha = 1.5;
                        if (a == 2) alpha = 2.0;

                        for (int b = 0; b < 4; b++) {
                            double beta = 0;
                            if (b == 0) beta = 0.5;
                            if (b == 1) beta = 1.0;
                            if (b == 2) beta = 5.0;
                            if (b == 3) beta = 10.0;
                            double betaPercent = beta;
                            beta = BETA_BASE[31 - Integer.numberOfLeadingZeros(length) - 8] * beta;

                            int rho = (int) (0.05 * length);

                            for (int o = 0; o < 10; o++) {
                                int offset = (new Random()).nextInt(N - length + 1) + 1;

                                // initialization: 0-T, 1-T_1, 2-T_2, 3-#candidates, 4-#answers, 5-#scans
                                List<StatisticInfo> statisticInfos = new ArrayList<>(6);
                                for (int j = 0; j < 6; j++) {
                                    statisticInfos.add(new StatisticInfo());
                                }

                                // execute the query requests
                                boolean ret = queryEngine.query(statisticInfos, offset, length, epsilon, rho, alpha, beta);
                                if (!ret) {  // warning for dismissals
                                    StatisticWriter.println("No result for the query," + offset + "," + length + "," + epsilon + "," + rho + "," + alpha + "," + beta);
                                }

                                // output statistic information
                                StatisticWriter.print(N + "," + offset + "," + length + "," + epsilon + "," + rho + "," + alpha + "," + beta + "," + betaPercent);
                                for (int i = 0; i < 6; i++) {
                                    StatisticWriter.print("," + statisticInfos.get(i).getLast());
                                }

                                // UCR-DTW
                                if (runUcrDtw) {
                                    @SuppressWarnings("unchecked")
                                    List<Double> queryData = timeSeriesOperator.readTimeSeries(offset, length);
                                    Iterator scanner = timeSeriesOperator.readAllTimeSeries();
                                    UcrDtwQueryExecutor executor = new UcrDtwQueryExecutor(N, length, queryData, epsilon, rho, alpha, beta, scanner);
                                    long startTime = System.currentTimeMillis();
                                    int cntAnswers = executor.run();
                                    long endTime = System.currentTimeMillis();
                                    StatisticWriter.println("," + (endTime - startTime) + "," + cntAnswers);
                                    logger.info("UCR-DTW: {} ({} ms)", cntAnswers, endTime - startTime);
                                } else {
                                    StatisticWriter.println();
                                }
                            }
                        }
                    }
                }
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e.getCause());
        }
    }
}
