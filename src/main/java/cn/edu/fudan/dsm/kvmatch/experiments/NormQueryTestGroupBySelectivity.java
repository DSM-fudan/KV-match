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

import cn.edu.fudan.dsm.kvmatch.NormQueryEngine;
import cn.edu.fudan.dsm.kvmatch.experiments.ucr.PaaUcrEdQueryExecutor;
import cn.edu.fudan.dsm.kvmatch.experiments.ucr.UcrEdQueryExecutor;
import cn.edu.fudan.dsm.kvmatch.operator.TimeSeriesOperator;
import cn.edu.fudan.dsm.kvmatch.operator.file.TimeSeriesFileOperator;
import cn.edu.fudan.dsm.kvmatch.statistic.StatisticInfo;
import cn.edu.fudan.dsm.kvmatch.statistic.StatisticWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Created by Jiaye Wu on 18-8-20.
 */
@SuppressWarnings("Duplicates")
public class NormQueryTestGroupBySelectivity {

    private static final Logger logger = LoggerFactory.getLogger(NormQueryTestGroupBySelectivity.class);

    private static final int NUM_STATISTIC_INFO = 10;

    public static void main(String[] args) {
        int N;
        String storageType;
        boolean runUcrEd;
        if (args.length == 3) {
            N = Integer.parseInt(args[0]);
            storageType = args[1];
            runUcrEd = Boolean.parseBoolean(args[2]);
        } else {
            Scanner scanner = new Scanner(System.in);
            System.out.print("Data Length = ");
            N = scanner.nextInt();
            System.out.print("Storage type [file/hbase/kudu] = ");
            storageType = scanner.next();
            System.out.print("Run UCR-ED? [true/false] = ");
            runUcrEd = scanner.nextBoolean();
        }

        try {
            NormQueryEngine queryEngine = new NormQueryEngine(N, storageType);

            TimeSeriesOperator timeSeriesOperator = new TimeSeriesFileOperator(N, false);

            int minSelectivity = (int) Math.log10(N);
            for (int i = minSelectivity; i >= minSelectivity - 4; i--) {  // 9 8 7 6 5
                String filename = 1 + "e-" + i;// + "(S)";  // {1}e-{i}, e.g. 1e-3

                List<Integer> queryOffsets = new ArrayList<>(100);
                List<Integer> queryLengths = new ArrayList<>(100);
                List<Integer> queryEpsilons = new ArrayList<>(100);
                List<Double> queryAlphas = new ArrayList<>(100);
                List<Double> queryBetas = new ArrayList<>(100);

                try (BufferedReader br = new BufferedReader(new FileReader("queries" + File.separator + "norm-selectivity-" + N + File.separator + filename + ".csv"))) {
                    String line = br.readLine();
                    while (line != null) {
                        String[] parameters = line.split(",");
                        queryOffsets.add(Integer.parseInt(parameters[0]));
                        queryLengths.add(Integer.parseInt(parameters[1]));
                        queryEpsilons.add(Integer.parseInt(parameters[2]));
                        queryAlphas.add(Double.parseDouble(parameters[3]));
                        queryBetas.add(Double.parseDouble(parameters[4]));
                        line = br.readLine();
                    }
                }

                // initialization: 0-T, 1-T_1, 2-T_2, 3-#candidates, 4-#answers, 5-#scans
                List<StatisticInfo> statisticInfos = new ArrayList<>(NUM_STATISTIC_INFO);
                for (int j = 0; j < NUM_STATISTIC_INFO; j++) {
                    statisticInfos.add(new StatisticInfo());
                }

                // execute the query requests
                for (int j = 0; j < queryOffsets.size(); j++) {
                    boolean ret = queryEngine.query(statisticInfos, queryOffsets.get(j), queryLengths.get(j), queryEpsilons.get(j), queryAlphas.get(j), queryBetas.get(j));
                    if (!ret) {  // warning for dismissals
                        StatisticWriter.println("No result for the query," + queryOffsets.get(j) + "," + queryLengths.get(j) + "," + queryEpsilons.get(j) + "," + queryAlphas.get(j) + "," + queryBetas.get(j));
                    }
                    // output detail log to file
                    StatisticWriter.print(N + "," + queryOffsets.get(j) + "," + queryLengths.get(j) + "," + queryEpsilons.get(j) + "," + queryAlphas.get(j) + "," + queryBetas.get(j));
                    for (int k = 0; k < NUM_STATISTIC_INFO; k++) {
                        StatisticWriter.print("," + statisticInfos.get(k).getLast());
                    }

                    // UCR-ED
                    if (runUcrEd && (j == 0 || !queryBetas.get(j).equals(queryBetas.get(j-1)))) {
                        @SuppressWarnings("unchecked")
                        List<Double> queryData = timeSeriesOperator.readTimeSeries(queryOffsets.get(j), queryLengths.get(j));

                        UcrEdQueryExecutor executor = new UcrEdQueryExecutor(N, queryLengths.get(j), queryData, queryEpsilons.get(j), queryAlphas.get(j), queryBetas.get(j), timeSeriesOperator.readAllTimeSeries());
                        long startTime = System.currentTimeMillis();
                        int cntAnswers = executor.run();
                        long endTime = System.currentTimeMillis();
                        StatisticWriter.print("," + (endTime - startTime) + "," + cntAnswers);
                        logger.info("UCR-ED: {} ({} ms)", cntAnswers, endTime - startTime);

                        PaaUcrEdQueryExecutor executor2 = new PaaUcrEdQueryExecutor(N, queryLengths.get(j), queryData, queryEpsilons.get(j), queryAlphas.get(j), queryBetas.get(j), 24, timeSeriesOperator.readAllTimeSeries());
                        long startTime2 = System.currentTimeMillis();
                        int cntAnswers2 = executor2.run();
                        long endTime2 = System.currentTimeMillis();
                        StatisticWriter.println("," + (endTime2 - startTime2) + "," + cntAnswers2);
                        logger.info("PAA-UCR-ED: {} ({} ms)", cntAnswers2, endTime2 - startTime2);
                    } else {
                        StatisticWriter.println();
                    }
                }

                // output statistic information
                logger.info("T: {} ms, T_1: {} ms, T_2: {} ms, #candidates: {}, #answers: {}, #scans: {}", statisticInfos.get(0).getAverage(), statisticInfos.get(1).getAverage(), statisticInfos.get(2).getAverage(), statisticInfos.get(3).getAverage(), statisticInfos.get(4).getAverage(), statisticInfos.get(5).getAverage());
                StatisticWriter.print(filename + ",");
                for (int j = 0; j < NUM_STATISTIC_INFO; j++) {
                    StatisticWriter.print(statisticInfos.get(j).getAverage() + ",");
                }
                StatisticWriter.println();
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e.getCause());
        }
    }
}
