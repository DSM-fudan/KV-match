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

import cn.edu.fudan.dsm.kvmatch.mapreduce.experiments.ucr.FloatUcrEdQueryExecutor;
import cn.edu.fudan.dsm.kvmatch.mapreduce.operator.FloatTimeSeriesTableOperator;
import cn.edu.fudan.dsm.kvmatch.statistic.StatisticInfo;
import cn.edu.fudan.dsm.kvmatch.statistic.StatisticWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

public class UcrEdHBaseTest {

    private static final Logger logger = LoggerFactory.getLogger(UcrEdHBaseTest.class.getName());

    private static FloatTimeSeriesTableOperator timeSeriesOperator = null;

    @SuppressWarnings("Duplicates")
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        System.out.print("Data Length = ");
        int N = scanner.nextInt();

        try {
            timeSeriesOperator = new FloatTimeSeriesTableOperator(N, 1, false);

            int minSelectivity = (int) Math.log10(N);
            for (int i = minSelectivity; i >= minSelectivity - 4; i--) {  // 9 8 7 6 5
                String filename = 1 + "e-" + i + "(S)";  // {1}e-{i}, e.g. 1e-3
                logger.info("selectivity: {}", filename);
                StatisticWriter.print(filename + ",");

                List<Integer> queryOffsets = new ArrayList<>(100);
                List<Integer> queryLengths = new ArrayList<>(100);
                List<Integer> queryEpsilons = new ArrayList<>(100);

                try (BufferedReader br = new BufferedReader(new FileReader("queries" + File.separator + "selectivity-" + N + File.separator + filename + ".csv"))) {
                    String line = br.readLine();
                    while (line != null) {
                        String[] parameters = line.split(",");
                        queryOffsets.add(Integer.parseInt(parameters[1]));
                        queryLengths.add(Integer.parseInt(parameters[2]));
                        queryEpsilons.add(Integer.parseInt(parameters[3]));
                        line = br.readLine();
                    }
                }

                // execute the query requests
                StatisticInfo statistic = new StatisticInfo();
                long startTime = System.currentTimeMillis();
                for (int j = 0; j < queryOffsets.size(); j++) {
                    queryUcrEdStandalone(statistic, N, queryOffsets.get(j), queryLengths.get(j), queryEpsilons.get(j));
                }
                long endTime = System.currentTimeMillis();

                // output statistic information
                logger.info("Time usage in total: {} ms, average: {} ms, minimum: {} ms, maximum: {} ms", endTime - startTime, statistic.getAverage(), statistic.getMinimum(), statistic.getMaximum());
                StatisticWriter.println(statistic.getAverage() + "," + statistic.getMinimum() + "," + statistic.getMaximum());
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e.getCause());
        }
    }

    private static void queryUcrEdStandalone(StatisticInfo statisticInfo, int N, int offset, int length, double epsilon) throws IOException {
        logger.info("offset: {}, length: {}, epsilon: {}", offset, length, epsilon);
        List<Float> queryData = timeSeriesOperator.readTimeSeries(offset, length);

        Iterator scanner = timeSeriesOperator.readAllTimeSeries();
        FloatUcrEdQueryExecutor executor = new FloatUcrEdQueryExecutor(N, length, queryData, epsilon, scanner);

        long startTime = System.currentTimeMillis();
        executor.run();
        long endTime = System.currentTimeMillis();

        statisticInfo.append(endTime - startTime);
        logger.info("Total time usage: {} ms", endTime - startTime);
    }
}
