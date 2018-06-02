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

import cn.edu.fudan.dsm.kvmatch.operator.TimeSeriesOperator;
import cn.edu.fudan.dsm.kvmatch.operator.file.TimeSeriesFileOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Created by Jiaye Wu on 18-2-22.
 */
public class GMatchQueryDataExtractor {

    private static final Logger logger = LoggerFactory.getLogger(GMatchQueryDataExtractor.class);

    public static void main(String args[]) {
        int N;
        if (args.length == 2) {
            N = Integer.parseInt(args[0]);
        } else {
            Scanner scanner = new Scanner(System.in);
            System.out.print("Data Length = ");
            N = scanner.nextInt();
        }

        try {
            TimeSeriesOperator timeSeriesOperator = new TimeSeriesFileOperator(N, false);

            int minSelectivity = (int) Math.log10(N);
            for (int i = minSelectivity; i >= minSelectivity - 4; i--) {  // 9 8 7 6 5
                String filename = 1 + "e-" + i;// + "(S)";  // {1}e-{i}, e.g. 1e-3

                List<Integer> queryOffsets = new ArrayList<>(100);
                List<Integer> queryLengths = new ArrayList<>(100);
                List<Double> queryEpsilons = new ArrayList<>(100);

                try (BufferedReader br = new BufferedReader(new FileReader("queries" + File.separator + "dtw-selectivity-" + N + File.separator + filename + ".csv"))) {
                    String line = br.readLine();
                    while (line != null) {
                        String[] parameters = line.split(",");
                        queryOffsets.add(Integer.parseInt(parameters[0]));
                        queryLengths.add(Integer.parseInt(parameters[1]));
                        queryEpsilons.add(Double.parseDouble(parameters[2]));
                        line = br.readLine();
                    }
                }

                // write queries
                int count = 0;
                for (int j = 0; j < queryOffsets.size(); j++) {
                    count++;
                    @SuppressWarnings("unchecked")
                    List<Double> queryData = timeSeriesOperator.readTimeSeries(queryOffsets.get(j), queryLengths.get(j));
                    try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream("queries" + File.separator + "dtw-selectivity-" + N + File.separator + "data" + File.separator + "q." + filename + "-" + count)))) {
                        dos.writeFloat(queryEpsilons.get(j).floatValue());
                        dos.writeInt(queryLengths.get(j));
                        for (Double query : queryData) {
                            dos.writeDouble(query);
                        }
                    } catch (IOException e) {
                        logger.error(e.getMessage(), e.getCause());
                    }
                }
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e.getCause());
        }
    }
}
