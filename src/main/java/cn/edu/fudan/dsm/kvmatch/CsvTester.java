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
package cn.edu.fudan.dsm.kvmatch;

import cn.edu.fudan.dsm.kvmatch.common.Pair;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Scanner;

public class CsvTester {

    public static void main(String[] args) {
        // parse arguments
        if (args.length != 6 && args.length != 8) {
            System.out.println("Usage: CsvTester <measure (ED|DTW)> <problem (RSM|NSM|cNSM)> <data file path> <pattern begin offset> <pattern end offset> <epsilon> [<alpha> <beta>]");
            return;
        }
        String measure = args[0].trim().toUpperCase();
        String problem = args[1].trim().toUpperCase();
        String dataPath = args[2];
        int patternBegin = Integer.parseInt(args[3]), patternEnd = Integer.parseInt(args[4]);
        double epsilon = Double.parseDouble(args[5]), alpha = 1, beta = 0;
        if (args.length == 8) {
            alpha = Double.parseDouble(args[6]);
            beta = Double.parseDouble(args[7]);
        }

        // read data and extract query pattern
        List<Double> data = new ArrayList<>();
        try (Scanner scanner = new Scanner(new File(dataPath))) {
            while (scanner.hasNext()) {
                data.add(Double.parseDouble(scanner.nextLine()));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        List<Double> query = data.subList(patternBegin - 1, patternEnd);

        // execute query
        List<Pair<Integer, Double>> answers = new ArrayList<>();
        if (measure.equals("ED")) {
            if (problem.equals("RSM")) {  // RSM
                for (int i = 0; i + query.size() - 1 < data.size(); i++) {
                    double distance = 0;
                    for (int j = 0; j < query.size() && distance <= epsilon * epsilon; j++) {
                        distance += (data.get(i + j) - query.get(j)) * (data.get(i + j) - query.get(j));
                    }
                    if (distance <= epsilon * epsilon) {
                        answers.add(new Pair<>(i, Math.sqrt(distance)));
                    }
                }
            } else if (problem.endsWith("NSM")) {  // NSM & cNSM
                // calculate mean and std of whole query series
                double ex = 0, ex2 = 0;
                for (Double value : query) {
                    ex += value;
                    ex2 += value * value;
                }
                double meanQ = ex / query.size();
                double stdQ = Math.sqrt(ex2 / query.size() - meanQ * meanQ);

                // sliding window
                double[] T = new double[2 * query.size()];
                ex = ex2 = 0;
                for (int i = 0; i < data.size(); i++) {
                    double d = data.get(i);
                    ex += d;
                    ex2 += d * d;
                    T[i % query.size()] = d;
                    T[(i % query.size()) + query.size()] = d;

                    if (i >= query.size() - 1) {
                        // the current starting location of T
                        int j = (i + 1) % query.size();

                        // z-normalization of T will be calculated on the fly
                        double mean = ex / query.size();
                        double std = Math.sqrt(ex2 / query.size() - mean * mean);

                        if (problem.equals("NSM") || (problem.equals("CNSM") && Math.abs(mean - meanQ) <= beta && (std / stdQ) <= alpha && (std / stdQ) >= 1.0 / alpha)) {
                            // calculate ED distance & test single point range criterion
                            double dist = 0;
                            for (int k = 0; k < query.size() && dist <= epsilon * epsilon; k++) {
                                double x = (T[k + j] - mean) / std;
                                double y = (query.get(k) - meanQ) / stdQ;
                                dist += (x - y) * (x - y);
                            }
                            if (dist <= epsilon * epsilon) {
                                answers.add(new Pair<>(i - query.size() + 1, Math.sqrt(dist)));
                            }
                        }

                        ex -= T[j];
                        ex2 -= T[j] * T[j];
                    }
                }
            }
        } else if (measure.equals("DTW")) {
            System.out.println("Not supported yet!");
        }

        // sort answers and remove duplicate neighboring ones
        answers.sort(Comparator.comparing(Pair::getSecond));
        List<Pair<Integer, Double>> uniqueAnswers = new ArrayList<>();
        boolean[] visited = new boolean[answers.size()];
        for (int i = 0; i < answers.size(); i++) {
            if (!visited[i]) {
                uniqueAnswers.add(answers.get(i));
                for (int j = i + 1; j < answers.size(); j++) {
                    if (!visited[j]) {
                        if (answers.get(j).getFirst() < answers.get(i).getFirst() + query.size() && answers.get(j).getFirst() + query.size() > answers.get(i).getFirst()) {
                            visited[j] = true;
                        }
                    }
                }
            }
        }

        // output answers
        for (Pair<Integer, Double> answer : uniqueAnswers) {
            System.out.println(answer.getFirst() + 1 + "," + answer.getSecond());
        }
    }
}
