package cn.edu.fudan.dsm.kvmatch.experiments;

import cn.edu.fudan.dsm.kvmatch.QueryEngine;
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
 * Created by Jiaye Wu on 17-8-28.
 */
@SuppressWarnings("Duplicates")
public class QueryTestGroupBySelectivity {

    private static final Logger logger = LoggerFactory.getLogger(QueryTestGroupBySelectivity.class);

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        System.out.print("Data Length = ");
        int N = scanner.nextInt();
        System.out.print("Storage type [file/hbase/kudu] = ");
        String storageType = scanner.next();

        try {
            QueryEngine queryEngine = new QueryEngine(N, storageType);

            int minSelectivity = (int) Math.log10(N);
            for (int i = minSelectivity; i >= minSelectivity - 4; i--) {  // 9 8 7 6 5
                String filename = 1 + "e-" + i + "(S)";  // {1}e-{i}, e.g. 1e-3
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

                // initialization: 0-T, 1-T_1, 2-T_2, 3-#candidates, 4-#answers, 5-#scans
                List<StatisticInfo> statisticInfos = new ArrayList<>(6);
                for (int j = 0; j < 6; j++) {
                    statisticInfos.add(new StatisticInfo());
                }

                // execute the query requests
                for (int j = 0; j < queryOffsets.size(); j++) {
                    boolean ret = queryEngine.query(statisticInfos, queryOffsets.get(j), queryLengths.get(j), queryEpsilons.get(j));
                    if (!ret) {
                        StatisticWriter.println("No result for the query," + queryOffsets.get(j) + "," + queryLengths.get(j) + "," + queryEpsilons.get(j));
                    }
                }

                // output statistic information
                logger.info("T: {} ms, T_1: {} ms, T_2: {} ms, #candidates: {}, #answers: {}, #scans: {}", statisticInfos.get(0).getAverage(), statisticInfos.get(1).getAverage(), statisticInfos.get(2).getAverage(), statisticInfos.get(3).getAverage(), statisticInfos.get(4).getAverage(), statisticInfos.get(5).getAverage());
                for (int j = 0; j < 6; j++) {
                    StatisticWriter.print(statisticInfos.get(j).getAverage() + ",");
                }
                StatisticWriter.println("");
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e.getCause());
        }
    }
}
