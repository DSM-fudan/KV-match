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

import cn.edu.fudan.dsm.kvmatch.QueryEngineDtw;
import cn.edu.fudan.dsm.kvmatch.statistic.StatisticInfo;
import cn.edu.fudan.dsm.kvmatch.statistic.StatisticWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

/**
 * Created by Jiaye Wu on 18-2-22.
 */
public class QueryDtwSelectivityGenerate {

    private static final Logger logger = LoggerFactory.getLogger(QueryDtwSelectivityGenerate.class);

    public static void main(String args[]) {
        int N;
        String storageType;
        if (args.length == 2) {
            N = Integer.parseInt(args[0]);
            storageType = args[1];
        } else {
            Scanner scanner = new Scanner(System.in);
            System.out.print("Data Length = ");
            N = scanner.nextInt();
            System.out.print("Storage type [file/hdfs/hbase/kudu] = ");
            storageType = scanner.next();
        }

        try {
            QueryEngineDtw queryEngine = new QueryEngineDtw(N, storageType);

            for (int k = 10; k <= 10; k++) {  // different query length
                int length = (int) Math.pow(2, k);  // 256, 512, 1024, 2048, 4096

                int aa = 90, bb = 100;
//                if (k == 8) { aa = 1; bb = 10; }
//                if (k == 9) { aa = 2; bb = 13; }
                //if (k == 10) { aa = 90; bb = 100;}
                //if (k == 11) { aa = 80; bb = 90; }
                //if (k == 12) { aa = 80; bb = 100; }
                for (int e = aa; e <= bb; e += 5) {  // different query delta
                    double epsilon = e == 0 ? 0.1 : e;

                    int rho = (int) (0.05 * length);

                    for (int o = 0; o < 20; o++) {
                        int offset = (new Random()).nextInt(N - length + 1) + 1;

                        // initialization: 0-T, 1-T_1, 2-T_2, 3-#candidates, 4-#answers, 5-#scans
                        List<StatisticInfo> statisticInfos = new ArrayList<>(6);
                        for (int j = 0; j < 6; j++) {
                            statisticInfos.add(new StatisticInfo());
                        }

                        // execute the query requests
                        boolean ret = queryEngine.query(statisticInfos, offset, length, epsilon, rho);
                        if (!ret) {  // warning for dismissals
                            StatisticWriter.println("No result for the query," + offset + "," + length + "," + epsilon + "," + rho);
                        }

                        // output statistic information
                        StatisticWriter.print(N + "," + offset + "," + length + "," + epsilon + "," + rho);
                        for (int i = 0; i < 6; i++) {
                            StatisticWriter.print("," + statisticInfos.get(i).getLast());
                        }
                        StatisticWriter.println();
                    }
                }
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e.getCause());
        }
    }
}
