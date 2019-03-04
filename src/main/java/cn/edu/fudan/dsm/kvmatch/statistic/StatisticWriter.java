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
package cn.edu.fudan.dsm.kvmatch.statistic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * A file writer to output statistic information
 * <p>
 * Created by Jiaye Wu on 16-2-11.
 */
public class StatisticWriter {

    private static final Logger logger = LoggerFactory.getLogger(StatisticWriter.class.getName());

    private static Writer writer = null;

    static {
        try {
            File file = new File("statistic." + System.currentTimeMillis() + ".csv");
            if (!file.exists()) {
                file.createNewFile();
            }
            writer = new BufferedWriter(new FileWriter(file));
        } catch (IOException e) {
            logger.error(e.getMessage(), e.getCause());
        }
    }

    synchronized public static void println() {
        print(System.lineSeparator());
    }

    synchronized public static void println(String str) {
        print(str + System.lineSeparator());
    }

    synchronized public static void print(String str) {
        try {
            writer.write(str);
            writer.flush();
        } catch (IOException e) {
            logger.error(e.getMessage(), e.getCause());
        }
    }

    synchronized public static void close() {
        try {
            writer.close();
        } catch (IOException e) {
            logger.error(e.getMessage(), e.getCause());
        }
    }
}
