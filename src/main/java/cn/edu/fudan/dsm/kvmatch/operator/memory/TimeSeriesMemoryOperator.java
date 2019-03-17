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
package cn.edu.fudan.dsm.kvmatch.operator.memory;

import cn.edu.fudan.dsm.kvmatch.common.entity.TimeSeriesNode;
import cn.edu.fudan.dsm.kvmatch.common.entity.rowkey.TimeSeriesRowKey;
import cn.edu.fudan.dsm.kvmatch.operator.TimeSeriesOperator;
import cn.edu.fudan.dsm.kvmatch.operator.file.TimeSeriesNodeIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class TimeSeriesMemoryOperator implements TimeSeriesOperator {

    private static final Logger logger = LoggerFactory.getLogger(TimeSeriesMemoryOperator.class.getName());

    private int N;
    private List<Double> data;

    public TimeSeriesMemoryOperator(int N) {
        this.N = N;
        loadData();
    }

    private void loadData() {
        data = new ArrayList<>(N + 10);
        long startTime = System.currentTimeMillis();
        File file = new File("files" + File.separator + "data-" + N);
        try (DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)))) {
            for (int i = 1; i <= N; i++) {
                data.add(dis.readDouble());
                logger.debug("Data #{} - {}", i, data.get(i - 1));
            }
        } catch (EOFException e) {
            // do nothing
        } catch (IOException e) {
            logger.error(e.getMessage(), e.getCause());
        }
        long endTime = System.currentTimeMillis();
        logger.info("Data load time: {} ms", endTime - startTime);
    }

    @Override
    public List<Double> readTimeSeries(int left, int length) {
        if (left < 1 || left + length - 1 > N || length < 1) {
            throw new IllegalArgumentException("left: " + left + ", length: " + length);
        }

        return data.subList(left - 1, left - 1 + length);
    }

    @Override
    public TimeSeriesNodeIterator readAllTimeSeries() {
        throw new UnsupportedOperationException("unimplemented.");
    }

    @Override
    public void writeTimeSeriesNode(TimeSeriesRowKey rowKey, TimeSeriesNode node) {
        throw new UnsupportedOperationException("unimplemented.");
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("unimplemented.");
    }
}
