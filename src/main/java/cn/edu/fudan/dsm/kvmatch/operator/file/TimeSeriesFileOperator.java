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
package cn.edu.fudan.dsm.kvmatch.operator.file;

import cn.edu.fudan.dsm.kvmatch.common.entity.TimeSeriesNode;
import cn.edu.fudan.dsm.kvmatch.common.entity.rowkey.TimeSeriesRowKey;
import cn.edu.fudan.dsm.kvmatch.operator.TimeSeriesOperator;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A wrapper class to interact with the data file.
 * Only support time series whose length is less than INT_MAX.
 *
 * Created by Jiaye Wu on 17-8-24.
 */
public class TimeSeriesFileOperator implements TimeSeriesOperator {

    private static final Logger logger = LoggerFactory.getLogger(TimeSeriesFileOperator.class.getName());

    private int N;

    private FileHandler fileHandler;

    private long lastPos;
    private int lastLengthOfBytes;
    private byte[] lastBytes;

    public TimeSeriesFileOperator(int N, boolean rebuild) throws IOException {
        this.N = N;
        fileHandler = new FileHandler("files" + File.separator + "data-" + N, rebuild ? "w" : "r");
    }

    @Override
    public List<Double> readTimeSeries(int left, int length) throws IOException {
        if (left < 1 || left + length - 1 > N || length < 1) {
            throw new IllegalArgumentException("left: " + left + ", length: " + length);
        }

        int nodeBytes = TimeSeriesNode.ROW_LENGTH * Bytes.SIZEOF_DOUBLE;
        int begin = (left - 1) / TimeSeriesNode.ROW_LENGTH * TimeSeriesNode.ROW_LENGTH + 1;
        int end = (left + length - 2) / TimeSeriesNode.ROW_LENGTH * TimeSeriesNode.ROW_LENGTH + 1;
        long pos = (left - 1) / TimeSeriesNode.ROW_LENGTH * (long) nodeBytes;
        int lengthOfBytes = (end - begin + TimeSeriesNode.ROW_LENGTH) * Bytes.SIZEOF_DOUBLE;

        byte[] bytes;
        if (pos == lastPos && lengthOfBytes == lastLengthOfBytes) {  // avoid duplicate file reads
            bytes = lastBytes;
        } else {
            bytes = fileHandler.read(pos, lengthOfBytes);
            lastPos = pos;
            lastLengthOfBytes = lengthOfBytes;
            lastBytes = bytes;
        }

        List<Double> ret = new ArrayList<>(length);
        for (int i = 0; i < bytes.length; i += nodeBytes) {
            TimeSeriesNode node = new TimeSeriesNode();
            byte[] tmp = new byte[nodeBytes];
            System.arraycopy(bytes, i, tmp, 0, nodeBytes);
            node.parseBytes(tmp);
            if (i == 0) {
                int offset = (left - 1) % TimeSeriesNode.ROW_LENGTH;
                for (int j = offset; j < Math.min(offset + length, TimeSeriesNode.ROW_LENGTH); j++) {
                    ret.add(node.getData().get(j));
                }
            } else if (i == bytes.length - nodeBytes) {
                int offset = (left + length - 2) % TimeSeriesNode.ROW_LENGTH;
                for (int j = 0; j <= offset; j++) {
                    ret.add(node.getData().get(j));
                }
            } else {
                ret.addAll(node.getData());
            }
        }
        return ret;
    }

    @Override
    public TimeSeriesNodeIterator readAllTimeSeries() throws IOException {
        return new TimeSeriesNodeIterator(fileHandler.getFile(), TimeSeriesNode.ROW_LENGTH);
    }

    @Override
    public void writeTimeSeriesNode(TimeSeriesRowKey rowKey, TimeSeriesNode node) throws IOException {
        fileHandler.write(node.toBytes());
    }

    @Override
    public void close() throws IOException {
        fileHandler.close();
    }
}
