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

import cn.edu.fudan.dsm.kvmatch.common.Pair;
import cn.edu.fudan.dsm.kvmatch.common.entity.IndexNode;
import cn.edu.fudan.dsm.kvmatch.operator.IndexOperator;
import cn.edu.fudan.dsm.kvmatch.utils.ByteUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * A wrapper class to interact with the index file.
 * <p>
 * Created by Jiaye Wu on 17-8-24.
 */
public class IndexFileOperator implements IndexOperator {

    private static final Logger logger = LoggerFactory.getLogger(IndexFileOperator.class.getName());

    private FileHandler fileHandler;

    private List<Integer> offsets;  // once open the file, read offsets
    private int offset;  // used to record current position during write

    public IndexFileOperator(String type, int N, int Wu, boolean rebuild) throws IOException {
        String path = "files" + File.separator + "index-" + (type.equals("standard") ? "" : type) + N + "-" + Wu;
        fileHandler = new FileHandler(path, rebuild ? "w" : "r");
        if (!rebuild) {  // read mode
            readOffsetInfo();
        }
    }

    private void readOffsetInfo() throws IOException {
        int fileLength = (int) fileHandler.getFile().length();
        if (fileLength < Bytes.SIZEOF_LONG) return;
        // read from the end of file, get (the position of offsets start)
        byte[] bytes = fileHandler.read(fileLength - Bytes.SIZEOF_INT, Bytes.SIZEOF_INT);
        int lastLineOffset = Bytes.toInt(bytes);
        int lastLineLength = fileLength - lastLineOffset;
        // get last line of all offsets
        bytes = fileHandler.read(lastLineOffset, lastLineLength);
        offsets = ByteUtils.byteArrayToListInt(bytes);
    }

    @Override
    public Map<Double, IndexNode> readIndexes(double keyFrom, double keyTo) throws IOException {
        Map<Double, IndexNode> indexes = new HashMap<>();  // sort increasingly by key
        int startOffsetId = lowerBound(keyFrom);  // find the first key >= keyFrom
        int endOffsetId = upperBound(keyTo);  // find the last key <= keyTo
        if (startOffsetId != -1 && endOffsetId != -1) {
            for (int i = startOffsetId; i <= endOffsetId; i++) {
                int lengthOfLine = offsets.get(i + 1) - offsets.get(i);
                byte[] bytes = fileHandler.read(offsets.get(i), lengthOfLine);
                double key = Bytes.toDouble(bytes, 0);
                byte[] valueBytes = new byte[bytes.length - Bytes.SIZEOF_DOUBLE];
                System.arraycopy(bytes, Bytes.SIZEOF_DOUBLE, valueBytes, 0, valueBytes.length);
                IndexNode value = new IndexNode();
                value.parseBytesCompact(valueBytes);
                indexes.put(key, value);
            }
        }
        return indexes;
    }

    @Override
    public List<Pair<Double, Pair<Integer, Integer>>> readStatisticInfo() throws IOException {
        if (offsets.isEmpty()) return null;
        int statisticInfoOffset = offsets.get(offsets.size() - 2);
        int offsetInfoOffset = offsets.get(offsets.size() - 1);
        byte[] bytes = fileHandler.read(statisticInfoOffset, offsetInfoOffset - statisticInfoOffset);
        return ByteUtils.byteArrayToListTriple(bytes);
    }

    private int lowerBound(double keyFrom) throws IOException {  // left range, return index of List<Integer> offsets
        int left = 0, right = offsets.size() - 3;
        while (left <= right) {
            int mid = left + ((right - left) >> 1);
            if (getKeyByOffset(offsets.get(mid)) >= keyFrom) {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }
        if (left <= offsets.size() - 3) return left;
        return -1;
    }

    private int upperBound(double keyTo) throws IOException {  // right range, return index of List<Integer> offsets
        int left = 0, right = offsets.size() - 3;
        while (left <= right) {
            int mid = left + ((right - left) >> 1);
            if (getKeyByOffset(offsets.get(mid)) <= keyTo) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }
        if (right >= 0) return right;
        return -1;
    }

    private double getKeyByOffset(int offset) throws IOException {
        byte[] bytes = fileHandler.read(offset, Bytes.SIZEOF_DOUBLE);
        return Bytes.toDouble(bytes);
    }

    @Override
    public void writeAll(Map<Double, IndexNode> sortedIndexes, List<Pair<Double, Pair<Integer, Integer>>> statisticInfo) throws IOException {
        offset = 0;
        offsets = new ArrayList<>(sortedIndexes.size() + 2);
        writeIndexes(sortedIndexes);
        writeStatisticInfo(statisticInfo);
        writeOffsetInfo();  // write file offset information to the end of file
    }

    private void writeIndexes(Map<Double, IndexNode> sortedIndexes) throws IOException {
        for (Map.Entry<Double, IndexNode> entry : sortedIndexes.entrySet()) {
            writeIndex(entry.getKey(), entry.getValue());
        }
    }

    private void writeIndex(double key, IndexNode value) throws IOException {
        offsets.add(offset);
        byte[] keyBytes = Bytes.toBytes(key);
        byte[] valueBytes = value.toBytesCompact();
        byte[] oneLineBytes = ByteUtils.combineTwoByteArrays(keyBytes, valueBytes);
        // write result and record offset
        fileHandler.write(oneLineBytes);
        offset += keyBytes.length + valueBytes.length;
    }

    private void writeStatisticInfo(List<Pair<Double, Pair<Integer, Integer>>> statisticInfo) throws IOException {
        offsets.add(offset);
        // store statistic information for query order optimization
        statisticInfo.sort(Comparator.comparingDouble(Pair::getFirst));
        byte[] result = ByteUtils.listTripleToByteArray(statisticInfo);
        // write result and record offset
        fileHandler.write(result);
        offset += result.length;
    }

    private void writeOffsetInfo() throws IOException {
        offsets.add(offset);
        fileHandler.write(ByteUtils.listIntToByteArray(offsets));
    }

    @Override
    public void close() throws IOException {
        fileHandler.close();
    }
}
