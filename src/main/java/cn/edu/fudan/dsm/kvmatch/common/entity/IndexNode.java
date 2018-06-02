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
package cn.edu.fudan.dsm.kvmatch.common.entity;

import cn.edu.fudan.dsm.kvmatch.common.Pair;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

/**
 * A node of the index
 * *
 * Created by Jiaye Wu on 16-3-24.
 */
public class IndexNode {

    public static int MAXIMUM_DIFF = 256;

    private List<Pair<Integer, Integer>> positions;

    public IndexNode() {
        positions = new ArrayList<>(100);
    }

    public byte[] toBytes() {
        /*
         * {left 1}{right 1}{left 2}{right 2}...{left n}{right n}
         */
        byte[] result = new byte[Bytes.SIZEOF_INT * positions.size() * 2];
        for (int i = 0; i < positions.size(); i++) {
            System.arraycopy(Bytes.toBytes(positions.get(i).getFirst()), 0, result, 2 * i * Bytes.SIZEOF_INT, Bytes.SIZEOF_INT);
            System.arraycopy(Bytes.toBytes(positions.get(i).getSecond()), 0, result, (2 * i + 1) * Bytes.SIZEOF_INT, Bytes.SIZEOF_INT);
        }
        return result;
    }

    public byte[] toBytesCompact() {
        /*
         * {left 1}{count 1}{right 1 - left 1}{left 2 - right 1}{right 2 - left 2}...{right count}{left count 1}...
         */
        byte[] result = new byte[Bytes.SIZEOF_INT * positions.size() * 2];

        int index = 0, length = 0, count = 0;
        boolean isPacking = false;
        while (index < positions.size()) {
            if (!isPacking) {
                System.arraycopy(Bytes.toBytes(positions.get(index).getFirst()), 0, result, length, Bytes.SIZEOF_INT);
                length += Bytes.SIZEOF_INT + Bytes.SIZEOF_BYTE;  // first: 4 bytes, remain 1 byte for count

                int diff = positions.get(index).getSecond() - positions.get(index).getFirst();
                result[length++] = (byte) (diff - 128);

                isPacking = true;
                count = 1;
            } else {
                int diff = positions.get(index).getFirst() - positions.get(index - 1).getSecond();
                if (diff < MAXIMUM_DIFF && (count - 1) / 2 + 2 < MAXIMUM_DIFF) {
                    result[length++] = (byte) (diff - 128);

                    diff = positions.get(index).getSecond() - positions.get(index).getFirst();
                    result[length++] = (byte) (diff - 128);

                    count += 2;
                } else {
                    // write count
                    result[length - count - 1] = (byte) ((count - 1) / 2 - 128);

                    isPacking = false;
                    continue;
                }
            }
            index++;
        }
        if (isPacking) {  // write last count
            result[length - count - 1] = (byte) ((count - 1) / 2 - 128);
        }
        // TODO: resize array, use bytebuffer instead?
        byte[] newArray = new byte[length];
        System.arraycopy(result, 0, newArray, 0, length);
        return newArray;
    }

    public void parseBytes(byte[] concatData) {
        if (concatData == null) return;
        byte[] tmp = new byte[Bytes.SIZEOF_INT];
        positions.clear();
        for (int i = 0; i < concatData.length; i += 2 * Bytes.SIZEOF_INT) {
            System.arraycopy(concatData, i, tmp, 0, Bytes.SIZEOF_INT);
            int left = Bytes.toInt(tmp);
            System.arraycopy(concatData, i + Bytes.SIZEOF_INT, tmp, 0, Bytes.SIZEOF_INT);
            int right = Bytes.toInt(tmp);
            positions.add(new Pair<>(left, right));
        }
    }

    public void parseBytesCompact(byte[] concatData) {
        if (concatData == null) return;
        positions.clear();

        int index = 0;
        while (index < concatData.length) {
            byte[] tmp = new byte[Bytes.SIZEOF_INT];
            System.arraycopy(concatData, index, tmp, 0, Bytes.SIZEOF_INT);
            int left = Bytes.toInt(tmp);
            index += Bytes.SIZEOF_INT;
            int count = (int) concatData[index++] + 128;
            int right = left + (int) concatData[index++] + 128;
            positions.add(new Pair<>(left, right));
            for (int i = 0; i < count; i++) {
                left = right + (int) concatData[index++] + 128;
                right = left + (int) concatData[index++] + 128;
                positions.add(new Pair<>(left, right));
            }
        }
    }

    @Override
    public String toString() {
        return "IndexNode{" + "positions=" + positions + '}';
    }

    public List<Pair<Integer, Integer>> getPositions() {
        return positions;
    }

    public void setPositions(List<Pair<Integer, Integer>> positions) {
        this.positions = positions;
    }

    private int getNumOfIntervals() {
        return positions.size();
    }

    private int getNumOfOffsets() {
        int sum = 0;
        for (Pair<Integer, Integer> pair : positions) {
            sum += pair.getSecond() - pair.getFirst() + 1;
        }
        return sum;
    }

    public Pair<Integer, Integer> getStatisticInfoPair() {
        return new Pair<>(getNumOfIntervals(), getNumOfOffsets());
    }
}
