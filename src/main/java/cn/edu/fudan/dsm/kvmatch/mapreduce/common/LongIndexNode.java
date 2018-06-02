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
package cn.edu.fudan.dsm.kvmatch.mapreduce.common;

import cn.edu.fudan.dsm.kvmatch.common.Pair;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Jiaye Wu on 17-8-1.
 * <p/>
 * A node of the index
 * <p/>
 * Properties: positions(left, right)
 */
public class LongIndexNode implements Writable {

    public static int MAXIMUM_DIFF = 256;

    private List<Pair<Long, Long>> positions;

    public LongIndexNode() {
        positions = new ArrayList<>();
    }

    public static LongIndexNode read(DataInput in) throws IOException {
        LongIndexNode node = new LongIndexNode();
        node.readFields(in);
        return node;
    }

    public byte[] toBytes() {
        /*
         * {left 1}{right 1}{left 2}{right 2}...{left n}{right n}
         */
        byte[] result = new byte[Bytes.SIZEOF_LONG * positions.size() * 2];
        for (int i = 0; i < positions.size(); i++) {
            System.arraycopy(Bytes.toBytes(positions.get(i).getFirst()), 0, result, 2 * i * Bytes.SIZEOF_LONG, Bytes.SIZEOF_LONG);
            System.arraycopy(Bytes.toBytes(positions.get(i).getSecond()), 0, result, (2 * i + 1) * Bytes.SIZEOF_LONG, Bytes.SIZEOF_LONG);
        }
        return result;
    }

    public byte[] toBytesCompact() {
        /*
         * {left 1}{count 1}{right 1 - left 1}{left 2 - right 1}{right 2 - left 2}...{right count}{left count 1}...
         */
        byte[] result = new byte[Bytes.SIZEOF_LONG * positions.size() * 2];

        int index = 0, length = 0, count = 0;
        boolean isPacking = false;
        while (index < positions.size()) {
            if (!isPacking) {
                System.arraycopy(Bytes.toBytes(positions.get(index).getFirst()), 0, result, length, Bytes.SIZEOF_LONG);
                length += Bytes.SIZEOF_LONG + Bytes.SIZEOF_BYTE;  // first: 4 bytes, remain 1 byte for count

                long diff = positions.get(index).getSecond() - positions.get(index).getFirst();
                result[length++] = (byte) (diff - 128);

                isPacking = true;
                count = 1;
            } else {
                long diff = positions.get(index).getFirst() - positions.get(index - 1).getSecond();
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
        byte[] tmp = new byte[Bytes.SIZEOF_LONG];
        positions.clear();
        for (int i = 0; i < concatData.length; i += 2 * Bytes.SIZEOF_LONG) {
            System.arraycopy(concatData, i, tmp, 0, Bytes.SIZEOF_LONG);
            long left = Bytes.toLong(tmp);
            System.arraycopy(concatData, i + Bytes.SIZEOF_LONG, tmp, 0, Bytes.SIZEOF_LONG);
            long right = Bytes.toLong(tmp);
            positions.add(new Pair<>(left, right));
        }
    }

    public void parseBytesCompact(byte[] concatData) {
        if (concatData == null) return;
        positions.clear();

        int index = 0;
        while (index < concatData.length) {
            byte[] tmp = new byte[Bytes.SIZEOF_LONG];
            System.arraycopy(concatData, index, tmp, 0, Bytes.SIZEOF_LONG);
            long left = Bytes.toLong(tmp);
            index += Bytes.SIZEOF_LONG;
            int count = (int) concatData[index++] + 128;
            long right = left + (int) concatData[index++] + 128;
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

    public List<Pair<Long, Long>> getPositions() {
        return positions;
    }

    public void setPositions(List<Pair<Long, Long>> positions) {
        this.positions = positions;
    }

    private long getNumOfIntervals() {
        return positions.size();
    }

    private long getNumOfOffsets() {
        long sum = 0;
        for (Pair<Long, Long> pair : positions) {
            sum += pair.getSecond() - pair.getFirst() + 1;
        }
        return sum;
    }

    public Pair<Long, Long> getStatisticInfoPair() {
        return new Pair<>(getNumOfIntervals(), getNumOfOffsets());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(positions.size());
        for (Pair<Long, Long> position : positions) {
            out.writeLong(position.getFirst());
            out.writeLong(position.getSecond());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        positions.clear();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            long left = in.readLong();
            long right = in.readLong();
            positions.add(new Pair<>(left, right));
        }
    }
}
