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
package cn.edu.fudan.dsm.kvmatch.utils;

import cn.edu.fudan.dsm.kvmatch.common.Pair;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

/**
 * This is the class implementing conversion of objects and byte arrays.
 * <p>
 * Created by Ningting Pan on 17-8-24.
 */
public class ByteUtils {

    public static byte[] listLongToByteArray(List<Long> offsets) {
        byte[] bytes = new byte[Bytes.SIZEOF_LONG * offsets.size()];
        int curOffset = 0;
        for (long offset : offsets) {
            System.arraycopy(Bytes.toBytes(offset), 0, bytes, curOffset, Bytes.SIZEOF_LONG);
            curOffset += Bytes.SIZEOF_LONG;
        }
        return bytes;
    }

    public static byte[] listIntToByteArray(List<Integer> offsets) {
        byte[] bytes = new byte[Bytes.SIZEOF_INT * offsets.size()];
        int curOffset = 0;
        for (int offset : offsets) {
            System.arraycopy(Bytes.toBytes(offset), 0, bytes, curOffset, Bytes.SIZEOF_INT);
            curOffset += Bytes.SIZEOF_INT;
        }
        return bytes;
    }

    public static List<Long> byteArrayToListLong(byte[] bytes) {
        List<Long> offsets = new ArrayList<>();
        int size = bytes.length / Bytes.SIZEOF_LONG;
        int curOffset = 0;
        for (int i = 0; i < size; i++) {
            byte[] tmp = new byte[Bytes.SIZEOF_LONG];
            System.arraycopy(bytes, curOffset, tmp, 0, Bytes.SIZEOF_LONG);
            offsets.add(Bytes.toLong(tmp));
            curOffset += Bytes.SIZEOF_LONG;
        }
        return offsets;
    }

    public static List<Integer> byteArrayToListInt(byte[] bytes) {
        List<Integer> offsets = new ArrayList<>();
        int size = bytes.length / Bytes.SIZEOF_INT;
        int curOffset = 0;
        for (int i = 0; i < size; i++) {
            byte[] tmp = new byte[Bytes.SIZEOF_INT];
            System.arraycopy(bytes, curOffset, tmp, 0, Bytes.SIZEOF_INT);
            offsets.add(Bytes.toInt(tmp));
            curOffset += Bytes.SIZEOF_INT;
        }
        return offsets;
    }

    public static byte[] combineTwoByteArrays(byte[] firstBytes, byte[] secondBytes) {
        byte[] combineBytes = new byte[firstBytes.length + secondBytes.length];
        System.arraycopy(firstBytes, 0, combineBytes, 0, firstBytes.length);
        System.arraycopy(secondBytes, 0, combineBytes, firstBytes.length, secondBytes.length);
        return combineBytes;
    }

    public static byte[] listTripleToByteArray(List<Pair<Double, Pair<Integer, Integer>>> statisticInfo) {
        byte[] result = new byte[(Bytes.SIZEOF_DOUBLE + 2 * Bytes.SIZEOF_INT) * statisticInfo.size()];
        System.arraycopy(Bytes.toBytes(statisticInfo.get(0).getFirst()), 0, result, 0, Bytes.SIZEOF_DOUBLE);
        System.arraycopy(Bytes.toBytes(statisticInfo.get(0).getSecond().getFirst()), 0, result, Bytes.SIZEOF_DOUBLE, Bytes.SIZEOF_INT);
        System.arraycopy(Bytes.toBytes(statisticInfo.get(0).getSecond().getSecond()), 0, result, Bytes.SIZEOF_DOUBLE + Bytes.SIZEOF_INT, Bytes.SIZEOF_INT);
        for (int i = 1; i < statisticInfo.size(); i++) {
            statisticInfo.get(i).getSecond().setFirst(statisticInfo.get(i).getSecond().getFirst() + statisticInfo.get(i - 1).getSecond().getFirst());
            statisticInfo.get(i).getSecond().setSecond(statisticInfo.get(i).getSecond().getSecond() + statisticInfo.get(i - 1).getSecond().getSecond());
            System.arraycopy(Bytes.toBytes(statisticInfo.get(i).getFirst()), 0, result, i * (Bytes.SIZEOF_DOUBLE + 2 * Bytes.SIZEOF_INT), Bytes.SIZEOF_DOUBLE);
            System.arraycopy(Bytes.toBytes(statisticInfo.get(i).getSecond().getFirst()), 0, result, i * (Bytes.SIZEOF_DOUBLE + 2 * Bytes.SIZEOF_INT) + Bytes.SIZEOF_DOUBLE, Bytes.SIZEOF_INT);
            System.arraycopy(Bytes.toBytes(statisticInfo.get(i).getSecond().getSecond()), 0, result, i * (Bytes.SIZEOF_DOUBLE + 2 * Bytes.SIZEOF_INT) + Bytes.SIZEOF_DOUBLE + Bytes.SIZEOF_INT, Bytes.SIZEOF_INT);
        }
        return result;
    }

    public static List<Pair<Double, Pair<Integer, Integer>>> byteArrayToListTriple(byte[] bytes) {
        List<Pair<Double, Pair<Integer, Integer>>> statisticInfo = new ArrayList<>();
        int tripleSize = Bytes.SIZEOF_DOUBLE + 2 * Bytes.SIZEOF_INT;
        int infoSize = bytes.length / tripleSize;
        int curOffset = 0;
        for (int i = 0; i < infoSize; i++) {
            byte[] t1 = new byte[Bytes.SIZEOF_DOUBLE];
            byte[] t2 = new byte[Bytes.SIZEOF_INT];
            byte[] t3 = new byte[Bytes.SIZEOF_INT];
            System.arraycopy(bytes, curOffset, t1, 0, Bytes.SIZEOF_DOUBLE);
            curOffset += Bytes.SIZEOF_DOUBLE;
            System.arraycopy(bytes, curOffset, t2, 0, Bytes.SIZEOF_INT);
            curOffset += Bytes.SIZEOF_INT;
            System.arraycopy(bytes, curOffset, t3, 0, Bytes.SIZEOF_INT);
            curOffset += Bytes.SIZEOF_INT;
            statisticInfo.add(new Pair<>(Bytes.toDouble(t1), new Pair<>(Bytes.toInt(t2), Bytes.toInt(t3))));
        }
        return statisticInfo;
    }
}
