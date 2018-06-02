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

import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

/**
 * A node of the time series
 * <p>
 * Created by Jiaye Wu on 16-2-16.
 */
public class TimeSeriesNode {

    public static int ROW_LENGTH = 1000;

    private List<Double> data;

    public TimeSeriesNode() {
        this.data = new ArrayList<>();
    }

    public byte[] toBytes() {
        byte[] result = new byte[Bytes.SIZEOF_DOUBLE * data.size()];
        for (int i = 0; i < data.size(); i++) {
            System.arraycopy(Bytes.toBytes(data.get(i)), 0, result, Bytes.SIZEOF_DOUBLE * i, Bytes.SIZEOF_DOUBLE);
        }
        return result;
    }

    public void parseBytes(byte[] concatData) {
        for (int i = 0; i < concatData.length / Bytes.SIZEOF_DOUBLE; i++) {
            byte[] temp = new byte[Bytes.SIZEOF_DOUBLE];
            System.arraycopy(concatData, Bytes.SIZEOF_DOUBLE * i, temp, 0, Bytes.SIZEOF_DOUBLE);
            data.add(Bytes.toDouble(temp));
        }
    }

    @Override
    public String toString() {
        return "TimeSeriesNode{" + "data=" + data + '}';
    }

    public List<Double> getData() {
        return data;
    }

    public void setData(List<Double> data) {
        this.data = data;
    }
}
