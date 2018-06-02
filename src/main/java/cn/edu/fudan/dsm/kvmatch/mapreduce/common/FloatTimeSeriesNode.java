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

import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

/**
 * A node of the time series
 * <p>
 * Created by Jiaye Wu on 17-8-16.
 */
public class FloatTimeSeriesNode {

    public static int ROW_LENGTH = 1000;

    private List<Float> data;

    public FloatTimeSeriesNode() {
        this.data = new ArrayList<>();
    }

    public byte[] toBytes() {
        byte[] result = new byte[Bytes.SIZEOF_FLOAT * data.size()];
        for (int i = 0; i < data.size(); i++) {
            System.arraycopy(Bytes.toBytes(data.get(i)), 0, result, Bytes.SIZEOF_FLOAT * i, Bytes.SIZEOF_FLOAT);
        }
        return result;
    }

    public void parseBytes(byte[] concatData) {
        for (int i = 0; i < concatData.length / Bytes.SIZEOF_FLOAT; i++) {
            byte[] temp = new byte[Bytes.SIZEOF_FLOAT];
            System.arraycopy(concatData, Bytes.SIZEOF_FLOAT * i, temp, 0, Bytes.SIZEOF_FLOAT);
            data.add(Bytes.toFloat(temp));
        }
    }

    @Override
    public String toString() {
        return "TimeSeriesNode{" + "data=" + data + '}';
    }

    public List<Float> getData() {
        return data;
    }

    public void setData(List<Float> data) {
        this.data = data;
    }
}
