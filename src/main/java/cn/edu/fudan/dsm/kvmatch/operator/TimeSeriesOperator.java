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
package cn.edu.fudan.dsm.kvmatch.operator;

import cn.edu.fudan.dsm.kvmatch.common.entity.TimeSeriesNode;
import cn.edu.fudan.dsm.kvmatch.common.entity.rowkey.TimeSeriesRowKey;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Jiaye Wu on 17-8-24.
 */
public interface TimeSeriesOperator extends Closeable {

    /**
     * Read time series in specific interval.
     *
     * @param left   the start time
     * @param length the length of time series
     * @return the time series data in the given time interval
     */
    List readTimeSeries(int left, int length) throws IOException;

    /**
     * Read time series in streaming fashion.
     *
     * @return the iterator used to read data
     */
    Iterator readAllTimeSeries() throws IOException;

    /**
     * Write a time series node into data (file/HBase table)
     *
     * @param rowKey row key of the time series node
     * @param node   time series node containing data
     */
    void writeTimeSeriesNode(TimeSeriesRowKey rowKey, TimeSeriesNode node) throws IOException;
}
