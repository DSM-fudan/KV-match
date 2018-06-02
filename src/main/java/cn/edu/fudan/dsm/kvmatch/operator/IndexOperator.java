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

import cn.edu.fudan.dsm.kvmatch.common.Pair;
import cn.edu.fudan.dsm.kvmatch.common.entity.IndexNode;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by Jiaye Wu on 17-8-24.
 */
public interface IndexOperator extends Closeable {

    /**
     * Read index rows from the index file.
     *
     * @param keyFrom the lower-bound of the key (inclusive)
     * @param keyTo   the upper-bound of the key (inclusive)
     * @return index rows whose keys in the specified range
     * @throws IOException if any error occurred during reading process
     */
    Map<Double, IndexNode> readIndexes(double keyFrom, double keyTo) throws IOException;

    /**
     * Read the statistic information (metadata) of the index, e.g. the number of offsets and intervals in every row.
     *
     * @return statistic information (metadata) of this index
     * @throws IOException if any error occurred during reading process
     */
    List<Pair<Double, Pair<Integer, Integer>>> readStatisticInfo() throws IOException;

    /**
     * Write the whole index structure to index file.
     * Including the index rows, statistic information (metadata) and offset information used to random access index rows.
     *
     * @param sortedIndexes the index rows ordered by the key in ascending order
     * @param statisticInfo the statistic information (metadata) corresponding to the index
     * @throws IOException if any error occurred during write process
     */
    void writeAll(Map<Double, IndexNode> sortedIndexes, List<Pair<Double, Pair<Integer, Integer>>> statisticInfo) throws IOException;
}
