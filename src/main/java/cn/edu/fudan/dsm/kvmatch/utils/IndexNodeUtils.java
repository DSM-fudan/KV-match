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
import cn.edu.fudan.dsm.kvmatch.common.entity.IndexNode;
import cn.edu.fudan.dsm.kvmatch.mapreduce.common.LongIndexNode;

/**
 * In order to keep performance, we avoid type converts here but leave duplicate code instead.
 *
 * @author Jiaye Wu
 */
@SuppressWarnings("Duplicates")
public class IndexNodeUtils {

    public static IndexNode mergeIndexNode(IndexNode node1, IndexNode node2) {
        IndexNode ret = new IndexNode();

        int index1 = 0, index2 = 0;
        Pair<Integer, Integer> last1 = null, last2 = null;
        while (index1 < node1.getPositions().size() && index2 < node2.getPositions().size()) {
            Pair<Integer, Integer> position1 = node1.getPositions().get(index1);
            Pair<Integer, Integer> position2 = node2.getPositions().get(index2);
            if (last1 == null) last1 = new Pair<>(position1.getFirst(), position1.getSecond());
            if (last2 == null) last2 = new Pair<>(position2.getFirst(), position2.getSecond());

            if (last1.getSecond() + 1 < last2.getFirst()) {
                addInterval(ret, last1);
                index1++;
                last1 = null;
            } else if (last2.getSecond() + 1 < last1.getFirst()) {
                addInterval(ret, last2);
                index2++;
                last2 = null;
            } else {
                if (last1.getSecond() < last2.getSecond()) {
                    if (last1.getFirst() < last2.getFirst()) {
                        last2.setFirst(last1.getFirst());
                    }
                    index1++;
                    last1 = null;
                } else {
                    if (last2.getFirst() < last1.getFirst()) {
                        last1.setFirst(last2.getFirst());
                    }
                    index2++;
                    last2 = null;
                }
            }
        }
        for (int i = index1; i < node1.getPositions().size(); i++) {
            Pair<Integer, Integer> position1 = node1.getPositions().get(i);
            if (last1 == null) last1 = new Pair<>(position1.getFirst(), position1.getSecond());
            addInterval(ret, last1);
            last1 = null;
        }
        for (int i = index2; i < node2.getPositions().size(); i++) {
            Pair<Integer, Integer> position2 = node2.getPositions().get(i);
            if (last2 == null) last2 = new Pair<>(position2.getFirst(), position2.getSecond());
            addInterval(ret, last2);
            last2 = null;
        }

        return ret;
    }

    private static void addInterval(IndexNode node, Pair<Integer, Integer> position) {
        // diff can not exceed 255
        while (position.getSecond() - position.getFirst() >= IndexNode.MAXIMUM_DIFF) {
            int newFirst = position.getFirst() + IndexNode.MAXIMUM_DIFF - 1;
            node.getPositions().add(new Pair<>(position.getFirst(), newFirst));
            position.setFirst(newFirst + 1);
        }
        // add last part
        node.getPositions().add(position);
    }

    public static LongIndexNode mergeIndexNode(LongIndexNode node1, LongIndexNode node2) {
        LongIndexNode ret = new LongIndexNode();

        int index1 = 0, index2 = 0;
        Pair<Long, Long> last1 = null, last2 = null;
        while (index1 < node1.getPositions().size() && index2 < node2.getPositions().size()) {
            Pair<Long, Long> position1 = node1.getPositions().get(index1);
            Pair<Long, Long> position2 = node2.getPositions().get(index2);
            if (last1 == null) last1 = new Pair<>(position1.getFirst(), position1.getSecond());
            if (last2 == null) last2 = new Pair<>(position2.getFirst(), position2.getSecond());

            if (last1.getSecond() + 1 < last2.getFirst()) {
                addInterval(ret, last1);
                index1++;
                last1 = null;
            } else if (last2.getSecond() + 1 < last1.getFirst()) {
                addInterval(ret, last2);
                index2++;
                last2 = null;
            } else {
                if (last1.getSecond() < last2.getSecond()) {
                    if (last1.getFirst() < last2.getFirst()) {
                        last2.setFirst(last1.getFirst());
                    }
                    index1++;
                    last1 = null;
                } else {
                    if (last2.getFirst() < last1.getFirst()) {
                        last1.setFirst(last2.getFirst());
                    }
                    index2++;
                    last2 = null;
                }
            }
        }
        for (int i = index1; i < node1.getPositions().size(); i++) {
            Pair<Long, Long> position1 = node1.getPositions().get(i);
            if (last1 == null) last1 = new Pair<>(position1.getFirst(), position1.getSecond());
            addInterval(ret, last1);
            last1 = null;
        }
        for (int i = index2; i < node2.getPositions().size(); i++) {
            Pair<Long, Long> position2 = node2.getPositions().get(i);
            if (last2 == null) last2 = new Pair<>(position2.getFirst(), position2.getSecond());
            addInterval(ret, last2);
            last2 = null;
        }

        return ret;
    }

    private static void addInterval(LongIndexNode node, Pair<Long, Long> position) {
        // diff can not exceed 255
        while (position.getSecond() - position.getFirst() >= IndexNode.MAXIMUM_DIFF) {
            long newFirst = position.getFirst() + IndexNode.MAXIMUM_DIFF - 1;
            node.getPositions().add(new Pair<>(position.getFirst(), newFirst));
            position.setFirst(newFirst + 1);
        }
        // add last part
        node.getPositions().add(position);
    }
}
