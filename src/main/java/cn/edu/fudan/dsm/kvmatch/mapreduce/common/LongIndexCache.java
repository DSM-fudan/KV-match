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

import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Created by wujy on 17-8-1.
 */
public class LongIndexCache {

    private double beginRound;

    private double endRound;

    private NavigableMap<Double, LongIndexNode> caches;

    public LongIndexCache(double beginRound, double endRound) {
        this.beginRound = beginRound;
        this.endRound = endRound;
        caches = new ConcurrentSkipListMap<>();
    }

    public LongIndexCache(double beginRound, double endRound, NavigableMap<Double, LongIndexNode> caches) {
        this.beginRound = beginRound;
        this.endRound = endRound;
        this.caches = caches;
    }

    public void addCache(double meanRound, LongIndexNode indexNode) {
        caches.put(meanRound, indexNode);
    }

    public double getBeginRound() {
        return beginRound;
    }

    public void setBeginRound(double beginRound) {
        this.beginRound = beginRound;
    }

    public double getEndRound() {
        return endRound;
    }

    public void setEndRound(double endRound) {
        this.endRound = endRound;
    }

    public NavigableMap<Double, LongIndexNode> getCaches() {
        return caches;
    }

    public void setCaches(NavigableMap<Double, LongIndexNode> caches) {
        this.caches = caches;
    }
}
