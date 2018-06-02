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
package cn.edu.fudan.dsm.kvmatch.common;

import cn.edu.fudan.dsm.kvmatch.common.entity.IndexNode;

import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Index cache node
 * <p>
 * Created by Jiaye Wu on 16-4-23.
 */
public class IndexCache {

    private double beginRound;

    private double endRound;

    private NavigableMap<Double, IndexNode> caches;

    public IndexCache(double beginRound, double endRound) {
        this.beginRound = beginRound;
        this.endRound = endRound;
        caches = new ConcurrentSkipListMap<>();
    }

    public IndexCache(double beginRound, double endRound, NavigableMap<Double, IndexNode> caches) {
        this.beginRound = beginRound;
        this.endRound = endRound;
        this.caches = caches;
    }

    public void addCache(double meanRound, IndexNode indexNode) {
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

    public NavigableMap<Double, IndexNode> getCaches() {
        return caches;
    }

    public void setCaches(NavigableMap<Double, IndexNode> caches) {
        this.caches = caches;
    }
}
