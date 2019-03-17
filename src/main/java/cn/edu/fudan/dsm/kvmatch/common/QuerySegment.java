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

/**
 * Disjoint windows of query series
 * <p>
 * Created by Jiaye Wu on 16-8-9.
 */
public class QuerySegment {

    private double mean;

    private int order;

    private int count;

    private int Wu;

    public QuerySegment(double mean, int order, int count, int Wu) {
        this.mean = mean;
        this.order = order;
        this.count = count;
        this.Wu = Wu;
    }

    @Override
    public String toString() {
        return order + "(" + Wu + ")";
    }

    public double getMean() {
        return mean;
    }

    public void setMean(double mean) {
        this.mean = mean;
    }

    public int getOrder() {
        return order;
    }

    public void setOrder(int order) {
        this.order = order;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public int getWu() {
        return Wu;
    }

    public void setWu(int wu) {
        Wu = wu;
    }
}
