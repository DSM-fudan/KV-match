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
package cn.edu.fudan.dsm.kvmatch.common.entity.rowkey;

import cn.edu.fudan.dsm.kvmatch.utils.StringUtils;

/**
 * Created by Jiaye Wu on 16-2-16.
 */
public class TimeSeriesRowKey {

    public static int ROWKEY_FIXED_WIDTH = 13;

    private long first;

    public TimeSeriesRowKey(long first) {
        this.first = first;
    }

    @Override
    public String toString() {
        return StringUtils.toStringFixedWidth(first, ROWKEY_FIXED_WIDTH);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TimeSeriesRowKey that = (TimeSeriesRowKey) o;

        return first == that.first;
    }

    @Override
    public int hashCode() {
        return (int) (first ^ (first >>> 32));
    }

    public long getFirst() {
        return first;
    }
}
