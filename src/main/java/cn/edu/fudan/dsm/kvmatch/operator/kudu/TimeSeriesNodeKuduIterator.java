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
package cn.edu.fudan.dsm.kvmatch.operator.kudu;

import cn.edu.fudan.dsm.kvmatch.common.Pair;
import cn.edu.fudan.dsm.kvmatch.common.entity.TimeSeriesNode;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;

import java.io.IOException;
import java.util.Iterator;

public class TimeSeriesNodeKuduIterator implements Iterator {

    private KuduScanner scanner;
    private RowResultIterator results = null;
    private Pair<Long, TimeSeriesNode> next = null;

    public TimeSeriesNodeKuduIterator(KuduScanner scanner) {
        this.scanner = scanner;
    }

    @Override
    public boolean hasNext() {
        if (next == null) {
            try {
                if (results == null || !results.hasNext()) {
                    if (scanner.hasMoreRows()) {
                        results = scanner.nextRows();
                        if (!results.hasNext()) {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                RowResult row = results.next();
                long time = row.getLong("time");
                byte[] bytes = row.getBinary("value").array();
                TimeSeriesNode node = new TimeSeriesNode();
                node.parseBytes(bytes);
                next = new Pair<>(time, node);
                return true;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return true;
    }

    @Override
    public Object next() {
        if (!hasNext()) {
            return null;
        }

        Pair<Long, TimeSeriesNode> temp = next;
        next = null;
        return temp;
    }
}
