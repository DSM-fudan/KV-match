/*
 * Copyright 2018 Jiaye Wu
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
 * Created by wujy on 16-2-23.
 */
public class CircularArray {

    private int[] dq;

    private int size;

    private int capacity;

    private int f;

    private int r;

    /**
     * Initial the queue at the beginning step of envelop calculation
     *
     * @param capacity the size of the circular array
     */
    public CircularArray(int capacity) {
        this.capacity = capacity;
        this.size = 0;
        this.dq = new int[capacity];
        this.f = 0;
        this.r = capacity - 1;
    }

    /**
     * Insert to the queue at the back
     *
     * @param v the inserted value
     */
    public void pushBack(int v) {
        dq[r] = v;
        r--;
        if (r < 0) {
            r = capacity - 1;
        }
        size++;
    }

    /**
     * Delete the current (front) element from queue
     */
    public void popFront() {
        f--;
        if (f < 0) {
            f = capacity - 1;
        }
        size--;
    }

    /**
     * Delete the last element from queue
     */
    public void popBack() {
        r = (r + 1) % capacity;
        size--;
    }

    /**
     * Get the value at the current position of the circular queue
     *
     * @return the value
     */
    public int front() {
        int aux = f - 1;
        if (aux < 0) {
            aux = capacity - 1;
        }
        return dq[aux];
    }

    /**
     * Get the value at the last position of the circular queue
     *
     * @return the value
     */
    public int back() {
        int aux = (r + 1) % capacity;
        return dq[aux];
    }

    /**
     * Check whether or not the queue is empty
     *
     * @return true if it's empty, false if it's not
     */
    public boolean isEmpty() {
        return size == 0;
    }
}

