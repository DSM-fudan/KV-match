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

/**
 * Utilities for operations on String
 * <p>
 * Created by Jiaye Wu on 16-2-10.
 */
public class StringUtils {

    /**
     * Generate fixed length string from integer
     *
     * @param x     the integer want to parse to string
     * @param width desired length of the output string
     * @return the length-width string from x
     */
    public static String toStringFixedWidth(long x, int width) {
        String str = String.valueOf(x);
        if (str.length() > width) {
            throw new IllegalArgumentException("width is too short (x: " + str + ", width: " + width + ")");
        }
        StringBuilder sb = new StringBuilder(str);
        for (int i = str.length(); i < width; i++) {
            sb.insert(0, "0");
        }
        return sb.toString();
    }
}

