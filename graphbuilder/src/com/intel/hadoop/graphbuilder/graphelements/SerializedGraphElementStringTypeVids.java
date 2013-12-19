/**
 * Copyright (C) 2013 Intel Corporation.
 *     All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * For more about this software visit:
 *     http://www.01.org/GraphBuilder
 */
package com.intel.hadoop.graphbuilder.graphelements;

import com.intel.hadoop.graphbuilder.types.StringType;
import org.apache.hadoop.io.WritableComparable;

/**
 * Serialized form of the {@code GraphElement} class that uses {@code StringType} vertex IDs.
 */

public class SerializedGraphElementStringTypeVids
        extends SerializedGraphElement<StringType> {

    /**
     * Allocates a new vertex ID.
     * @return  A new {@code StringType} object.
     *
     */

    public StringType createVid() {
        return new StringType();
    }

    /**
     * The compare function to enable key comparisons for
     * {@code WritableComparable} child classes.
     * @param {@code o}
     * @return -1 if less than o, 0 if equal to o, 1 if otherwise.
     */
    public int compareTo(SerializedGraphElementStringTypeVids o) {
        if (this.graphElement() == null && o.graphElement() == null) {
            return 0;
        } else if (this.graphElement() == null || o.graphElement() == null) {
            return 1;
        } else {
            return (this.graphElement().equals(o.graphElement())) ? 0 : 1;
        }
    }
}
