/**
 * Copyright (C) 2012 Intel Corporation.
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

import com.intel.hadoop.graphbuilder.types.LongType;
import com.intel.hadoop.graphbuilder.types.StringType;

/**
 * A Factory class that creates a concrete {@code SerializedPropertyGraphElement}
 * which is used in {@code TextGraphMR} as the intermediate value type.
 */
public class ValueClassFactory {

    /**
     * @param  vidClassName
     * @return a Class which inherits {@code SerializedPropertyGraphElement} and
     *         overrides {@code createVid} to return the correct vertex ID class.
     * @throws IllegalArgumentException
     */

    public static Class getValueClassByVidClassName(String vidClassName) throws IllegalArgumentException {

        if (vidClassName.equals(StringType.class.getName())) {
            return SerializedPropertyGraphElementStringTypeVids.class;
        } else if (vidClassName.equals(LongType.class.getName())) {
            return SerializedPropertyGraphElementLongTypeVids.class;
        } else {
            throw new IllegalArgumentException("Illegal vertex ID class " + vidClassName);
        }
    }
}
