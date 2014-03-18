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

package com.intel.hadoop.graphbuilder.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * A map which provides a simple interface for storing a set of values for each key.
 * <p>Just a user-friendly wrapper around <code>HashMap &lt Key, Set &lt Value &gt &gt </code></p>
 *
 * @param <Key>   The key class.
 * @param <Value> The value class.
 */
public class MultiValuedMap<Key, Value> {

    private HashMap<Key, Set<Value>> map;

    /**
     * Allocates an initially empty mapping.
     */
    public MultiValuedMap() {
        map = new HashMap<Key, Set<Value>>();
    }

    /**
     * Adds a key to the map but does not attach any values to its entry.
     *
     * @param key The key which is being added to the map.
     * @return <code>true</code> if the key was successfully added, <code>false</code> if it could not be added because
     * it was already in the map.
     */
    public boolean addKey(Key key) {
        if (!map.containsKey(key)) {
            Set<Value> set = new HashSet<Value>();
            map.put(key, set);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Adds a value to the set belonging to a key.
     *
     * @param key   The key of the set which is being augmented.
     * @param value The value being added to the set.
     * @return <code>true</code> if <code>value</code> was successfully added to the set.
     */
    public boolean add(Key key, Value value) {

        if (!map.containsKey(key)) {
            this.addKey(key);
        }

        return map.get(key).add(value);
    }

    /**
     * Get the set of values belonging to a key.
     *
     * @param key The key used to select the set.
     * @return The set of values belonging to the key. If the key is not present in the map, <code>null</code>
     * is returned.
     */
    public Set<Value> getValues(Key key) {

        if (!map.containsKey(key)) {
            return null;
        }

        return map.get(key);
    }

    /**
     * @return The key set of the map.
     */
    public Set<Key> keySet() {
        return map.keySet();
    }
}
