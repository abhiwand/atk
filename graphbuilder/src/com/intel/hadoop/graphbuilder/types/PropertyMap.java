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
package com.intel.hadoop.graphbuilder.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.MapWritable;

/**
 *  implements a property map
 *  MapWritable with a friendly toString() method
 */
public class PropertyMap implements Writable
{
    private MapWritable properties = null;

    public PropertyMap() {
        this.properties = new MapWritable();
    }

    public void setProperty(final String key, final Writable value) {
        StringType keyStr = new StringType(key);
        this.properties.put(keyStr, value);
    }

    public Writable removeProperty(final String key) {
        StringType keyStr = new StringType(key);
        return properties.remove(keyStr);
    }

    public Writable getProperty(final String key) {
        StringType keyStr = new StringType(key);
        return this.properties.get(keyStr);
    }

    public Set<Writable> getPropertyKeys() {
        return this.properties.keySet();
    }

    public void mergeProperties(PropertyMap propertyMap) {
        for(Writable key : propertyMap.getPropertyKeys()) {
            this.setProperty(key.toString(), propertyMap.getProperty(key.toString()));
        }
    }

    @Override
    public  void readFields(DataInput in) throws IOException {
        this.properties.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.properties.write(out);
    }


    @Override
    public int hashCode() {
        return properties.hashCode();
    }

    @Override
    public String toString() {

        String s = new String("");

        if (!properties.isEmpty())
        {
            s.concat("[" + "\t");

            for (Map.Entry<Writable, Writable> entry : properties.entrySet())
            {
               s += (entry.getKey() + ":" + entry.getValue()) + "\t";
            }

            s.concat("\t" + "]");
        }

        return s;
    }
}
