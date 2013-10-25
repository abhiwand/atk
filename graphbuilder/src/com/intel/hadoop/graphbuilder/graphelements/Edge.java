/* Copyright (C) 2012 Intel Corporation.
 *     All rights reserved.
 *           
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 * For more about this software visit:
 *      http://www.01.org/GraphBuilder 
 */
package com.intel.hadoop.graphbuilder.graphelements;

import com.intel.hadoop.graphbuilder.types.PropertyMap;
import com.intel.hadoop.graphbuilder.types.StringType;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.intel.hadoop.graphbuilder.util.HashUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * Represents an Edge object with getSrc, getDst vertex id and edge data.
 * 
 * @param <VidType>
 *          the type of vertex id.
 */

public class Edge<VidType extends WritableComparable<VidType>> implements Writable{

    private VidType     src;
    private VidType     dst;
    private StringType  label;
    private PropertyMap properties;

  /**
   *  Default constructor. Creates an empty edge.
   *
   */

    public Edge() {
        this.properties = new PropertyMap();
    }

    /**
    * Creates an edge with given getSrc, getDst and edge data.
    *
    * @param src
    * @param dst
    * @param label
    */

    public Edge(VidType src, VidType dst, StringType label) {
        this.src = src;
        this.dst = dst;
        this.label = label;
        this.properties = new PropertyMap();
    }

    public void configure(VidType src, VidType dst, StringType label, PropertyMap properties)
    {
        this.src = src;
        this.dst = dst;
        this.label = label;
        this.properties = properties;
    }

    /**
     * get an edge property
     */
    public Object getProperty(String key)
    {
        return properties.getProperty(key);
    }

    /**
     * set an edge property
     */
    public Edge setProperty(String key, Writable val)
    {
        properties.setProperty(key, val);
        return this;
    }
    /**
     * @return the edge label.
     */
    public StringType getEdgeLabel() {
        return label;
    }

    /**
    * @return getSrc vertex id.
    */
    public VidType getSrc() {
        return src;
    }

    /**
    * @return getDst vertex id.
    */
    public VidType getDst() {
        return dst;
    }

    /**
     * @return boolean : is this edge a self-edge? (loop).
     */
    public boolean isSelfEdge() {
        return Objects.equals(src, dst);
    }
    /**
     * @return the property map
     */
    public PropertyMap getProperties()
    {
        return properties;
    }

    @Override
    public final boolean equals(Object obj) {
        if (obj instanceof Edge) {
          Edge other = (Edge) obj;
          return (src.equals(other.src)
                  && dst.equals(other.dst)
                  && label.equals(other.label)
                  && properties.equals(other.properties));
        } else {
          return false;
        }
    }

      @Override
      public final int hashCode() {
        return HashUtil.hashTriple(src, dst, label);
      }

      @Override
      public final String toString() {
        return src.toString() + "\t" + dst.toString() + "\t"
               + label.toString() + "\t" + properties.toString();
      }



    @Override
    public void readFields(DataInput input) throws IOException {
        src.readFields(input);
        dst.readFields(input);
        label.readFields(input);
        properties.readFields(input);
    }

    @Override
    public void write(DataOutput output) throws IOException {
            src.write(output);
            dst.write(output);
            label.write(output);
            properties.write(output);
    }
}
