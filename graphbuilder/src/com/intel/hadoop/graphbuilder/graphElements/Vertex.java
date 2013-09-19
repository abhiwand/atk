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
package com.intel.hadoop.graphbuilder.graphElements;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import com.intel.hadoop.graphbuilder.types.PropertyMapType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Represents a vertex object with id and vertex data.
 * 
 * @param <VertexIdType>
 *          the type of vertex id.
 */
public class Vertex<VertexIdType extends WritableComparable<VertexIdType>> implements Writable {


  private VertexIdType vertexId;
  private PropertyMapType properties;

  /** Default constructor. Creates an empty vertex. */
  public Vertex() {
      this.properties = new PropertyMapType();
  }

  /**
   * Creates a vertex with given vertex id and vertex data.
   * 
   * @param vid
   */
  public Vertex(VertexIdType vid) {
    this.vertexId = vid;
    this.properties = new PropertyMapType();
  }

  /** Returns the id of the vertex. */
  public VertexIdType getVertexId() {
    return vertexId;
  }
  /** overrides the id of the vertex. used when id not specified on creation */
  public void configure(VertexIdType vid, PropertyMapType properties)
  {
    this.vertexId = vid;
    this.properties = properties;
  }

  /** Returns a property of the vertex. */
  public Object getProperty(String key) {
      return properties.getProperty(key);
  }

  public Vertex<VertexIdType> setProperty(String key, Writable val)
  {
    this.properties.setProperty(key, val);
    return this;
  }

    /**
     * @return the property map
     */
    public PropertyMapType getProperties()
    {
        return properties;
    }

  @Override
  public final boolean equals(Object obj) {
    if (obj instanceof Vertex) {
      Vertex other = (Vertex) obj;
      return (vertexId.equals(other.vertexId) && properties.equals(other.properties));
    }
    return false;
  }

  @Override
  public final int hashCode() {
    return vertexId.hashCode();
  }

  @Override
  public final String toString() {
    return vertexId.toString() + "\t" + properties.toString();
  }

    @Override
    public void readFields(DataInput input) throws IOException {
        vertexId.readFields(input);
        this.properties.readFields(input);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        vertexId.write(output);
        properties.write(output);
    }
}
