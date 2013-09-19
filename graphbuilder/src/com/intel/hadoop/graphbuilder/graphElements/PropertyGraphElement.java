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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.intel.hadoop.graphbuilder.types.PropertyMapType;
import com.intel.hadoop.graphbuilder.types.StringType;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Abstract union type of {@code Vertex} and {@code Edge}. Used as intermediate
 * map output value to hold either a vertex or an edge.
 * 
 * @param <VidType>
 */
public abstract class PropertyGraphElement<VidType extends WritableComparable<VidType>>
    implements Writable {

  public static final boolean VERTEXVAL = false;
  public static final boolean EDGEVAL = true;;

  public abstract VidType createVid();

  /**
   * Creates an empty value.
   */
  public PropertyGraphElement() {
    vertex = new Vertex<VidType>();
    edge = new Edge<VidType>();
  }

  /**
   * Initialize the value.
   * 
   * @param flag
   * @param value
   */
  public void init(boolean flag, Object value) {
    this.flag = flag;
    if (flag == VERTEXVAL) {
      this.vertex = (Vertex<VidType>) value;
      this.edge = null;
    } else {
      this.edge = (Edge<VidType>) value;
      this.vertex = null;
    }
  }

  /**
   * @return the type flag of the value.
   */
  public boolean flag() {
    return flag;
  }

  /**
   * @return the vertex value, used only when flag == VERTEXVAL.
   */
  public Vertex<VidType> vertex() {
    return vertex;
  }

  /**
   * @return the vertex value, used only when flag == EDGEXVAL.
   */
  public Edge<VidType> edge() {
    return edge;
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    flag = input.readBoolean();

    if (flag == VERTEXVAL) {
      VidType vid        = createVid();
      PropertyMapType pm = new PropertyMapType();
      vertex.configure(vid, pm);

      vertex.readFields(input);
    } else {
      VidType source = createVid();
      VidType target = createVid();
      StringType label = new StringType();
      PropertyMapType pm = new PropertyMapType();
      edge.configure(source, target, label, pm);
      edge.readFields(input);
    }
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeBoolean(flag);
    if (flag == VERTEXVAL) {
      vertex.write(output);
    } else {
      edge.write(output);
    }
  }

  @Override
  public String toString() {
    if (flag == VERTEXVAL)
      return vertex.toString();
    return edge.toString();
  }

  private boolean flag;
  private Vertex<VidType> vertex;
  private Edge<VidType> edge;
}
