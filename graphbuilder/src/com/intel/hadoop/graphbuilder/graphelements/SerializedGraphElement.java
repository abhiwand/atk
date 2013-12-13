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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.intel.hadoop.graphbuilder.types.PropertyMap;
import com.intel.hadoop.graphbuilder.types.StringType;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Abstract union type of {@code Vertex} and {@code Edge}. Used as intermediate
 * map output value to hold either a vertex or an edge.
 *
 * @param <VidType>
 */

public abstract class SerializedGraphElement<VidType extends WritableComparable<VidType>>
        implements Writable, WritableComparable<Object> {


    private GraphElement graphElement;

    public SerializedGraphElement() {
        this.graphElement = null;
    }

    protected SerializedGraphElement(GraphElement graphElement) {
        this.graphElement = graphElement;
    }

    public abstract VidType createVid();

    /**
     * Initialize the graphElement.
     *
     * @param graphElement
     */
    public void init(GraphElement graphElement) {

        this.graphElement = graphElement;
    }


    /**
     * @return a vertex or edge property graph element
     */

    public GraphElement graphElement() {
        if (this.graphElement == null)
            return null;
        return this.graphElement.get();
    }

    @Override
    public void readFields(DataInput input) throws IOException {

        boolean isVertex = input.readBoolean();

        if (isVertex) {

            Vertex<VidType> vertex = new Vertex<VidType>();

            VidType vid = null;

            try {
                vid = createVid();
            } catch (Exception e) {
                e.printStackTrace();
            }

            PropertyMap pm = new PropertyMap();

            vertex.configure(vid, pm);
            vertex.readFields(input);

            graphElement = vertex;

        } else {
            try {
                Edge<VidType> edge =  new Edge<VidType>();

                VidType source = createVid();
                VidType target = createVid();

                StringType  label = new StringType();
                PropertyMap pm    = new PropertyMap();

                edge.configure(source, target, label, pm);
                edge.readFields(input);

                graphElement = edge;

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void write(DataOutput output) throws IOException {

        boolean isVertex = graphElement.isVertex();

        output.writeBoolean(isVertex);

        graphElement.write(output);
    }

    @Override
    public int compareTo(Object o) {
        int ret = 1;
        if (this.graphElement.isEdge()) {
            Edge<VidType> in_edge = (Edge<VidType>) o;
            ret = in_edge.compareTo((Edge<VidType>) this.graphElement);
        } else if (this.graphElement.isVertex()) {
            Vertex<VidType> in_vertex = (Vertex<VidType>) o;
            ret = in_vertex.compareTo((Vertex<VidType>) this.graphElement);
        }
        return ret;
    }
}


