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

public abstract class PropertyGraphElement<VidType extends WritableComparable<VidType>>
        implements Writable {

    public enum GraphElementType {
        NULL_ELEMENT,
        VERTEX,
        EDGE
    }

    private GraphElementType graphElementType;
    private Vertex           vertex;
    private Edge             edge;

    public PropertyGraphElement() {
        vertex = new Vertex<VidType>();
        edge   = new Edge<VidType>();
    }

    public abstract VidType createVid();

    /**
     * Initialize the value.
     *
     * @param graphElementType
     * @param value
     */
    public void init(GraphElementType graphElementType, Object value) {

        this.graphElementType = graphElementType;

        if (graphElementType == GraphElementType.VERTEX) {
            this.vertex = (Vertex<VidType>) value;
            this.edge = null;
        } else {
            this.edge = (Edge<VidType>) value;
            this.vertex = null;
        }
    }

    /**
     * @return the type graphElementType of the value.
     */

    public GraphElementType graphElementType() {
        return graphElementType;
    }

    /**
     * @return the vertex value, used only when graphElementType == VERTEX.
     */

    public Vertex<VidType> vertex() {
        return vertex;
    }

    /**
     * @return the vertex value, used only when graphElementType == EDGE.
     */

    public Edge<VidType> edge() {
        return edge;
    }

    @Override
    public void readFields(DataInput input) throws IOException {

        graphElementType = input.readBoolean() ? GraphElementType.EDGE : GraphElementType.VERTEX;

        if (graphElementType == GraphElementType.VERTEX) {

            VidType vid = null;

            try {
                vid = createVid();
            } catch (Exception e) {
                e.printStackTrace();
            }

            PropertyMap pm = new PropertyMap();

            vertex.configure(vid, pm);
            vertex.readFields(input);

        } else {
            try {

                VidType source = createVid();
                VidType target = createVid();

                StringType      label = new StringType();
                PropertyMap pm    = new PropertyMap();

                edge.configure(source, target, label, pm);
                edge.readFields(input);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void write(DataOutput output) throws IOException {

        boolean typeBit = graphElementType == GraphElementType.EDGE ? true : false;

        output.writeBoolean(typeBit);

        if (graphElementType == GraphElementType.VERTEX) {
            vertex.write(output);
        } else {
            edge.write(output);
        }
    }

    @Override
    public String toString() {
        switch (graphElementType) {
            case VERTEX:
                return vertex.toString();
            case EDGE:
                return edge.toString();
            default:
                return new String("null graph element");
        }
    }
}

