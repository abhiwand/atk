/* Copyright (C) 2013 Intel Corporation.
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
 * The abstract union type of {@code Vertex} and {@code Edge}. Used as an intermediate
 * map output value to hold either a vertex or an edge.
 *
 * <p> This type is abstract only because we need a constructor for {@code VidType}.</p>
 *
 * @param {@code <VidType>}  The vertex ID type.
 */

public abstract class PropertyGraphElement<VidType extends WritableComparable<VidType>>
        implements Writable {



    /**
     * Abstract method for the {@code VidType} constructor.
     * @return  A new {@code VidType} object.
     */
    public abstract VidType createVid();

    /**
     * A flag to communicate if the property graph element in question is a vertex, an edge, or unassigned.
     */
    public enum GraphElementType {
        NULL_ELEMENT,
        VERTEX,
        EDGE
    }

    private GraphElementType graphElementType;
    private Vertex           vertex;
    private Edge             edge;

    /**
     * A default constructor that allocates both the edge and vertex fields, and flags the element as {@code NULL_ELEMENT}.
     */
    public PropertyGraphElement() {
        graphElementType = GraphElementType.NULL_ELEMENT;
        vertex = new Vertex<VidType>();
        edge   = new Edge<VidType>();
    }


    /**
     * Initializes the {@code graphElementType} value.
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
     * @return The type graphElementType of the value.
     */

    public GraphElementType graphElementType() {
        return graphElementType;
    }

    /**
     * @return The vertex value, used only when the graphElementType == VERTEX.
     */

    public Vertex<VidType> vertex() {
        return vertex;
    }

    /**
     * @return The vertex value, used only when the graphElementType == EDGE.
     */

    public Edge<VidType> edge() {
        return edge;
    }

    /**
     * Reads a property graph element from an input stream.
     * @param input The input stream.
     * @throws IOException
     */
    @Override
    public void readFields(DataInput input) throws IOException {

        graphElementType = input.readBoolean() ? GraphElementType.EDGE : GraphElementType.VERTEX;

        if (graphElementType == GraphElementType.VERTEX) {

            VidType vid = null;

            vid = createVid();

            PropertyMap pm = new PropertyMap();

            vertex.configure(vid, pm);

            try {
                vertex.readFields(input);
            } catch (IOException e) {
                throw e;
            }
        } else {

                VidType source = createVid();
                VidType target = createVid();

                StringType  label = new StringType();
                PropertyMap pm    = new PropertyMap();

                edge.configure(source, target, label, pm);

                try {
                    edge.readFields(input);
                } catch (IOException e) {
                    throw e;
                }
        }
    }

    /**
     * Writes a property graph element to an output stream.
     * @param output The output stream.
     * @throws IOException
     */
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

    /**
     * Converts a property graph element to a string.
     * @return
     */
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


