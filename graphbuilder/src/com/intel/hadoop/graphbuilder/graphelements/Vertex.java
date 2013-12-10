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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import com.intel.hadoop.graphbuilder.types.PropertyMap;
import com.intel.hadoop.graphbuilder.types.StringType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * Represents a vertex object with a vertex ID and a (potentially empty) property map.
 *
 *  * <p>
 * This class is mutable. See the {@code configure} and {@code setProperty} methods.
 * </p>
 *
 * @param <VertexIdType> The type of vertex id.
 */
public class Vertex<VertexIdType extends WritableComparable<VertexIdType>> implements Writable {

    private VertexIdType vertexId;
    private PropertyMap  properties;
    private StringType   vertexLabel;

    /**
     * Default constructor. Creates a placeholder vertex.
     */
    public Vertex() {
        this.properties  = new PropertyMap();
        this.vertexLabel = new StringType();
    }

    /**
     * Creates a vertex with given vertex ID.
     *
     * @param vid Vertex ID.
     */
    public Vertex(VertexIdType vid) {
        this.vertexId    = vid;
        this.properties  = new PropertyMap();
        this.vertexLabel = new StringType();
    }

    /**
     * Create a vertex with given vertex ID.
     *
     * @param vid vertex ID
     * @param label vertex Label
     */
    public Vertex(VertexIdType vid, String label) {
        this.vertexId    = vid;
        this.properties  = new PropertyMap();
        this.vertexLabel = new StringType(label);
    }

    /**
     * Returns the ID of the vertex.
     * @return The ID of the vertex.
     */
    public VertexIdType getVertexId() {
        return vertexId;
    }

    /**
 * Return the label of the vertex.
     * @return the label of the vertex
     */
    public StringType getVertexLabel() {
        return this.vertexLabel;
    }
    /**
     * Overwrites the ID and property map of the vertex.
     * @param vid The new vertex ID.
     * @param properties The new {@code PropertyMap}.
     */
    public void configure(VertexIdType vid, PropertyMap properties) {
        this.vertexId   = vid;
        this.properties = properties;
    }

    /**
     * Returns a property of the vertex.
     * @param key The lookup key for the property.
     * @return The value of the property.
     */
    public Object getProperty(String key) {
        return properties.getProperty(key);
    }

    /**
     * Set a property of the vertex.
     * @param key  The key of the property being updated.
     * @param val  The new value for the property.
     */
    public void setProperty(String key, Writable val) {
        this.properties.setProperty(key, val);
    }

     /**
     * Set label of the vertex (RDF label in case of RDF graphs)
      * @param label  the label of the vertex
      */
    public void setVertexLabel(StringType label) {
        this.vertexLabel = label;
    }

    /**
     * Gets the property map for the vertex.
     * @return The property map.
     */
    public PropertyMap getProperties() {
        return properties;
    }

    /**
     * Converts the vertex to a string.
     * @return  AA string representation of the vertex.
     */
    @Override
    public final String toString() {
        if (this.vertexLabel.isEmpty()) {
            return this.vertexId.toString() + "\t" + this.properties.toString();
        } else {
            return this.vertexId.toString() + "\t" + this.vertexLabel.toString() + "\t" + properties.toString();
        }
    }

    /**
     * Reads a vertex from an input stream.
     * @param input The input stream.
     * @throws IOException.
     */
    @Override
    public void readFields(DataInput input) throws IOException {
        vertexId.readFields(input);
        vertexLabel.readFields(input);
        this.properties.readFields(input);
    }

    /**
     * Writes a vertex to an output stream.
     * @param output The output stream.
     * @throws IOException.
     */
    @Override
    public void write(DataOutput output) throws IOException {
        vertexId.write(output);
        vertexLabel.write(output);
        properties.write(output);
    }
}
