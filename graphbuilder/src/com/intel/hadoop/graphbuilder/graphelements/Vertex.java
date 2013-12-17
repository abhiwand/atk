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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import com.intel.hadoop.graphbuilder.types.PropertyMap;
import com.intel.hadoop.graphbuilder.types.StringType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Represents a vertex object with a vertex ID and a (potentially empty) property map.
 *
 * <p>
 * This class is mutable. See the {@code configure} and {@code setProperty} methods.
 * </p>
 *
 * @param <VertexIdType> The type of the vertex ID.
 */
public class Vertex<VertexIdType extends WritableComparable<VertexIdType>>
        extends GraphElement implements Writable {

    private VertexIdType id;
    private PropertyMap  properties;
    private StringType label;

    /**
     * The default vertex constructor. Creates a placeholder vertex.
     */
    public Vertex() {
        super();

        this.label = new StringType();
        this.properties = new PropertyMap();
    }

    /**
     * Creates a vertex with given vertex ID.
     *
     * @param vid The vertex ID.
     */
    public Vertex(VertexIdType vid) {

        this(vid, new StringType(), new PropertyMap());
    }

    /**
     * Create a vertex with ID, label, and property map.
     *
     * @param vid vertex ID
     */
    public Vertex(VertexIdType vid, StringType label, PropertyMap propertyMap) {
        this();

        this.id = vid;
        this.properties = propertyMap;
        this.label = label;
    }

    /**
     * Create a vertex with ID, label, and property map
     *
     * @param vid vertex ID
     */
    public Vertex(VertexIdType vid, String label, PropertyMap propertyMap) {
        this();

        this.id = vid;
        this.properties = propertyMap;
        this.label = new StringType(label);
    }

    /**
     * Creates a vertex with the given vertex ID.
     *
     * @param vid The vertex ID.
     * @param label The vertex Label.
     */
    public Vertex(VertexIdType vid, String label) {

        this(vid, label, new PropertyMap());
    }

    /**
     * Create a vertex with Id, and property map.
     *
     * @param vid vertex ID
     * @param propertyMap a define property map to set
     */
    public Vertex(VertexIdType vid, PropertyMap propertyMap) {

        this(vid, new StringType(), propertyMap);
    }


    /**
     * This is not an edge.
     * @return  {@code false}
     */
    @Override
    public boolean isEdge() {
        return false;
    }

    /**
     * This is not a vertex.
     * @return  {@code true}
     */
    @Override
    public boolean isVertex() {
        return true;
    }

    @Override
    public boolean isNull(){
        if(id == null){
            return true;
        }else{
            return false;
        }
    }

    /**
     * Returns the ID of the vertex.
     * @return The ID of the vertex.
     */
    public VertexIdType getId() {
        return id;
    }

    /**
     * @return get the graph element
     */
    public Vertex get(){
        return this;
    }

    /**
     * Returns the label of the vertex.
     * @return The label of the vertex.
     */
    public StringType getLabel() {
        return this.label;
    }
    /**
     * Overwrites the ID and property map of the vertex.
     * @param vid The new vertex ID.
     * @param properties The new {@code PropertyMap}.
     */
    public void configure(VertexIdType vid, PropertyMap properties) {
        this.id = vid;
        this.properties = properties;
    }

    /**
     * Returns a property of the vertex.
     * @param key  The lookup key for the property.
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
     * set the entire property map
     * @param propertyMap
     */
    public void setProperties(PropertyMap propertyMap){
        this.properties = propertyMap;
    }

     /**
     * Sets the label of the vertex (the RDF label in the case of RDF graphs).
      * @param label  The label of the vertex, or the RDF label if an RDF graph.
      */
    public void setLabel(StringType label) {
        this.label = label;
    }

    /**
     * Gets the property map for the vertex.
     * @return The vertex property map.
     */
    public PropertyMap getProperties() {
        return properties;
    }

    /**
     * Converts the vertex to a string.
     * @return  A string representation of the vertex.
     */
    @Override
    public final String toString() {
        if (this.label == null || this.label.isEmpty()) {
            return this.id.toString() + "\t" + this.properties.toString();
        } else {
            return this.id.toString() + "\t" + this.label.toString() + "\t" + properties.toString();
        }
    }

    /**
     * Reads a vertex from an input stream.
     * @param input The input stream.
     * @throws IOException
     */
    @Override
    public void readFields(DataInput input) throws IOException {
        id.readFields(input);
        label.readFields(input);
        this.properties.readFields(input);
    }

    /**
     * Writes a vertex to an output stream.
     * @param output The output stream.
     * @throws IOException
     */
    @Override
    public void write(DataOutput output) throws IOException {
        id.write(output);
        label.write(output);
        properties.write(output);
    }

    /**
     * To compare another vertex from a serializable graph element
     * @param vertex
     * @return -1 if less than edge, 0 if equal, 1 otherwise
     */
    public int compareTo(Vertex<VertexIdType> vertex) {
        return equals(vertex) ? 0 : 1;
    }

    /**
     * Checks if the input vertex is equal to passed vertex
     * This is a deep check which means source, destination
     * vertex ID's and all properties are checked to decide
     * equality
     * @param ge
     */
    @Override
    public boolean equals(GraphElement ge) {
        Vertex<VertexIdType> vertex = (Vertex<VertexIdType>) ge;
        return (this.id.equals(vertex.getId()) && this.label.equals(vertex.getLabel()) &&
                this.properties.equals(vertex.getProperties()));
    }
}
