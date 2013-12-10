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
 *  * <p>
 * This class is mutable. See the {@code configure} and {@code setProperty} methods.
 * </p>
 *
 * @param <VertexIdType> the type of vertex id.
 */
public class Vertex<VertexIdType extends WritableComparable<VertexIdType>>
        extends GraphElement implements Writable {

    private VertexIdType id;
    private PropertyMap  properties;
    private StringType label;

    /**
     * Default constructor. Creates an placeholder vertex.
     */
    public Vertex() {
        super();

        this.label = new StringType();
        this.properties = new PropertyMap();
    }

    /**
     * Create a vertex with an ID.
     *
     * @param vid vertex ID
     */
    public Vertex(VertexIdType vid) {

        this(vid, new StringType(), new PropertyMap());
    }

    /**
     * Create a vertex with ID, label, and property map
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
     * Create a vertex with an ID.
     *
     * @param vid vertex ID
     * @param label vertex Label
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
     * Return the ID of the vertex.
     * @return the ID of the vertex
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
 * Return the label of the vertex.
     * @return the label of the vertex
     */
    public StringType getLabel() {
        return this.label;
    }
    /**
     * Overwrite the ID and property map of the vertex.
     * @param vid new vertex ID
     * @param properties new {@code PropertyMap}
     */
    public void configure(VertexIdType vid, PropertyMap properties) {
        this.id = vid;
        this.properties = properties;
    }

    /**
     * Return a property of the vertex.
     * @param key lookup key for the property
     * @return the value of the property
     */
    public Object getProperty(String key) {
        return properties.getProperty(key);
    }

    /**
     * Set a property of the vertex
     * @param key  the key of the property being updated
     * @param val  the new value for the property
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
     * Set label of the vertex (RDF label in case of RDF graphs)
      * @param label  the label of the vertex
      */
    public void setLabel(StringType label) {
        this.label = label;
    }

    /**
     * Get the property map for the vertex.
     * @return the property map
     */
    public PropertyMap getProperties() {
        return properties;
    }

    /**
     * Convert the vertex to a string.
     * @return  a string representation of the vertex
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
     * Read a vertex from an input stream
     * @param input the input stream
     * @throws IOException
     */
    @Override
    public void readFields(DataInput input) throws IOException {
        id.readFields(input);
        label.readFields(input);
        this.properties.readFields(input);
    }

    /**
     * Write a vertex to an output stream
     * @param output the output stream
     * @throws IOException
     */
    @Override
    public void write(DataOutput output) throws IOException {
        id.write(output);
        label.write(output);
        properties.write(output);
    }
}
