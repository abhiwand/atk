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

import com.intel.hadoop.graphbuilder.util.HashUtil;
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
 * @param <VidNameType> The type of vertex name.
 */
public class Vertex<VidNameType extends WritableComparable<VidNameType>>
        extends GraphElement implements Writable {

    private VertexID<VidNameType> vertexId;
    private PropertyMap           properties;

    /**
     * Default constructor. Creates a placeholder vertex.
     */
    public Vertex() {
        super();
        vertexId = new VertexID<VidNameType>(null,null);
        properties = new PropertyMap();
    }

    /**
     * Creates a vertex with given vertex name.
     *
     * @param name vertex name
     */
    public Vertex(VidNameType name) {
        super();

        VertexID<VidNameType> vertexId = new VertexID<VidNameType>(name, null);
        this.vertexId = vertexId;
        this.properties = new PropertyMap();
    }

    /**
     * Create a vertex with name, label, and property map.
     *
     * @param name vertex name
     * @param propertyMap a property map
     */
    public Vertex(VidNameType name, StringType label, PropertyMap propertyMap) {
        super();

        VertexID<VidNameType> vertexId = new VertexID<VidNameType>(name, label);
        this.vertexId = vertexId;
        this.properties = propertyMap;
    }

    /**
     * Create a vertex with name, label (as a {@code String}), and property map
     *
     * @param name vertex nam
     * @param label vertex label as a {@code String}
     * @param propertyMap a property map
     *
     */
    public Vertex(VidNameType name, String label, PropertyMap propertyMap) {
        super();

        VertexID<VidNameType> vertexId = new VertexID<VidNameType>(name, new StringType(label));
        this.vertexId = vertexId;
        this.properties = propertyMap;
    }

    /**
     * Create a vertex from a name and String label
     *
     * @param name vertex nam
     * @param label vertex label as a {@code String}
     */
    public Vertex(VidNameType name, String label) {

        this(name, label, new PropertyMap());
    }

    /**
     * Create a vertex from a name and StringType label
     *
     * @param name vertex nam
     * @param label vertex label as a {@code StringType}
     */
    public Vertex(VidNameType name, StringType label) {

        this(name, label, new PropertyMap());
    }

    /**
     * Create a vertex with name, and property map.
     *
     * @param name vertex name
     * @param propertyMap a define property map to set
     */
    public Vertex(VidNameType name, PropertyMap propertyMap) {

        this(name, new StringType(), propertyMap);
    }


    /**
     * This {@code GraphElement} is not an edge.
     * @return  {@code false}
     */
    @Override
    public boolean isEdge() {
        return false;
    }

    /**
     * This {@code GraphElement} is a vertex.
     * @return  {@code true}
     */
    @Override
    public boolean isVertex() {
        return true;
    }

    /**
     * Is this {@code GraphElement} null?
     * @return  {@literal true} if the {@code VertexID} is null
     */
    @Override
    public boolean isNull(){
        if(vertexId == null){
            return true;
        }else{
            return false;
        }
    }

    /**
     * Returns the ID of the vertex.
     * @return The ID of the vertex.
     */
    public VertexID<VidNameType> getId() {
        return vertexId;
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
        return this.vertexId.getLabel();
    }
    /**
     * Overwrites the ID and property map of the vertex.
     * @param vid The new vertex ID.
     * @param properties The new {@code PropertyMap}.
     */
    public void configure(VertexID<VidNameType> vid, PropertyMap properties) {
        this.vertexId = vid;
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
     * set the entire property map
     * @param propertyMap
     */
    public void setProperties(PropertyMap propertyMap){
        this.properties = propertyMap;
    }

     /**
     * Set label of the vertex (RDF label in case of RDF graphs)
      *
      * @param label  the label of the vertex
      */
    public void setLabel(StringType label) {
        this.vertexId.setLabel(label);
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
        if (vertexId == null && properties == null) {
            return "null vertex";
        } else if (vertexId != null && properties == null) {
            return vertexId.toString();
        } else if (vertexId == null && properties != null) {
            return "null vertex with properties (???) " + properties.toString();
        } else {
            return this.vertexId.toString() +  properties.toString();
        }
    }

    /**
     * Reads a vertex from an input stream.
     * @param input The input stream.
     * @throws IOException
     */
    @Override
    public void readFields(DataInput input) throws IOException {
        vertexId.readFields(input);
        this.properties.readFields(input);
    }

    /**
     * Writes a vertex to an output stream.
     * @param output The output stream.
     * @throws IOException
     */
    @Override
    public void write(DataOutput output) throws IOException {
        vertexId.write(output);
        properties.write(output);
    }

    /**
     * To compare another vertex from a serializable graph element
     * @param vertex
     * @return -1 if less than edge, 0 if equal, 1 otherwise
     */
    public int compareTo(Vertex<VidNameType> vertex) {
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
        if (ge instanceof Vertex) {
            Vertex<VidNameType> vertex = (Vertex<VidNameType>) ge;
            return (this.vertexId.equals(vertex.getId()) &&
                    this.properties.equals(vertex.getProperties()));
        } else {
            return false;
        }
    }

    /**
     */
    @Override
    public int hashCode() {
        return HashUtil.hashPair(vertexId, properties);
    }
}
