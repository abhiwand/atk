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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * Represents an Edge object with a source, destination, label and a  (possibly empty) property map.
 * <p>
 * This class is mutable. See the {@code configure} and {@code setProperty} methods.
 * </p>
 *
 * @param <VidType> the type of vertex id.
 */
public class Edge<VidType extends WritableComparable<VidType>>  extends PropertyGraphElement implements Writable {

    private VidType     src;
    private VidType     dst;
    private StringType  label;
    private PropertyMap properties;

    /**
     * Create a placeholder edge.
     */

    public Edge() {
        this.properties = new PropertyMap();
    }

    /**
     * Creates an edge with given source, destination and label..
     *
     * @param src vertex ID of the edge's source vertex
     * @param dst vertex ID of the edge's destination vertex
     * @param label the edge label
     */
    public Edge(VidType src, VidType dst, StringType label) {
        this.src = src;
        this.dst = dst;
        this.label = label;
        this.properties = new PropertyMap();
    }

    /**
     * This is an edge.
     * @return  {@code true}
     */
    @Override
    public boolean isEdge() {
        return true;
    }

    /**
     * This is not a vertex.
     * @return  {@code false}
     */
    @Override
    public boolean isVertex() {
        return false;
    }

    /**
     *  Overwrite an edge's fields with the given parameters.
     *  @param src vertex ID of the edge's source vertex
     *  @param dst vertex ID of the edge's destination vertex
     *  @param properties the edge's property map
     */
    public void configure(VidType src, VidType dst, StringType label, PropertyMap properties) {
        this.src        = src;
        this.dst        = dst;
        this.label      = label;
        this.properties = properties;
    }

    /**
     * Get a property from the edge's property map.
     * @param key lookup key for the value in the edge's property map
     */
    public Object getProperty(String key) {
        return properties.getProperty(key);
    }

    /**
     * Set an edge property.
     *
     * This changes the property map of the edge
     *
     * @param key lookup key for the value in the edge's property map
     * @param val value to be put in the property map
     */
    public void setProperty(String key, Writable val) {
        properties.setProperty(key, val);
    }

    /**
     * @return the edge label.
     */
    public StringType getEdgeLabel() {
        return label;
    }

    /**
     * @return The vertex ID of the edge's source
     */
    public VidType getSrc() {
        return src;
    }

    /**
     * @return The vertex ID of the edge's destination
     */
    public VidType getDst() {
        return dst;
    }

    /**
     * Determine if the edge is a loop - that is, if its source and destination are the same vertex.
     * @return true iff the edge's source and destination are equal
     */
    public boolean isSelfEdge() {
        return Objects.equals(src, dst);
    }

    /**
     * @return the edge's property map
     */
    public PropertyMap getProperties() {
        return properties;
    }

    /**
     * Get the edge's ID - that is,  the triple of its source vertex ID, destination vertex ID and its label
     * @return  the triple of the edge's source vertex ID, destination vertex ID and its label
     */
    public EdgeID getEdgeID() {
        return new EdgeID(this.src, this.dst, this.label);
    }

    /**
     * Convert edge into  string for printing. Properties are tab separated.
     * @return   string form of the edge
     */
    @Override
    public final String toString() {
        return src.toString() + "\t" + dst.toString() + "\t"
                + label.toString() + "\t" + properties.toString();
    }

    /**
     * Read an edge from an input stream
     * @param input the input stream
     * @throws IOException
     */
    @Override
    public void readFields(DataInput input) throws IOException {
        src.readFields(input);
        dst.readFields(input);
        label.readFields(input);
        properties.readFields(input);
    }

    /**
     * Write an edge to an output stream
     * @param output the output stream
     * @throws IOException
     */
    @Override
    public void write(DataOutput output) throws IOException {
        src.write(output);
        dst.write(output);
        label.write(output);
        properties.write(output);
    }
}
