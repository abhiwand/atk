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


package com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema;

import com.intel.hadoop.graphbuilder.graphelements.GraphElement;
import com.intel.hadoop.graphbuilder.types.EncapsulatedObject;
import com.intel.hadoop.graphbuilder.types.StringType;
import com.intel.hadoop.graphbuilder.util.HashUtil;
import org.apache.hadoop.io.Writable;
import org.apache.http.annotation.NotThreadSafe;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * A schema element encapsulates the type information associated with vertices or edges of a particular label.
 * Such information is required when loading Titan.
 */
@NotThreadSafe
public class SchemaElement implements Writable {

    private static final String EDGE   = "EDGE";
    private static final String VERTEX = "VERTEX";

    private HashSet<PropertySchema> propertySchemata;

    private StringType ID = null;
    private StringType label = null;
    private boolean    isEdge = false;

    private static enum Type  { EDGE, VERTEX }

    /**
     * Default constructor, necessary for Hadoop.
     */

    public SchemaElement() {
        ID =  new StringType();
        label = new StringType();

        propertySchemata = new HashSet<PropertySchema>();
    }

    /*
     * private constructor for the factories
     */

    private SchemaElement(String label, Type type) {
        propertySchemata = new HashSet<PropertySchema>();

        this.label =  (label == null) ? null : new StringType(label);

        isEdge = (type == Type.EDGE);

        if (isEdge) {
            ID = new StringType(EDGE + "." + this.getLabel());
        } else {
            ID = new StringType(VERTEX + "." + this.getLabel());
        }
    }

    /**
     * Takes a property graph element and constructs its schema.
     * @param graphElement A property graph element.
     */
    public SchemaElement(GraphElement graphElement) {

        propertySchemata = new HashSet<PropertySchema>();
        label = graphElement.getLabel();

        isEdge = graphElement.isEdge();

        if (isEdge) {
            ID = new StringType(EDGE + "." + this.getLabel());
        } else {
            ID = new StringType(VERTEX + "." + this.getLabel());
        }

        for (Writable key : graphElement.getProperties().getPropertyKeys()) {
            PropertySchema propertySchema = new PropertySchema();

            propertySchema.setName(key.toString());

            Object v = graphElement.getProperty(key.toString());
            Class<?> dataType = ((EncapsulatedObject) v).getBaseType();
            propertySchema.setType(dataType);

            propertySchemata.add(propertySchema);
        }

    }

    /**
     * Factory that creates a new vertex schema from a label.
     * @param label The label for the new schema.
     * @return A new <schema>SchemaElement</schema> encapuslating a vertex schema with the given label.
     */
    public static SchemaElement createVertexSchemaElement(String label) {
        return new SchemaElement(label, Type.VERTEX);
    }

    /**
     * Factory that creates a new edge schema from a label.
     * @param label The label for the new schema.
     * @return A new <schema>SchemaElement</schema> encapuslating a vertex schema with the given label.
     */
    public static SchemaElement createEdgeSchemaElement(String label) {
        return new SchemaElement(label, Type.EDGE);
    }

    /**
     *
     * @return The label of the <code>SchemaElement</code>
     */
    public String getLabel() {
        return (label == null) ? null : label.get();
    }

    /**
     *
     * @return The ID of the <code>SchemaElement</code>
     */
    public String getID() {
        return ID.get();
    }

    /**
     * @return True if the <code>SchemaElement</code> is an edge schema.
     */
    public boolean isEdge() {
        return isEdge;
    }

    /**
     * Adds a property schema to the <code>SchemaElement</code>
     * @param schema A <code>PropertySchema</code> to be attached to the <code>SchemaElement</code>.
     */
    public void addPropertySchema(PropertySchema schema) {
        this.propertySchemata.add(schema);
    }

    /**
     * Takes a set of <code>PropertySchema</code>  and adds them all to the set of <code>PropertySchema</code>'s
     * attached to this <code>SchemaElement</code>
     * @param propertySchemata Incoming set of property schemas.
     */
    public void unionPropertySchemata(Set<PropertySchema> propertySchemata) {
        this.propertySchemata.addAll(propertySchemata);
    }

    /**
     * @return  The set of property schemata attached to this <code>SchemaElement</code>
     */
    public HashSet<PropertySchema> getPropertySchemata() {
        return propertySchemata;
    }


    /**
     * Reads an {@code EdgeSchema} from an input stream.
     *
     * @param input The input stream.
     * @throws java.io.IOException
     */
    @Override
    public void readFields(DataInput input) throws IOException {

        ID.readFields(input);

        boolean hasLabel = input.readBoolean();

        if (hasLabel) {
            label.readFields(input);
        }

        isEdge = input.readBoolean();

        propertySchemata.clear();
        int schemataSize = input.readInt();
        for (int i = 0; i < schemataSize; i++) {
            PropertySchema propertySchema = new PropertySchema();
            propertySchema.readFields(input);
            propertySchemata.add(propertySchema);
        }
    }

    /**
     * Writes an {@code EdgeSchema} to an output stream.
     *
     * @param output The output stream.
     * @throws IOException
     */
    @Override
    public void write(DataOutput output) throws IOException {

        ID.write(output);

        if (label == null) {
            output.writeBoolean(false);
        } else {
            output.writeBoolean(true);
            label.write(output);
        }

        output.writeBoolean(isEdge);

        output.writeInt(propertySchemata.size());
        for (PropertySchema propertySchema : propertySchemata) {
            propertySchema.write(output);
        }
    }


    /**
     * Equality function.
     *
     * @param in Object for comparison.
     * @return {@code true} if and only if the other object is an {@code EdgeSchema} whose label and set of
     *         {@code PropertySchema}'s   are all equal to the label and set of {@code PropertySchema}'s of this object.
     */
    @Override
    public boolean equals(Object in) {


        if (in instanceof SchemaElement) {
            SchemaElement inSchemaElement = (SchemaElement) in;

            if (this.getLabel() != null) {
                return (this.getLabel().equals(inSchemaElement.getLabel())
                        && this.getPropertySchemata().size() == (inSchemaElement.getPropertySchemata().size())
                        && (((SchemaElement) in).getPropertySchemata()).containsAll(this.getPropertySchemata()));
            } else {
                return (this.getLabel() == null && inSchemaElement.getLabel() == null);
            }
        } else {
            return false;
        }

    }

    /**
     * Hashcode function.
     *
     * @return integer hashcode
     * @see {@code equals}
     */
    @Override
    public int hashCode() {

        int hash = (label == null) ? 0xabbadaba : label.hashCode();

        for (PropertySchema propertySchema : this.getPropertySchemata()) {
            hash = HashUtil.combine(hash, propertySchema);
        }

        return hash;
    }

    /**
     * @return Same value as <code>getID</code>
     */
    @Override
    public String toString() {
        StringBuffer buffer = new StringBuffer();

        buffer.append("SCHEMA ID = " + this.getID() + " .");
        buffer.append("Property Schemata: ");
        for (PropertySchema pSchema : this.propertySchemata) {
            buffer.append(pSchema.getName());
            buffer.append(" ");
        }
        return buffer.toString();
    }
}
