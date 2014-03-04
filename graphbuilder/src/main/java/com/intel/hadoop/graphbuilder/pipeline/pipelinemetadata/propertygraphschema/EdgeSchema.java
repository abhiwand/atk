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

import com.intel.hadoop.graphbuilder.types.StringType;
import com.intel.hadoop.graphbuilder.util.HashUtil;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;

/**
 * The type of an edge declaration used in graph construction. It represents all of the
 * the names and datatypes of the properties that can be associated with edges of a given label.
 * <p/>
 * <p/>
 * The expected use of this information is declaring keys for loading the constructed
 * graph into a graph database.
 */
public class EdgeSchema extends EdgeOrPropertySchema implements Writable {

    private HashSet<PropertySchema> propertySchemata;
    private String label;

    private PropertySchemaArrayWritable serializedPropertySchemata = new PropertySchemaArrayWritable();
    private StringType serializedLabel = new StringType();

    /**
     * Constructs the {@code EdgeSchema}.
     */
    public EdgeSchema() {
        this.label = null;
        propertySchemata = new HashSet<>();
    }

    /**
     * Constructs the {@code EdgeSchema} from a given label.
     *
     * @param label Edge label for the new {@code EdgeSchema}.
     */
    public EdgeSchema(String label) {
        this.label = label;
        propertySchemata = new HashSet<>();
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
        boolean test = ((in instanceof EdgeSchema)
                && this.label.equals(((EdgeSchema) in).getLabel())
                && this.getPropertySchemata().size() == (((EdgeSchema) in).getPropertySchemata().size()));

        if (!test) {
            return false;
        } else {
            HashSet<PropertySchema> inPropertySchemata = ((EdgeSchema) in).getPropertySchemata();

            return inPropertySchemata.containsAll(this.getPropertySchemata());
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
        int hash = label.hashCode();

        for (PropertySchema propertySchema : this.getPropertySchemata()) {
            hash = HashUtil.combine(hash, propertySchema);
        }

        return hash;
    }

    /**
     * Sets the edge label of the {@code EdgeSchema}.
     *
     * @param label Incoming edge label.
     */
    public void setLabel(String label) {
        this.label = label;
    }

    /**
     * @return The edge label of the {@code EdgeSchema}
     */
    public String getLabel() {
        return this.label;
    }

    /**
     * @return The set of {@code PropertySchema} belonging to this {@code EdgeSchema}
     */
    public HashSet<PropertySchema> getPropertySchemata() {
        return propertySchemata;
    }

    /**
     * Add a {@code PropertySchema} to this {@code EdgeSchema}.
     *
     * @param propertySchema Incominng property schema.
     */
    public void addPropertySchema(PropertySchema propertySchema) {
        propertySchemata.add(propertySchema);
    }

    /**
     * Reads an {@code EdgeSchema} from an input stream.
     *
     * @param input The input stream.
     * @throws java.io.IOException
     */
    @Override
    public void readFields(DataInput input) throws IOException {

        serializedPropertySchemata.readFields(input);
        serializedLabel.readFields(input);

        propertySchemata.clear();
        PropertySchema[] values = (PropertySchema[]) serializedPropertySchemata.toArray();

        Collections.addAll(propertySchemata, values);
        label = serializedLabel.get();
    }

    /**
     * Writes an {@code EdgeSchema} to an output stream.
     *
     * @param output The output stream.
     * @throws IOException
     */
    @Override
    public void write(DataOutput output) throws IOException {

        PropertySchema[] array = new PropertySchema[propertySchemata.size()];
        propertySchemata.toArray(array);

        serializedPropertySchemata.set(array);

        serializedPropertySchemata.write(output);

        serializedLabel.set(label);
        serializedLabel.write(output);
    }
}
