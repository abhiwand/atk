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

/**
 * Encapsulates the name and datatype of a property label.
 * <p>The expected use of this information is declaring keys for loading the
 * constructed graph into a graph database.</p>
 */

public class PropertySchema implements Writable {

    private StringType name = new StringType();

    private StringType type = new StringType();

    /**
     * Empty constructor for read methods.
     */
    public PropertySchema() {
    }

    /**
     * Constructor.
     *
     * @param name The name of the property as a serialized string (<code>StringType</code>).
     * @param type The name of the datatype of the property as a serialized string (<code>StringType</code>).
     */
    public PropertySchema(String name, Class<?> type) {
        this.name.set(name);
        this.type.set(type.getName());
    }

    /**
     * Sets the name of the property.
     *
     * @param name The name of the property.
     */
    public void setName(String name) {
        this.name.set(name);
    }

    /**
     * Gets the name of the property.
     *
     * @return The name of the property.
     */
    public String getName() {
        return this.name.get();
    }

    /**
     *
     */
    @Override
    public String toString() {
        return "Property: name == " + name.get() + " type == " + type.get();
    }
    /**
     * Equality test.
     *
     * @return true if the constituent name and datatype of the two property schemata are equal
     */
    @Override
    public boolean equals(Object in) {
        return ((in instanceof PropertySchema)
                && (this.name.equals(((PropertySchema) in).name)
                && (this.type.equals(((PropertySchema) in).type))));
    }

    /**
     * Hash code.
     * Built from the name and type name.
     *
     * @return an integer.
     */
    @Override
    public int hashCode() {
        return HashUtil.hashPair(this.name, this.type);
    }

    /**
     * Sets the datatype of the property.
     *
     * @param type The datatype of the property.
     */
    public void setType(Class<?> type) {
        this.type.set(type.getName());
    }

    /**
     * Gets the datatype of the property.
     *
     * @return datatype The datatype of the property.
     */
    public Class<?> getType() throws ClassNotFoundException {
        return Class.forName(this.type.get());
    }

    /**
     * Reads a PropertySchema from an input stream.
     *
     * @param input The input stream.
     * @throws java.io.IOException
     */
    @Override
    public void readFields(DataInput input) throws IOException {
        name.readFields(input);
        type.readFields(input);
    }

    /**
     * Writes a PropertySchema to an output stream.
     *
     * @param output The output stream.
     * @throws IOException
     */
    @Override
    public void write(DataOutput output) throws IOException {
        name.write(output);
        type.write(output);
    }
}
