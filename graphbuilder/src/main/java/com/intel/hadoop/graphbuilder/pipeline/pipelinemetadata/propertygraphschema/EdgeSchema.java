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
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

/**
 * The type of an edge declaration used in graph construction. It encapsulates the
 * label of the edge, as well as the names and datatypes of the properties that can
 * be associated with edges of this type.
 * <p/>
 * The expected use of this information is declaring keys for loading the constructed
 * graph into a graph database.
 */
public class EdgeSchema extends EdgeOrPropertySchema implements Writable {

    private ArrayList<PropertySchema> propertySchemata;
    private String label;
    private StringType id = new StringType();

    private PropertySchemaArrayWritable serializedPropertySchemata = new PropertySchemaArrayWritable();
    private StringType serializedLabel = new StringType();

    public EdgeSchema(String label) {
        this.label = label;
        propertySchemata = new ArrayList<>();
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getLabel() {
        return this.label;
    }

    public StringType getID() {
        id.set(EDGE_SCHEMA + "." + label);
        return id;
    }

    public ArrayList<PropertySchema> getPropertySchemata() {
        return propertySchemata;
    }

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
