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

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;

public class SerializedEdgeOrPropertySchema implements Writable {

    private EdgeOrPropertySchema schema;

    public SerializedEdgeOrPropertySchema() {
        schema = null;
    }

    public SerializedEdgeOrPropertySchema(EdgeOrPropertySchema schema) {
        this.schema = schema;
    }


    public EdgeOrPropertySchema getSchema() {
        return schema;
    }

    public void setSchema(EdgeOrPropertySchema schema) {
        this.schema = schema;
    }

    /**
     * Writes a {@code SerializedEdgeOrPropertySchema} to an output stream.
     *
     * @param output The output stream.
     * @throws IOException
     */
    @Override
    public void write(DataOutput output) throws IOException {

        if (schema != null) {
            output.writeBoolean(schema instanceof PropertySchema);

            schema.write(output);
        }
    }

    /**
     * Reads a {@code SerializedEdgeOrPropertySchema} from an input stream.
     *
     * @param input The input stream.
     * @throws java.io.IOException
     */
    @Override
    public void readFields(DataInput input) throws IOException {

        boolean isPropertySchema = input.readBoolean();

        if (isPropertySchema) {
            schema = new PropertySchema();

            schema.readFields(input);
        } else {
            schema = new EdgeSchema();

            schema.readFields(input);
        }
    }


    @Override
    public boolean equals (Object in) {
        if (in instanceof SerializedEdgeOrPropertySchema) {
            EdgeOrPropertySchema inSchema = ((SerializedEdgeOrPropertySchema) in).getSchema();
            return (schema == inSchema || schema.equals(inSchema));
        } else {
            return false;        }
    }


    @Override
    public int hashCode () {
        if (schema == null) {
            return 0xabba0666;
        }  else {
            return schema.hashCode();
        }
    }
}
