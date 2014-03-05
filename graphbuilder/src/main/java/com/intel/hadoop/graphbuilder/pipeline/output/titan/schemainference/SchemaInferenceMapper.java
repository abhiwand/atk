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

package com.intel.hadoop.graphbuilder.pipeline.output.titan.schemainference;

import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElementStringTypeVids;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.SchemaElement;
import com.intel.hadoop.graphbuilder.util.GraphBuilderExit;
import com.intel.hadoop.graphbuilder.util.StatusCode;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * The mapper for the schema inference map-reduce job.
 * <p>The mapper inspects each property graph element and determines the property/datatype pairs that it requires,
 * as well as its label, if it is an edge. In the latter case, the edge label is packaged with the property/datatype
 * pairs for the properties that appear on the edge. This information is propagated in the emitted
 * <code>SerializedGraphElementStringTypeVids</code> objects.</p>
 * <p/>
 * <p>The input must be a null-keyed sequence file of <code>SerializedPropertyGraphElements</code>.</p>
 */

public class SchemaInferenceMapper extends Mapper<NullWritable, SerializedGraphElementStringTypeVids,
        NullWritable, SchemaElement> {

    private static final Logger LOG = Logger.getLogger(SchemaInferenceMapper.class);

    /**
     * Perform the map task on the given input.
     *
     * @param key     The input key; but its just The <code>NullWritable.get()</code> value.
     * @param value   The input value, <code>SerializedGraphElementStringTypeVids</code>} object.
     * @param context The job context.
     */
    @Override
    public void map(NullWritable key, SerializedGraphElementStringTypeVids value, Context context) {

        SchemaElement schemaElement = new SchemaElement(value.graphElement());

        try {
            writeSchemata(schemaElement, context);
        } catch (IOException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.UNHANDLED_IO_EXCEPTION,
                    "Graph Builder: IO Exception during schema inference.", LOG, e);
        }
    }

    /**
     * Writes a <code>SchemeElement</code> to the context's output.
     *
     * @param schemaElement The input <code>SchemaElement</code>.
     * @param context       The job context.
     * @throws IOException
     */
    public void writeSchemata(SchemaElement schemaElement, Mapper.Context context)
            throws IOException {

        try {
            context.write(NullWritable.get(), schemaElement);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
