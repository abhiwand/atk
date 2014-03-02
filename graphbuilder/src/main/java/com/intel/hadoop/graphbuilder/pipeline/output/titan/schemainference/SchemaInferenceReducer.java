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

import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.EdgeOrPropertySchema;
import com.intel.hadoop.graphbuilder.types.StringType;
import com.intel.hadoop.graphbuilder.util.GraphDatabaseConnector;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class SchemaInferenceReducer extends Reducer<StringType, EdgeOrPropertySchema,  NullWritable, NullWritable> {

    private TitanGraph titanGraph = null;
    /**
     * Creates the Titan graph for saving edges and removes the static open
     * method from setup so it can be mocked-up.
     *
     * @return TitanGraph  For saving edges.
     * @throws IOException
     */
    private TitanGraph getTitanGraphInstance (Context context) throws
            IOException {
        BaseConfiguration titanConfig = new BaseConfiguration();
        return GraphDatabaseConnector.open("titan", titanConfig,
                context.getConfiguration());
    }

    /**
     * Sets up the reducer at the start of the task.
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void setup(Context context)  throws IOException,
            InterruptedException {

        titanGraph = getTitanGraphInstance(context);

    }


    @Override
    public void reduce(StringType key, Iterable<EdgeOrPropertySchema> values,
                       Reducer<StringType, EdgeOrPropertySchema,  NullWritable, NullWritable>.Context context)
            throws IOException, InterruptedException {

        ArrayList<EdgeOrPropertySchema> schemas = SchemaInferenceUtils.combineSchemata(values);

        SchemaInferenceUtils.writeSchemaToTitan(schemas, titanGraph, context);

    }

    /**
     * Closes the Titan graph connection at the end of the reducer.
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void cleanup(Context context) throws IOException,
            InterruptedException {
        this.titanGraph.shutdown();
    }
}
