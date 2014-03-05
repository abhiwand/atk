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

import com.intel.hadoop.graphbuilder.pipeline.output.titan.GBTitanKey;
import com.intel.hadoop.graphbuilder.pipeline.output.titan.KeyCommandLineParser;
import com.intel.hadoop.graphbuilder.pipeline.output.titan.TitanGraphInitializer;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.*;
import com.intel.hadoop.graphbuilder.util.GraphDatabaseConnector;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The reducer for the schema inference job. It performs two tasks:
 * <ul>
 * <li>Similar to the {@code SchemaInferenceCombiner} it combines the input graph elements into a duplicate-free list.</li>
 * <li>It then uses the combined schema to initialize the Titan connection.</li>
 * </ul>
 * <p/>
 * Because the values are all null-keyed, all values will appear at a single reducer. This is on purpose so the
 * call to the {@code TitanGraphInitializer} can have a view of all the information needed to initialize Titan.
 *
 * @see SchemaInferenceCombiner
 * @see MergeSchemataUtility
 */
public class SchemaInferenceReducer extends Reducer<NullWritable, SchemaElement,
        NullWritable, SchemaElement> {

    private static final Logger LOG = Logger.getLogger(SchemaInferenceReducer.class);

    private TitanGraph titanGraph = null;
    private TitanGraphInitializer initializer = null;

    /**
     * Sets the {@code TitanGraphInitializerObject} used to initialize Titan. Used for testing.
     *
     * @param initializer the incoming  {@code TitanGraphInitializerObject}.
     */
    protected void setInitializer(TitanGraphInitializer initializer) {
        this.initializer = initializer;
    }

    /**
     * The reduce call for the reducer.
     *
     * @param key     The {@code NullWritable.get()}  value.
     * @param values  A multi-set of edge and property schemata.
     * @param context The job context.
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(NullWritable key, Iterable<SchemaElement> values,
                       Reducer<NullWritable, SchemaElement, NullWritable,
                               SchemaElement>.Context context)
            throws IOException, InterruptedException {

        MergeSchemataUtility mergeUtility = new MergeSchemataUtility();
        writeSchemaToTitan(mergeUtility.merge(values), context);
    }

    /**
     * Write a list of edge and/or property schemata to Titan during graph initialization. The writer will
     * also get property information from any keys declared on the command line (if there are any.)
     *
     * @param schemas The schemas to be written; presumably obtained by scanning over the data.
     * @param context The job context..
     */
    public void writeSchemaToTitan(List<SchemaElement> schemas, Context context) {

        Configuration conf = context.getConfiguration();
        String keyCommandLine = conf.get("keyCommandLine");

        KeyCommandLineParser titanKeyParser = new KeyCommandLineParser();
        List<GBTitanKey> declaredKeyList = titanKeyParser.parse(keyCommandLine);

        initializer.setConf(context.getConfiguration());
        initializer.setGraphSchema(schemas);
        initializer.setDeclaredKeys(declaredKeyList);
        initializer.run(titanGraph);
    }

    /**
     * Creates the Titan graph for saving edges and removes the static open
     * method from setup so it can be mocked-up.
     *
     * @return TitanGraph  For saving edges.
     * @throws IOException
     */
    private TitanGraph getTitanGraphInstance(Context context) throws
            IOException {
        BaseConfiguration titanConfig = new BaseConfiguration();
        return GraphDatabaseConnector.open("titan", titanConfig,
                context.getConfiguration());
    }

    /**
     * Sets up the reducer at the start of the task.
     *
     * @param context The job context.
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void setup(Context context) throws IOException,
            InterruptedException {

        titanGraph = getTitanGraphInstance(context);

        initializer = new TitanGraphInitializer();
    }

    /**
     * Closes the Titan graph connection at the end of the reducer.
     *
     * @param context The job context.
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void cleanup(Context context) throws IOException,
            InterruptedException {
        this.titanGraph.shutdown();
    }
}
