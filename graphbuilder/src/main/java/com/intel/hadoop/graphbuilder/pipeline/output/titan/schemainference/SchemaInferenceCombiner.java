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
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.SerializedEdgeOrPropertySchema;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;

/**
 * This combiner  takes a multiset of {@code SerializedEdgeOrPropertySchema} objects, and merges them
 * into a duplicate-free list which is passed to the reducer.
 * <p/>
 * <p> Combination is done with the following semantics:
 * <li>
 * <ul>Two {@code PropertySchema} objects are identified if they have the same name.
 * They "combine" by throwing an exception  when two {@code Propertyschema} of the same name have different dataypes.</ul>
 * <ul>Two {@code EdgeSchema} objects are identified if they have the same label. They combine by merging their sets of
 * {@code PropertySchema}</ul>
 * </li></p>
 * <p/>
 * {@see MergeSchemaUtility}
 */
public class SchemaInferenceCombiner extends Reducer<NullWritable, SerializedEdgeOrPropertySchema,
        NullWritable, SerializedEdgeOrPropertySchema> {

    private static final Logger LOG = Logger.getLogger
            (SchemaInferenceCombiner.class);

    /**
     * The reduction method of this combiner.
     *
     * @param key     Hadoop mapreduce key shared by this batch of inputs.
     * @param values  The list of values sharing this key produced by the mapper.
     * @param context Hadoop context.
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(NullWritable key, Iterable<SerializedEdgeOrPropertySchema> values, Context context)
            throws IOException, InterruptedException {

        ArrayList<EdgeOrPropertySchema> outValues = MergeSchemataUtility.merge(values, LOG);

        writeSchemata(outValues, context);
    }

    /**
     * Writes a list of {@code EdgeOrPropertySchema}'s to the output.
     *
     * @param list    The {@code EdgeOrPropertySchema}'s to be written.
     * @param context The {@code Reducer.context} that tells Hadoop where and how to write.
     * @throws IOException
     */
    public void writeSchemata(ArrayList<EdgeOrPropertySchema> list, Reducer.Context context)
            throws IOException {

        SerializedEdgeOrPropertySchema serializedOut = new SerializedEdgeOrPropertySchema();

        for (EdgeOrPropertySchema schema : list) {
            try {
                serializedOut.setSchema(schema);
                context.write(NullWritable.get(), serializedOut);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
