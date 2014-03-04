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
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.EdgeSchema;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.PropertySchema;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.SerializedEdgeOrPropertySchema;
import com.intel.hadoop.graphbuilder.util.GraphBuilderExit;
import com.intel.hadoop.graphbuilder.util.StatusCode;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * A utility class that takes a multiset of {@code SerializedEdgeOrPropertySchema} objects, and merges so
 * into a duplicate-free list.
 * <p> Combination is done with the following semantics:
 * <li>
 * <ul>Two {@code PropertySchema} objects are identified if they have the same name.
 * They "combine" by throwing an exception  when two {@code Propertyschema} of the same name have different dataypes.</ul>
 * <ul>Two {@code EdgeSchema} objects are identified if they have the same label. They combine by merging their sets of
 * {@code PropertySchema}</ul>
 * </li></p>
 */
public class MergeSchemataUtility {

    /**
     * Performs the merge of a multiset of {@code SerializedEdgeOrPropertySchema} objects into a duplicate free list.
     *
     * @param values The {@code SerializedEdgeOrPropertySchema} objects to be merged.
     * @param LOG    A logger for reporting errors.
     * @return Duplicate-free list of {@code SerializedEdgeOrPropertySchema}'s.
     */
    public static ArrayList<EdgeOrPropertySchema> merge(Iterable<SerializedEdgeOrPropertySchema> values,
                                                        Logger LOG) {

        HashMap<String, EdgeSchema> edgeSchemaHashMap = new HashMap<>();
        HashMap<String, PropertySchema> propertySchemaHashMap = new HashMap<>();

        for (SerializedEdgeOrPropertySchema serializedSchema : values) {
            EdgeOrPropertySchema schema = serializedSchema.getSchema();

            if (schema instanceof PropertySchema) {
                PropertySchema propertySchema = (PropertySchema) schema;
                String propertyName = propertySchema.getName();

                if (propertySchemaHashMap.containsKey(propertyName)) {
                    try {
                        if (!propertySchema.getType().equals(propertySchemaHashMap.get(propertyName).getType())) {
                            String errorMessage = "Schema Inference error: Conflicting datatypes for property " +
                                    ((PropertySchema) schema).getName();
                            GraphBuilderExit.graphbuilderFatalExitNoException(StatusCode.CLASS_INSTANTIATION_ERROR,
                                    errorMessage, LOG);
                        }
                    } catch (ClassNotFoundException e) {
                        String errorMessage = "Schema Inference error: Datatype not found for property " +
                                ((PropertySchema) schema).getName();
                        GraphBuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                                errorMessage, LOG, e);
                    }
                } else {
                    propertySchemaHashMap.put(propertyName, propertySchema);
                }
            } else {
                EdgeSchema edgeSchema = (EdgeSchema) schema;
                String label = edgeSchema.getLabel();
                if (edgeSchemaHashMap.containsKey(label)) {
                    edgeSchemaHashMap.get(label).getPropertySchemata().addAll(edgeSchema.getPropertySchemata());
                } else {
                    edgeSchemaHashMap.put(label, edgeSchema);
                }
            }
        }

        ArrayList<EdgeOrPropertySchema> outValues = new ArrayList<>();

        outValues.addAll(edgeSchemaHashMap.values());
        outValues.addAll(propertySchemaHashMap.values());

        return outValues;
    }
}
