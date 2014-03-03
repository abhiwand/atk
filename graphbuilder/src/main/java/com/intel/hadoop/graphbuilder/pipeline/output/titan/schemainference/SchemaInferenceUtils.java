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

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElementStringTypeVids;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.pipeline.output.titan.GBTitanKey;
import com.intel.hadoop.graphbuilder.pipeline.output.titan.KeyCommandLineParser;
import com.intel.hadoop.graphbuilder.pipeline.output.titan.TitanGraphInitializer;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.*;
import com.intel.hadoop.graphbuilder.types.EncapsulatedObject;
import com.intel.hadoop.graphbuilder.util.GraphBuilderExit;
import com.intel.hadoop.graphbuilder.util.StatusCode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * nls todo JAVADOC
 */
public class SchemaInferenceUtils {

    public static ArrayList<EdgeOrPropertySchema> schemataFromGraphElement(SerializedGraphElementStringTypeVids value) {
        ArrayList<EdgeOrPropertySchema> list = new ArrayList<>();

        if (value.graphElement().isEdge()) {

            Edge edge = (Edge) value.graphElement().get();

            EdgeSchema edgeSchema = new EdgeSchema(edge.getLabel().get());

            for (Writable key : edge.getProperties().getPropertyKeys()) {
                PropertySchema propertySchema = new PropertySchema();

                propertySchema.setName(key.toString());

                Object v = edge.getProperty(key.toString());
                Class<?> dataType = ((EncapsulatedObject) v).getBaseType();
                propertySchema.setType(dataType);

                edgeSchema.addPropertySchema(propertySchema);

                list.add(propertySchema);
            }

            list.add(edgeSchema);

        } else if (value.graphElement().isVertex()) {

            Vertex vertex = (Vertex) value.graphElement().get();
            value.graphElement().getProperties();

            for (Writable key : vertex.getProperties().getPropertyKeys()) {
                PropertySchema propertySchema = new PropertySchema();

                propertySchema.setName(key.toString());

                Object v = vertex.getProperty(key.toString());
                Class<?> dataType = ((EncapsulatedObject) v).getBaseType();
                propertySchema.setType(dataType);

                list.add(propertySchema);
            }
        }
        return list;
    }

    public static void writeSchemata(ArrayList<EdgeOrPropertySchema> list, Mapper.Context context)
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

    public static void writeSchemata(ArrayList<EdgeOrPropertySchema> list, Reducer.Context context)
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

    public static ArrayList<EdgeOrPropertySchema> combineSchemata(Iterable<SerializedEdgeOrPropertySchema> values,
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
                }  else {
                    propertySchemaHashMap.put(propertyName,propertySchema);
                }
            } else {
                EdgeSchema edgeSchema = (EdgeSchema) schema;
                String label = edgeSchema.getLabel();
                if (edgeSchemaHashMap.containsKey(label)) {
                    edgeSchemaHashMap.get(label).getPropertySchemata().addAll(edgeSchema.getPropertySchemata());
                }  else {
                    edgeSchemaHashMap.put(label, edgeSchema);
                }
            }
        }

        ArrayList<EdgeOrPropertySchema> outValues = new ArrayList<>();

        outValues.addAll(edgeSchemaHashMap.values());
        outValues.addAll(propertySchemaHashMap.values());

        return outValues;
    }

    public static void writeSchemaToTitan(ArrayList<EdgeOrPropertySchema> schemas, Reducer.Context context, Logger LOG) {

        PropertyGraphSchema graphSchema = new PropertyGraphSchema();

        for (EdgeOrPropertySchema schema : schemas) {
            if (schema instanceof EdgeSchema) {
                graphSchema.addEdgeSchema((EdgeSchema) schema);
            } else {
                try {
                    graphSchema.addPropertySchema((PropertySchema) schema);
                } catch (ClassNotFoundException e) {
                    String errorMessage = "Schema Inference error: Datatype not found for property " +
                            ((PropertySchema) schema).getName();
                    GraphBuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                            errorMessage, LOG, e);
                }
            }
        }

        Configuration conf = context.getConfiguration();
        String keyCommandLine = conf.get("keyCommandLine");

        KeyCommandLineParser titanKeyParser = new KeyCommandLineParser();
        List<GBTitanKey> keyList = titanKeyParser.parse(keyCommandLine);

        TitanGraphInitializer initializer = new TitanGraphInitializer(context.getConfiguration(), graphSchema, keyList);
        initializer.run();

    }


}
