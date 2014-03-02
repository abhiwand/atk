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
import com.intel.hadoop.graphbuilder.pipeline.output.titan.TitanGraphInitializer;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.EdgeOrPropertySchema;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.EdgeSchema;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.PropertyGraphSchema;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.PropertySchema;
import com.intel.hadoop.graphbuilder.types.EncapsulatedObject;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * nls todo JAVADOC
 */
public class SchemaInferenceUtils {

    public static ArrayList<EdgeOrPropertySchema> getSchemaInfo(SerializedGraphElementStringTypeVids value) {
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

        for (EdgeOrPropertySchema schema : list) {
            try {
                context.write(NullWritable.get(),schema);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public static void writeSchemata(ArrayList<EdgeOrPropertySchema> list, Reducer.Context context)
            throws IOException {

        for (EdgeOrPropertySchema schema : list) {
            try {
                context.write(NullWritable.get(),schema);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public static ArrayList<EdgeOrPropertySchema> combineSchemata(Iterable<EdgeOrPropertySchema> values) {


        HashMap<String, EdgeSchema> edgeSchemaHashMap = new HashMap<>();
        HashMap<String, PropertySchema> propertySchemaHashMap = new HashMap<>();

        for (EdgeOrPropertySchema schema : values) {
            if (schema instanceof PropertySchema) {
                PropertySchema propertySchema = (PropertySchema) schema;
                String propertyName = propertySchema.getName();

                if (propertySchemaHashMap.containsKey(propertyName)) {
                    try {
                        if (!propertySchema.getType().equals(propertySchemaHashMap.get(propertyName).getType())) {
                            // raise bloody hell
                        }
                    } catch (ClassNotFoundException e) {
                        // raise bloody hell
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

    public static void writeSchemaToTitan(ArrayList<EdgeOrPropertySchema> schemas, TitanGraph titanGraph,
                                          Reducer.Context context) {

        PropertyGraphSchema graphSchema = new PropertyGraphSchema();

        for (EdgeOrPropertySchema schema : schemas) {
            if (schema instanceof EdgeSchema) {
                graphSchema.addEdgeSchema((EdgeSchema) schema);
            } else {
                try {
                    graphSchema.addPropertySchema((PropertySchema) schema);
                } catch (ClassNotFoundException e) {
                    // raise bloody hell
                }
            }
        }

        // todo : we need to pass the key argument around through the context or something and then
        //  stuff it into the context or something like that

        ArrayList<GBTitanKey> keyList = new ArrayList<>();
        TitanGraphInitializer initializer = new TitanGraphInitializer(context.getConfiguration(), graphSchema, keyList);
        initializer.run();

    }


}
