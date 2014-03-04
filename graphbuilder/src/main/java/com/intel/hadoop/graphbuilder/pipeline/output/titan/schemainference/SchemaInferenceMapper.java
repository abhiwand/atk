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
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.EdgeOrPropertySchema;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.EdgeSchema;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.PropertySchema;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.SerializedEdgeOrPropertySchema;
import com.intel.hadoop.graphbuilder.types.EncapsulatedObject;
import com.intel.hadoop.graphbuilder.util.GraphBuilderExit;
import com.intel.hadoop.graphbuilder.util.StatusCode;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;

/**
 * The mapper for the schema inference map-reduce job.
 * <p>The mapper inspects each property graph element and determines the property/datatype pairs that it requires,
 * as well as its label, if it is an edge. In the latter case, the edge label is packaged with the property/datatype
 * pairs for the properties that appear on the edge. This information is propagated in the emitted
 * {@code SerializedGraphElementStringTypeVids} objects.</p>
 * <p/>
 * <p>The input must be a null-keyed sequence file of {@code SerializedPropertyGraphElements}.</p>
 */

public class SchemaInferenceMapper extends Mapper<NullWritable, SerializedGraphElementStringTypeVids,
        NullWritable, SerializedEdgeOrPropertySchema> {

    private static final Logger LOG = Logger.getLogger(SchemaInferenceMapper.class);

    /**
     * Perform the map task on the given input.
     *
     * @param key     The input key; but its just The {@code NullWritable.get()} value.
     * @param value   The input value, {@code SerializedGraphElementStringTypeVids} object.
     * @param context The job context.
     */
    @Override
    public void map(NullWritable key, SerializedGraphElementStringTypeVids value, Context context) {

        ArrayList<EdgeOrPropertySchema> list = schemataFromGraphElement(value);

        try {
            writeSchemata(list, context);
        } catch (IOException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.UNHANDLED_IO_EXCEPTION,
                    "Graph Builder: IO Exception during schema inference.", LOG, e);
        }
    }

    /**
     * Writes a list of {@code EdgeOrPropertySchema}'s to the context's output.
     *
     * @param list    The input list of  {@code EdgeOrPropertySchema}'s.
     * @param context The job context.
     * @throws IOException
     */
    public void writeSchemata(ArrayList<EdgeOrPropertySchema> list, Mapper.Context context)
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

    /**
     * Scan over a {@code SerializedGraphElementStringTypeVids} and return its schema information in a list.
     *
     * @param value A serialized graph element.
     * @return The schema information of {@code value} in the form of a list of {@code EdgeOrPropertySchema}'s.
     */
    public ArrayList<EdgeOrPropertySchema> schemataFromGraphElement(SerializedGraphElementStringTypeVids value) {
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
}
