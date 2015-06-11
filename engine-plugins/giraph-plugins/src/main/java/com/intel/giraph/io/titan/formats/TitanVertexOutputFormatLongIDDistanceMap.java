/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package com.intel.giraph.io.titan.formats;

import com.intel.giraph.io.DistanceMapWritable;
import com.intel.giraph.io.titan.TitanGraphWriter;
import com.intel.giraph.io.titan.common.GiraphTitanUtils;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.intel.giraph.io.titan.common.GiraphTitanConstants.CLOSED_GRAPH;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.OUTPUT_VERTEX_PROPERTY_KEY_LIST;

/**
 * The Vertex Output Format which writes back Giraph algorithm results
 * to Titan.
 * <p/>
 * Each Vertex is with <code>Long</code> id,
 * and <code>DistanceMap</code> values.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
public class TitanVertexOutputFormatLongIDDistanceMap<I extends LongWritable,
    V extends DistanceMapWritable, E extends NullWritable>
    extends TitanVertexOutputFormat<I,V,E> {

    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context) {
        return new TitanLongIDDistanceMapWriter();
    }

    /**
     * VertexWriter that writes Giraph results to Titan via BluePrint API
     * vertices with <code>Long</code> id
     * and <code>TwoVector</code> values.
     */
    public class TitanLongIDDistanceMapWriter extends TitanVertexWriterToEachLine {

        /**
         * Write results to Titan vertex
         *
         * @param vertex Giraph vertex
         * @return   Text line to be written
         * @throws IOException
         */
        @Override
        public Text convertVertexToLine(Vertex<I, V, E> vertex) throws IOException {
            long vertexId = vertex.getId().get();
            long numSources = 0;
            long sumHopCounts = 0;
            com.tinkerpop.blueprints.Vertex bluePrintVertex = this.graph.getVertex(vertexId);
            HashMap<Long, Integer> distanceMap = vertex.getValue().getDistanceMap();

            for (Map.Entry<Long, Integer> entry : distanceMap.entrySet()) {
                numSources++;
                sumHopCounts += entry.getValue();
            }

            bluePrintVertex.setProperty(vertexValuePropertyKeyList[0], Long.toString(numSources));
            bluePrintVertex.setProperty(vertexValuePropertyKeyList[1], Long.toString(sumHopCounts));
            commitVerticesInBatches();
            return null;
        }

    }
}
