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

import com.intel.giraph.io.VertexData4LBPWritable;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.apache.mahout.math.Vector;

import java.io.IOException;

import static com.intel.giraph.io.titan.common.GiraphTitanConstants.*;

/**
 * The Vertex Output Format which writes back Giraph LBP algorithm
 * results to Titan.
 * <p/>
 * Each Vertex is with <code>Long</code> id,
 * and <code>VertexData4LBPWritable</code> values.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */

public class TitanVertexOutputFormatPropertyGraph4LBP<I extends LongWritable,
        V extends VertexData4LBPWritable, E extends Writable>
        extends TitanVertexOutputFormat<I, V, E> {

    /**
     * LOG class
     */
    private static final Logger LOG = Logger
            .getLogger(TitanVertexOutputFormatPropertyGraph4LBP.class);

    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context) {
        return new TitanVertexPropertyGraph4LBPWriter();
    }

    /**
     * VertexWriter that writes Giraph results to Titan via BluePrint API
     * vertices with <code>Long</code> id
     * and <code>TwoVector</code> values.
     */
    protected class TitanVertexPropertyGraph4LBPWriter extends TitanVertexWriterToEachLine {

        /**
         * Enable vector value
         */
        private String enableVectorValue = "true";

        @Override
        public void initialize(TaskAttemptContext context) throws IOException,
                InterruptedException {
            super.initialize(context);
            enableVectorValue = VECTOR_VALUE.get(context.getConfiguration());
        }


        @Override
        public Text convertVertexToLine(Vertex<I, V, E> vertex) throws IOException {

            long vertexId = vertex.getId().get();
            com.tinkerpop.blueprints.Vertex bluePrintVertex = this.graph.getVertex(vertexId);
            Vector vector = vertex.getValue().getPosteriorVector();

            //output vertex value
            if (enableVectorValue.equals("true")) {
                //output in a comma separated value format
                if (vertexValuePropertyKeyList.length == 1) {
                    String vertexValue = "";
                    for (int i = 0; i < vector.size(); i++) {
                        vertexValue += Double.toString(vector.getQuick(i));
                        if (i < (vector.size() - 1)) {
                            vertexValue = vertexValue + ",";
                        }
                    }
                    //LOG.info("LBP vertexValue:" + vertexValue);
                    bluePrintVertex.setProperty(vertexValuePropertyKeyList[0], vertexValue);
                } else {
                    generateErrorMsg(1, vertex.getId().get());
                }
            } else {
                if (vector.size() == vertexValuePropertyKeyList.length) {
                    for (int i = 0; i < vector.size(); i++) {
                        bluePrintVertex.setProperty(vertexValuePropertyKeyList[i],
                                Double.toString(vector.getQuick(i)));
                    }
                } else {
                    generateErrorMsg(vector.size(), vertex.getId().get());
                }
            }
            commitVerticesInBatches();
            return null;
        }


        /**
         * Generate error message when vertex is not in expected format.
         *
         * @param size     The number of vertex value properties
         * @param vertexId The vertex Id
         */
        public void generateErrorMsg(int size, long vertexId) {
            LOG.error(VERTEX_PROPERTY_MISMATCH + EXPECTED_SIZE_OF_VERTEX_PROPERTY + size +
                    REAL_SIZE_OF_VERTEX_PROPERTY + vertexValuePropertyKeyList.length);
            throw new IllegalArgumentException(VERTEX_PROPERTY_MISMATCH +
                    CURRENT_VERTEX + vertexId);
        }
    }
}
