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

package com.intel.giraph.algorithms.als;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.intel.giraph.algorithms.als.AlternatingLeastSquaresComputation.AlternatingLeastSquaresAggregatorWriter;
import com.intel.giraph.algorithms.als.AlternatingLeastSquaresComputation.AlternatingLeastSquaresMasterCompute;
import com.intel.giraph.io.formats.JsonPropertyGraph4CFInputFormat;
import com.intel.giraph.io.formats.JsonPropertyGraph4CFOutputFormat;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.utils.InternalVertexRunner;
import org.json.JSONArray;
import org.json.JSONException;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class AlternatingLeastSquaresComputationTest {

    /**
     * A local test on toy data
     */
    @Test
    public void testToyData() throws Exception {
        // a small five-vertex graph
        String[] graph = new String[] {
            "[0,[],[\"L\"],[[2,1,[\"TR\"]],[3,2,[\"te\"]]]]",
            "[1,[],[\"L\"],[[2,5,[\"tr\"]],[4,3,[\"VA\"]]]]",
            "[2,[],[\"R\"],[[0,1,[\"tr\"]],[1,5,[\"tr\"]]]]",
            "[3,[],[\"R\"],[[0,2,[\"te\"]]]]",
            "[4,[],[\"R\"],[[1,3,[\"va\"]]]]"
        };

        double[][] expectedValues = new double[][] {
            {0.16303398451511825,0.11920179824797916,0.14696659798422967},
            {0.8151699225755752,0.5960089912399223,0.7348329899211448},
            {2.6050355551222606,1.9046637644542772,2.348303111855532},
            {0,0,0},
            {0,0,0}
        };
        
        GiraphConfiguration conf = new GiraphConfiguration();

        conf.setComputationClass(AlternatingLeastSquaresComputation.class);
        conf.setMasterComputeClass(AlternatingLeastSquaresMasterCompute.class);
        conf.setAggregatorWriterClass(AlternatingLeastSquaresAggregatorWriter.class);
        conf.setVertexInputFormatClass(JsonPropertyGraph4CFInputFormat.class);
        conf.setVertexOutputFormatClass(JsonPropertyGraph4CFOutputFormat.class);
        conf.set("als.maxSupersteps", "6");
        conf.set("als.featureDimension", "3");
        conf.set("als.lambda", "0.05");
        conf.set("als.convergenceThreshold", "0");

        // run internally
        Iterable<String> results = InternalVertexRunner.run(conf, graph);

        Map<Long, Double[]> vertexValues = parseVertexValues(results);
        
        // verify results
        assertNotNull(vertexValues);
        assertEquals(5, vertexValues.size());
        for (Map.Entry<Long, Double[]> entry : vertexValues.entrySet()) {
            assertEquals(4, entry.getValue().length);
            assertEquals(0.0, entry.getValue()[0], 0d);
            for (int j = 0; j < 3; j++) {
                assertEquals(expectedValues[entry.getKey().intValue()][j], entry.getValue()[j+1], 0.01d);    
            }
        }
    }
  
    private Map<Long, Double[]> parseVertexValues(Iterable<String> results) {
        Map<Long, Double[]> vertexValues = Maps.newHashMapWithExpectedSize(Iterables.size(results));
        for (String line : results) {
            try {
                    JSONArray jsonVertex = new JSONArray(line);
                    if (jsonVertex.length() != 4) {
                        throw new IllegalArgumentException("Wrong vertex output format!");
                    }
                    // get vertex id
                    long id = jsonVertex.getLong(0);
                    // get vertex bias
                    JSONArray biasArray = jsonVertex.getJSONArray(1);
                    if (biasArray.length() != 1) {
                        throw new IllegalArgumentException("Wrong vertex bias value output value format!");
                    }
                    double bias = biasArray.getDouble(0);
                    JSONArray valueArray = jsonVertex.getJSONArray(2);
                    if (valueArray.length() != 3) {
                        throw new IllegalArgumentException("Wrong vertex vector output value format!");
                    }
                    Double[] values = new Double[4];
                    values[0] = bias;
                    for (int i = 0; i < 3; i++) {
                        values[i+1] = valueArray.getDouble(i);
                    }
                    vertexValues.put(id, values);
                    // get vertex type
                    JSONArray typeArray = jsonVertex.getJSONArray(3);
                    if (typeArray.length() != 1) {
                        throw new IllegalArgumentException("Wrong vertex type output value format!");
                    }
                } catch (JSONException e) {
                    throw new IllegalArgumentException("Couldn't get vertex from line " + line, e);
                }
        }
        return vertexValues;
    }
}
