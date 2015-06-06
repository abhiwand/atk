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

package com.intel.giraph.algorithms.gbp;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.intel.giraph.algorithms.gbp.GaussianBeliefPropagationComputation.GaussianBeliefPropagationAggregatorWriter;
import com.intel.giraph.algorithms.gbp.GaussianBeliefPropagationComputation.GaussianBeliefPropagationMasterCompute;
import com.intel.giraph.io.formats.JsonPropertyGraph4GBPInputFormat;
import com.intel.giraph.io.formats.JsonPropertyGraph4GBPOutputFormat;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.utils.InternalVertexRunner;
import org.json.JSONArray;
import org.json.JSONException;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class GaussianBeliefPropagationComputationTest {
    protected final GiraphConfiguration conf = new GiraphConfiguration();
    /**
     * set up GiraphConfiguration for different test cases
     */
    @Before
    public void setConf() throws Exception {
        conf.setComputationClass(GaussianBeliefPropagationComputation.class);
        conf.setMasterComputeClass(GaussianBeliefPropagationMasterCompute.class);
        conf.setAggregatorWriterClass(GaussianBeliefPropagationAggregatorWriter.class);
        conf.setVertexInputFormatClass(JsonPropertyGraph4GBPInputFormat.class);
        conf.setVertexOutputFormatClass(JsonPropertyGraph4GBPOutputFormat.class);
        conf.set("giraph.useSuperstepCounters", "false");
        conf.set("gbp.maxSupersteps", "2000");
        conf.set("gbp.bidirectionalCheck", "true");
    }

    /**
     * Test 3x3 symmetric matrix
     *  3*x1 + 2*x2 +  x3  = 10
     *  2*x1 + 4*x2        = 10
     *    x1        + 5*x3 = 16
     */
    @Test
    public void testSymmetricCase1() throws Exception {
        int graphSize = 3;
        HashMap<Long, Double> expectedValues = new HashMap<>();
        String[] graph = new String[] {
            "[0,[10,3],[[1,2],[2,1]]]",
            "[1,[10,4],[[0,2]]]",
            "[2,[16,5],[[0,1]]]"
        };
        expectedValues.put(0L, 1.0);
        expectedValues.put(1L, 2.0);
        expectedValues.put(2L, 3.0);
        conf.set("gbp.convergenceThreshold", "0.005");
        conf.set("gbp.outerLoop", "true");
        runTest(conf, graph, graphSize, expectedValues);
    }

    /**
     * Test another 3x3 symmetric matrix
     *
     *    x1 - 2*x2 + 3*x3 = -6
     * -2*x1 +   x2        =  0
     *  3*x1        +   x3 =  2
     */
    @Test
    public void testSymmetricCase2() throws Exception {
        int graphSize = 3;
        HashMap<Long, Double> expectedValues = new HashMap<>();

        String[] graph = new String[] {
            "[0,[-6,1],[[1,-2],[2,3]]]",
            "[1,[0,1],[[0,-2]]]",
            "[2,[2,1],[[0,3]]]"
        };
        expectedValues.put(0L, 1.0);
        expectedValues.put(1L, 2.0);
        expectedValues.put(2L, -1.0);
        conf.set("gbp.convergenceThreshold", "0.001");
        conf.set("gbp.outerLoop", "false");
        runTest(conf, graph, graphSize, expectedValues);
    }

    /**
     * Test 3x3 asymmetric matrix
     *  5*x1 - 2*x2 + 3*x3 = -1
     * -3*x1 + 9*x2 + x3   =  2
     *  2*x1 -   x2 - 7*x3 =  3
     */
    @Test
    public void testAsymmetricCase1() throws Exception {
        int graphSize = 3;
        HashMap<Long, Double> expectedValues = new HashMap<>();
        String[] graph = new String[] {
            "[0,[-1,5],[[1,-2,-3],[2,3,2]]]",
            "[1,[2,9],[[0,-3,-2],[2,1,-1]]]",
            "[2,[3,-7],[[0,2,3],[1,-1,1]]]"
        };
        expectedValues.put(0L, 0.182);
        expectedValues.put(1L, 0.330);
        expectedValues.put(2L, -0.407);
        conf.set("gbp.convergenceThreshold", "0.007");
        conf.set("gbp.outerLoop", "true");
        runTest(conf, graph, graphSize, expectedValues);
    }


    /**
     * Test 2x2 asymmetric matrix
     *  7*x1 -   x2 =  6
     *    x1 - 5*x2 = -4
     */
    @Test
    public void testAsymmetricCase2() throws Exception {
        int graphSize = 2;
        HashMap<Long, Double> expectedValues = new HashMap<>();
        String[] graph = new String[] {
            "[0,[6,7],[[1,-1,1]]]",
            "[1,[-4,-5],[[0,1,-1]]]"
        };
        expectedValues.put(0L, 1.0);
        expectedValues.put(1L, 1.0);
        conf.set("gbp.convergenceThreshold", "0.007");
        conf.set("gbp.outerLoop", "true");
        runTest(conf, graph, graphSize, expectedValues);
    }


    public void runTest(GiraphConfiguration conf, String[] graph, int graphSize, HashMap<Long, Double> expectedValues) throws Exception {
        // run internally
        Iterable<String> results = InternalVertexRunner.run(conf, graph);
        Map<Long, Double> vertexValues = parseVertexValues(results);

        // verify results
        assertNotNull(vertexValues);
        assertEquals(graphSize, vertexValues.size());
        for (Map.Entry<Long, Double> entry : vertexValues.entrySet()) {
            assertEquals(expectedValues.get(entry.getKey()), entry.getValue(), 0.03d);
        }
    }

    private Map<Long, Double> parseVertexValues(Iterable<String> results) {
        Map<Long, Double> vertexValues = Maps.newHashMapWithExpectedSize(Iterables.size(results));
        for (String line : results) {
            try {
                JSONArray jsonVertex = new JSONArray(line);
                if (jsonVertex.length() != 2) {
                    throw new IllegalArgumentException("Wrong vertex output format!");
                }
                // get id
                long id = jsonVertex.getLong(0);
                // get posterior mean
                vertexValues.put(id, jsonVertex.getDouble(1));
                } catch (JSONException e) {
                    throw new IllegalArgumentException("Couldn't get vertex from line " + line, e);
                }
        }
        return vertexValues;
    }
}
