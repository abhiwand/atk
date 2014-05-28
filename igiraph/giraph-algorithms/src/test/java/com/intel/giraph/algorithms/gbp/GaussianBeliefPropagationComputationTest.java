//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2013 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

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
        conf.set("gbp.maxSupersteps", "119");
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
        HashMap<Long, Double[]> expectedValues = new HashMap<>();
        String[] graph = new String[] {
            "[0,[10,3],[[1,2],[2,1]]]",
            "[1,[10,4],[[0,2]]]",
            "[2,[16,5],[[0,1]]]"
        };
        expectedValues.put(0L, new Double[]{ 10.0, 3.0, 1.0, 0.330});
        expectedValues.put(1L, new Double[]{ 10.0, 4.0, 2.0, 0.25});
        expectedValues.put(2L, new Double[]{ 16.0, 5.0, 3.0, 0.174});
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
        HashMap<Long, Double[]> expectedValues = new HashMap<>();

        String[] graph = new String[] {
            "[0,[-6,1],[[1,-2],[2,3]]]",
            "[1,[0,1],[[0,-2]]]",
            "[2,[2,1],[[0,3]]]"
        };
        expectedValues.put(0L, new Double[]{-6.0, 1.0,  1.0, -0.08333});
        expectedValues.put(1L, new Double[]{ 0.0, 1.0,  2.0,  0.66667});
        expectedValues.put(2L, new Double[]{ 2.0, 1.0, -1.0,  0.25});
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
        HashMap<Long, Double[]> expectedValues = new HashMap<>();
        String[] graph = new String[] {
            "[0,[-1,5],[[1,-2,-3],[2,3,2]]]",
            "[1,[2,9],[[0,-3,-2],[2,1,-1]]]",
            "[2,[3,-7],[[0,2,3],[1,-1,1]]]"
        };
        expectedValues.put(0L, new Double[]{-1.0, 5.0,  0.182, 0.1108});
        expectedValues.put(1L, new Double[]{ 2.0, 9.0,  0.330, 0.0896});
        expectedValues.put(2L, new Double[]{ 3.0, -7.0, -0.407,-0.2122});
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
        HashMap<Long, Double[]> expectedValues = new HashMap<>();
        String[] graph = new String[] {
            "[0,[6,7],[[1,-1,1]]]",
            "[1,[-4,-5],[[0,1,-1]]]"
        };
        expectedValues.put(0L, new Double[]{6.0, 7.0,  1.0, 0.129});
        expectedValues.put(1L, new Double[]{-4.0, -5.0,  1.0,  -0.258});
        conf.set("gbp.convergenceThreshold", "0.007");
        conf.set("gbp.outerLoop", "true");
        runTest(conf, graph, graphSize, expectedValues);
    }


    public void runTest(GiraphConfiguration conf, String[] graph, int graphSize, HashMap<Long, Double[]> expectedValues) throws Exception {
        // run internally
        Iterable<String> results = InternalVertexRunner.run(conf, graph);
        Map<Long, Double[]> vertexValues = parseVertexValues(results);

        // verify results
        assertNotNull(vertexValues);
        assertEquals(graphSize, vertexValues.size());
        for (Map.Entry<Long, Double[]> entry : vertexValues.entrySet()) {
            assertEquals(4, entry.getValue().length);
            for (int j = 0; j < 4; j++) {
                assertEquals(expectedValues.get(entry.getKey())[j], entry.getValue()[j], 0.03d);
            }
        }
    }

    private Map<Long, Double[]> parseVertexValues(Iterable<String> results) {
        Map<Long, Double[]> vertexValues = Maps.newHashMapWithExpectedSize(Iterables.size(results));
        for (String line : results) {
            try {
                JSONArray jsonVertex = new JSONArray(line);
                if (jsonVertex.length() != 3) {
                    throw new IllegalArgumentException("Wrong vertex output format!");
                }
                // get id
                long id = jsonVertex.getLong(0);
                // get prior
                JSONArray priorArray = jsonVertex.getJSONArray(1);
                if (priorArray.length() != 2) {
                    throw new IllegalArgumentException("Wrong vertex prior output value format!");
                }
                Double[] values = new Double[4];
                for (int i = 0; i < 2; i++) {
                    values[i] = priorArray.getDouble(i);
                }
                // get posterior
                JSONArray posteriorArray = jsonVertex.getJSONArray(2);
                if (posteriorArray.length() != 2) {
                    throw new IllegalArgumentException("Wrong vertex posterior output value format!");
                }
                for (int i = 2; i < 4; i++) {
                    values[i] = posteriorArray.getDouble(i - 2);
                }
                vertexValues.put(id, values);
                } catch (JSONException e) {
                    throw new IllegalArgumentException("Couldn't get vertex from line " + line, e);
                }
        }
        return vertexValues;
    }
}
