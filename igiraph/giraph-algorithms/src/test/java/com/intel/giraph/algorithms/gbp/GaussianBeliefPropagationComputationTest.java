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
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class GaussianBeliefPropagationComputationTest {

    /**
     * A local test on toy data
     */
    @Test
    public void testToyData() throws Exception {
        // a small five-vertex graph
        String[] graph = new String[] {
            "[0,[-6,1],[[1,-2],[2,3]]]",
            "[1,[0,1],[[0,-2]]]",
            "[2,[2,1],[[0,3]]]"
        };

        HashMap<Long, Double[]> expectedValues = new HashMap<Long, Double[]>();
        expectedValues.put(0L, new Double[]{-6.0, 1.0,  1.0, -12.0});
        expectedValues.put(1L, new Double[]{ 0.0, 1.0,  2.0,  1.5});
        expectedValues.put(2L, new Double[]{ 2.0, 1.0, -1.0,  4.0});

        GiraphConfiguration conf = new GiraphConfiguration();
        conf.setComputationClass(GaussianBeliefPropagationComputation.class);
        conf.setMasterComputeClass(GaussianBeliefPropagationMasterCompute.class);
        conf.setAggregatorWriterClass(GaussianBeliefPropagationAggregatorWriter.class);
        conf.setVertexInputFormatClass(JsonPropertyGraph4GBPInputFormat.class);
        conf.setVertexOutputFormatClass(JsonPropertyGraph4GBPOutputFormat.class);
        conf.set("gbp.maxSupersteps", "20");
        conf.set("gbp.convergenceThreshold", "0.001");
        conf.set("gbp.bidirectionalCheck", "true");

        // run internally
        Iterable<String> results = InternalVertexRunner.run(conf, graph);

        Map<Long, Double[]> vertexValues = parseVertexValues(results);

        // verify results
        assertNotNull(vertexValues);
        assertEquals(3, vertexValues.size());
        for (Map.Entry<Long, Double[]> entry : vertexValues.entrySet()) {
            assertEquals(4, entry.getValue().length);
            for (int j = 0; j < 4; j++) {
                assertEquals(expectedValues.get(entry.getKey())[j], entry.getValue()[j], 0.01d);    
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
