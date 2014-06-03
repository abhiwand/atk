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
import com.intel.giraph.io.formats.JsonGBPEdgeInputFormat;
import com.intel.giraph.io.formats.JsonGBPVertexValueInputFormat;
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

public class GBPWithVertexEdgeListInputFormatTest {

    /**
     * A local test on toy data
     */
    @Test
    public void testToyData() throws Exception {
        // a small three-vertex graph
        String[] vertices = new String[] {
            "[0,[-6,1]]",
            "[1,[0,1]]",
            "[2,[2,1]]"
        };
        String[] edges = new String[] {
            "[0,1,-2]",
            "[0,2,3]"
        };

        HashMap<Long, Double> expectedValues = new HashMap<Long, Double>();
        expectedValues.put(0L, 1.0);
        expectedValues.put(1L, 2.0);
        expectedValues.put(2L, -1.0);

        GiraphConfiguration conf = new GiraphConfiguration();
        conf.setComputationClass(GaussianBeliefPropagationComputation.class);
        conf.setMasterComputeClass(GaussianBeliefPropagationMasterCompute.class);
        conf.setAggregatorWriterClass(GaussianBeliefPropagationAggregatorWriter.class);
        conf.setVertexInputFormatClass(JsonGBPVertexValueInputFormat.class);
        conf.setEdgeInputFormatClass(JsonGBPEdgeInputFormat.class);
        conf.setVertexOutputFormatClass(JsonPropertyGraph4GBPOutputFormat.class);
        conf.set("gbp.maxSupersteps", "10");
        conf.set("gbp.convergenceThreshold", "0.001");
        conf.set("gbp.bidirectionalCheck", "true");
        conf.set("gbp.outerLoop", "false");

        // run internally
        Iterable<String> results = InternalVertexRunner.run(conf, vertices, edges);

        Map<Long, Double> vertexValues = parseVertexValues(results);
        
        // verify results
        assertNotNull(vertexValues);
        assertEquals(3, vertexValues.size());
        for (Map.Entry<Long, Double> entry : vertexValues.entrySet()) {
            assertEquals(expectedValues.get(entry.getKey()), entry.getValue(), 0.01d);
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
