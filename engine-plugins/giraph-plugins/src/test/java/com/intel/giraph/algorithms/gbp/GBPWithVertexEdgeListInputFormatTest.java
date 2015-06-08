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
