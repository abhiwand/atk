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

package com.intel.giraph.algorithms.lp;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.intel.giraph.algorithms.lp.LabelPropagationComputation.LabelPropagationAggregatorWriter;
import com.intel.giraph.algorithms.lp.LabelPropagationComputation.LabelPropagationMasterCompute;
import com.intel.giraph.io.formats.JsonPropertyGraph4LPInputFormat;
import com.intel.giraph.io.formats.JsonPropertyGraph4LPOutputFormat;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.utils.InternalVertexRunner;
import org.json.JSONArray;
import org.json.JSONException;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class LabelPropagationComputationTest {

    /**
     * A local test on toy data
     */
    @Test
    public void testToyData() throws Exception {
        // a small five-vertex graph
        String[] graph = new String[] {
            "[0,[1,0.1,0.1],[],[[1,1,[]],[3,3,[]]]]",
            "[1,[0.2,2,2],[],[[0,1,[]],[2,2,[]],[3,1,[]]]]",
            "[2,[0.3,0.3,3],[],[[1,2,[]],[4,4,[]]]]",
            "[3,[0.4,4,0.4],[],[[0,3,[]],[1,1,[]],[4,4,[]]]]",
            "[4,[5,5,0.5],[],[[3,4,[]],[2,4,[]]]]"
        };

        HashMap<Long, Double[]> expectedValues = new HashMap<Long, Double[]>();
        expectedValues.put(0L, new Double[]{0.833,0.083,0.083,0.833,0.083,0.083});
        expectedValues.put(1L, new Double[]{0.048,0.476,0.476,0.271,0.271,0.458});
        expectedValues.put(2L, new Double[]{0.083,0.083,0.833,0.083,0.083,0.833});
        expectedValues.put(3L, new Double[]{0.083,0.833,0.083,0.083,0.833,0.083});
        expectedValues.put(4L, new Double[]{0.476,0.476,0.048,0.083,0.458,0.458});

        GiraphConfiguration conf = new GiraphConfiguration();

        conf.setComputationClass(LabelPropagationComputation.class);
        conf.setMasterComputeClass(LabelPropagationMasterCompute.class);
        conf.setAggregatorWriterClass(LabelPropagationAggregatorWriter.class);
        conf.setVertexInputFormatClass(JsonPropertyGraph4LPInputFormat.class);
        conf.setVertexOutputFormatClass(JsonPropertyGraph4LPOutputFormat.class);
        conf.set("lp.maxSupersteps", "5");
        conf.set("lp.convergenceThreshold", "0.1");
        conf.set("lp.anchorThreshold", "0.8");
        conf.set("lp.lambda", "0");
        conf.set("lp.bidirectionalCheck", "true");

        // run internally
        Iterable<String> results = InternalVertexRunner.run(conf, graph);

        Map<Long, Double[]> vertexValues = parseVertexValues(results);

        // verify results
        assertNotNull(vertexValues);
        assertEquals(5, vertexValues.size());
        for (Map.Entry<Long, Double[]> entry : vertexValues.entrySet()) {
            assertEquals(6, entry.getValue().length);
            for (int j = 0; j < 6; j++) {
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
                if (priorArray.length() != 3) {
                    throw new IllegalArgumentException("Wrong vertex prior output value format!");
                }
                Double[] values = new Double[6];
                for (int i = 0; i < 3; i++) {
                    values[i] = priorArray.getDouble(i);
                }
                // get posterior
                JSONArray posteriorArray = jsonVertex.getJSONArray(2);
                if (posteriorArray.length() != 3) {
                    throw new IllegalArgumentException("Wrong vertex posterior output value format!");
                }
                for (int i = 3; i < 6; i++) {
                    values[i] = posteriorArray.getDouble(i - 3);
                }
                vertexValues.put(id, values);
                } catch (JSONException e) {
                    throw new IllegalArgumentException("Couldn't get vertex from line " + line, e);
                }
        }
        return vertexValues;
    }

}
