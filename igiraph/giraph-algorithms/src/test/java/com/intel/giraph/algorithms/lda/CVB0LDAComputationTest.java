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

package com.intel.giraph.algorithms.lda;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.intel.giraph.algorithms.lda.CVB0LDAComputation.CVB0LDAAggregatorWriter;
import com.intel.giraph.algorithms.lda.CVB0LDAComputation.CVB0LDAMasterCompute;
import com.intel.giraph.io.formats.JsonPropertyGraph4LDAInputFormat;
import com.intel.giraph.io.formats.JsonPropertyGraph4LDAOutputFormat;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.utils.InternalVertexRunner;
import org.json.JSONArray;
import org.json.JSONException;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class CVB0LDAComputationTest {

    /**
     * A local test on toy data
     */
    @Test
    public void testToyData() throws Exception {
        // a small 11-vertex graph
        String[] graph = new String[] {
            "[1,[],[\"L\"],[[-1,2,[]],[-3,1,[]]]]",
            "[2,[],[\"L\"],[[-1,4,[]],[-3,4,[]]]]",
            "[3,[],[\"L\"],[[-2,3,[]]]]",
            "[4,[],[\"L\"],[[-2,6,[]]]]",
            "[5,[],[\"L\"],[[-4,1,[]],[-5,3,[]]]]",
            "[6,[],[\"L\"],[[-4,4,[]],[-5,2,[]]]]",
            "[-1,[],[\"R\"],[[1,2,[]],[2,4,[]]]]",
            "[-2,[],[\"R\"],[[3,3,[]],[4,6,[]]]]",
            "[-3,[],[\"R\"],[[1,1,[]],[2,4,[]]]]",
            "[-4,[],[\"R\"],[[5,1,[]],[6,4,[]]]]",
            "[-5,[],[\"R\"],[[5,3,[]],[6,2,[]]]]"
        };

        HashMap<Long, Double[]> expectedValues = new HashMap<Long, Double[]>();
        expectedValues.put(1L, new Double[]{0.39327226022569006,0.5154734233615633,0.09125431641274662});
        expectedValues.put(2L, new Double[]{0.5558482319914905,0.3950470162065641,0.04910475180194528});
        expectedValues.put(3L, new Double[]{0.03104958745039701,0.2456944558812926,0.7232559566683103});
        expectedValues.put(4L, new Double[]{0.016228692898330264,0.224904766278556,0.7588665408231138});
        expectedValues.put(5L, new Double[]{0.913307431198414,0.041242684072792385,0.045449884728793674});
        expectedValues.put(6L, new Double[]{0.8864645776362499,0.06273490608963887,0.050800516274111134});
        expectedValues.put(-1L, new Double[]{0.18817778276151342,0.3608563562756368,0.05115513283486491});
        expectedValues.put(-2L, new Double[]{0.0072942875950831095,0.24210489486062656,0.8456708466481648});
        expectedValues.put(-3L, new Double[]{0.22363876113033948,0.20580039507671766,0.03364507449663104});
        expectedValues.put(-4L, new Double[]{0.32297026842503956,0.04200165323881062,0.035281823967909486});
        expectedValues.put(-5L, new Double[]{0.33968467678632336,0.023150094485135348,0.02639742680737546});

        GiraphConfiguration conf = new GiraphConfiguration();

        conf.setComputationClass(CVB0LDAComputation.class);
        conf.setMasterComputeClass(CVB0LDAMasterCompute.class);
        conf.setAggregatorWriterClass(CVB0LDAAggregatorWriter.class);
        conf.setVertexInputFormatClass(JsonPropertyGraph4LDAInputFormat.class);
        conf.setVertexOutputFormatClass(JsonPropertyGraph4LDAOutputFormat.class);
        conf.set("lda.maxSupersteps", "5");
        conf.set("lda.numTopics", "3");
        conf.set("lda.alpha", "0.1");
        conf.set("lda.beta", "0.1");
        conf.set("lda.convergenceThreshold", "0");
        conf.set("lda.evaluateCost", "true");
        conf.set("lda.bidirectionalCheck", "true");

        // run internally
        Iterable<String> results = InternalVertexRunner.run(conf, graph);

        Map<Long, Double[]> vertexValues = parseVertexValues(results);

        // verify results
        assertNotNull(vertexValues);
        assertEquals(11, vertexValues.size());
        for (Map.Entry<Long, Double[]> entry : vertexValues.entrySet()) {
            assertEquals(3, entry.getValue().length);
            for (int j = 0; j < 3; j++) {
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
                // get vertex id
                long id = jsonVertex.getLong(0);
                JSONArray valueArray = jsonVertex.getJSONArray(1);
                if (valueArray.length() != 3) {
                    throw new IllegalArgumentException("Wrong vertex vector output value format!");
                }
                Double[] values = new Double[3];
                for (int i = 0; i < 3; i++) {
                    values[i] = valueArray.getDouble(i);
                }
                vertexValues.put(id, values);
                // get vertex type
                JSONArray typeArray = jsonVertex.getJSONArray(2);
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
