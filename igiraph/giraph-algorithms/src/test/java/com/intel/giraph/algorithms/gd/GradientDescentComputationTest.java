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

package com.intel.giraph.algorithms.gd;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import java.util.Map;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.utils.InternalVertexRunner;
import org.json.JSONArray;
import org.json.JSONException;
import org.junit.Test;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.intel.giraph.algorithms.gd.GradientDescentComputation.GradientDescentMasterCompute;
import com.intel.giraph.algorithms.gd.GradientDescentComputation.SimpleAggregatorWriter;
import com.intel.giraph.io.formats.JsonPropertyGraph4CFInputFormat;
import com.intel.giraph.io.formats.JsonPropertyGraph4CFOutputFormat;

public class GradientDescentComputationTest {

    /**
     * A local test on toy data
     */
    @Test
    public void testToyData() throws Exception {
        // a small five-vertex graph
        String[] graph = new String[] {
            "[0,[],[l],[[2,1,[tr]],[3,2,[te]]]]",
            "[1,[],[l],[[2,5,[tr]],[4,3,[va]]]]",
            "[2,[],[r],[[0,1,[tr]],[1,5,[tr]]]]",
            "[3,[],[r],[[0,2,[te]]]]",
            "[4,[],[r],[[1,3,[va]]]]"
        };

        double[][] expectedValues = new double[][] {
            {-0.3118147381639843,-0.22784199961301865,-0.33892259896838434},
            {-2.2232780229917597,-1.6254371217735497,-1.7482122157302247},
            {0.1682397380639997,0.1212319312700709,-3.173393475437853},
            {0,0,0},
            {0,0,0}
        };
        
        GiraphConfiguration conf = new GiraphConfiguration();

        conf.setComputationClass(GradientDescentComputation.class);
        conf.setMasterComputeClass(GradientDescentMasterCompute.class);
        conf.setAggregatorWriterClass(SimpleAggregatorWriter.class);
        conf.setVertexInputFormatClass(JsonPropertyGraph4CFInputFormat.class);
        conf.setVertexOutputFormatClass(JsonPropertyGraph4CFOutputFormat.class);
        conf.set("gd.maxSupersteps", "6");
        conf.set("gd.featureDimension", "3");
        conf.set("gd.lambda", "0.05");
        conf.set("gd.convergenceThreshold", "0");
        conf.set("gd.learningRate", "0.1");
        conf.set("gd.discount", "0.99");
        conf.set("gd.minVal", "1");
        conf.set("gd.maxVal", "5");

        // run internally
        Iterable<String> results = InternalVertexRunner.run(conf, graph);

        Map<Long, Double[]> vertexValues = parseVertexValues(results);

        // verify results
        assertNotNull(vertexValues);
        assertEquals(5, vertexValues.size());
        for (long i = 0; i < 5; i++) {
            assertEquals(4, vertexValues.get(i).length);
            assertEquals(0.0, vertexValues.get(i)[0], 0d);
            for (int j = 0; j < 3; j++) {
                assertEquals(expectedValues[(int)i][j], vertexValues.get(i)[j+1], 0.01d);    
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
