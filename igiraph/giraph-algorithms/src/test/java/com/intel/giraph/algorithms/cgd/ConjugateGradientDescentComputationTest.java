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

package com.intel.giraph.algorithms.cgd;

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
import com.intel.giraph.algorithms.cgd.ConjugateGradientDescentComputation.ConjugateGradientDescentMasterCompute;
import com.intel.giraph.algorithms.cgd.ConjugateGradientDescentComputation.ConjugateGradientDescentAggregatorWriter;
import com.intel.giraph.io.formats.JsonPropertyGraph4CFCGDInputFormat;
import com.intel.giraph.io.formats.JsonPropertyGraph4CFOutputFormat;

public class ConjugateGradientDescentComputationTest {

    /**
     * A local test on toy data
     */
    @Test
    public void testToyData() throws Exception {
        // a small five-vertex graph
        String[] graph = new String[] {
            "[0,[],[L],[[2,1,[tr]],[3,2,[te]]]]",
            "[1,[],[L],[[2,5,[tr]],[4,3,[va]]]]",
            "[2,[],[R],[[0,1,[tr]],[1,5,[tr]]]]",
            "[3,[],[R],[[0,2,[te]]]]",
            "[4,[],[R],[[1,3,[va]]]]"
        };

        double[][] expectedValues = new double[][] {            
            {0.0039651661335228594,0.07930678825437212,0.05799735117092943,0.11802093907151029},
            {0.03703247235199952,0.7427304419460639,0.5430672995740519,0.7102763703821288},
            {0.4150403139989932,2.57968396659953,1.886090358725125,2.2607985402557946},
            {0,0,0,0},
            {0,0,0,0}
        };
        
        GiraphConfiguration conf = new GiraphConfiguration();

        conf.setComputationClass(ConjugateGradientDescentComputation.class);
        conf.setMasterComputeClass(ConjugateGradientDescentMasterCompute.class);
        conf.setAggregatorWriterClass(ConjugateGradientDescentAggregatorWriter.class);
        conf.setVertexInputFormatClass(JsonPropertyGraph4CFCGDInputFormat.class);
        conf.setVertexOutputFormatClass(JsonPropertyGraph4CFOutputFormat.class);
        conf.set("cgd.maxSupersteps", "6");
        conf.set("cgd.featureDimension", "3");
        conf.set("cgd.lambda", "0.05");
        conf.set("cgd.convergenceThreshold", "0");
        conf.set("cgd.minVal", "1");
        conf.set("cgd.maxVal", "5");
        conf.set("cgd.numCGDIters", "5");
        conf.set("cgd.biasOn", "true");

        // run internally
        Iterable<String> results = InternalVertexRunner.run(conf, graph);

        Map<Long, Double[]> vertexValues = parseVertexValues(results);

        // verify results
        assertNotNull(vertexValues);
        assertEquals(5, vertexValues.size());
        for (Map.Entry<Long, Double[]> entry : vertexValues.entrySet()) {
            assertEquals(4, entry.getValue().length);
            for (int j = 0; j < 4; j++) {
                assertEquals(expectedValues[entry.getKey().intValue()][j], entry.getValue()[j], 0.01d);    
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
