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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Map;
import java.util.HashMap;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.utils.InternalVertexRunner;
import org.json.JSONArray;
import org.json.JSONException;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.intel.giraph.algorithms.lda.CVB0LDAComputation.CVB0LDAMasterCompute;
import com.intel.giraph.algorithms.lda.CVB0LDAComputation.CVB0LDAAggregatorWriter;
import com.intel.giraph.io.formats.JsonPropertyGraph4LDAInputFormat;
import com.intel.giraph.io.formats.JsonPropertyGraph4LDAOutputFormat;

public class CVB0LDAComputationTest {

    /**
     * A local test on toy data
     */
    @Test
    public void testToyData() throws Exception {
        // a small five-vertex graph
        String[] graph = new String[] {
            "[1,[],[d],[[-1,2,[]],[-3,1,[]]]]",
            "[2,[],[d],[[-1,4,[]],[-3,4,[]]]]",
            "[3,[],[d],[[-2,3,[]]]]",
            "[4,[],[d],[[-2,6,[]]]]",
            "[5,[],[d],[[-4,1,[]],[-5,3,[]]]]",
            "[6,[],[d],[[-4,4,[]],[-5,2,[]]]]",
            "[-1,[],[w],[[1,2,[]],[2,4,[]]]]",
            "[-2,[],[w],[[3,3,[]],[4,6,[]]]]",
            "[-3,[],[w],[[1,1,[]],[2,4,[]]]]",
            "[-4,[],[w],[[5,1,[]],[6,4,[]]]]",
            "[-5,[],[w],[[5,3,[]],[6,2,[]]]]"
        };

        HashMap<Long, Double[]> expectedValues = new HashMap<Long, Double[]>();
        expectedValues.put(1L, new Double[]{0.39540971888836407,0.4400389228117571,0.1645513582998787});
        expectedValues.put(2L, new Double[]{0.5636766826066902,0.3392029513126395,0.09712036608067029});
        expectedValues.put(3L, new Double[]{0.03445607335852895,0.3623027892045044,0.6032411374369667});
        expectedValues.put(4L, new Double[]{0.01934509685453675,0.34691788510704574,0.6337370180384175});
        expectedValues.put(5L, new Double[]{0.8345155568866784,0.07516547903223232,0.0903189640810892});
        expectedValues.put(6L, new Double[]{0.7376090163164662,0.14546014724727488,0.11693083643625882});
        expectedValues.put(-1L, new Double[]{0.1864179107886848,0.3134536847019867,0.10402459427447132});
        expectedValues.put(-2L, new Double[]{0.009445193279079358,0.37335774842426245,0.7039562168192454});
        expectedValues.put(-3L, new Double[]{0.23041662400214913,0.17213628914790263,0.057403553630716905});
        expectedValues.put(-4L, new Double[]{0.26559424214339156,0.09496870724564004,0.07813036764847642});
        expectedValues.put(-5L, new Double[]{0.30812603030574376,0.046083571327991596,0.05648526851858576});
        
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
