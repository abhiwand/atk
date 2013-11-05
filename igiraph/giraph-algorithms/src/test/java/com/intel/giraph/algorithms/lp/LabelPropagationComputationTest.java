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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.utils.InternalVertexRunner;
import org.json.JSONArray;
import org.json.JSONException;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.intel.giraph.io.formats.JsonLongIDVectorValueOutputFormat;
import com.intel.giraph.io.formats.JsonLongTwoVectorDoubleVectorInputFormat;

public class LabelPropagationComputationTest {

    /**
     * A local test on toy data
     */
    @Test
    public void testToyData() throws Exception {
        // a small four vertex graph
        String[] graph = new String[] {
            "[0,[1,0.1,0.1],[[1,1],[3,3]]]",
            "[1,[0.2,2,2],[[0,1],[2,2],[3,1]]]",
            "[2,[0.3,0.3,3],[[1,2],[4,4]]]",
            "[3,[0.4,4,0.4],[[0,3],[1,1],[4,4]]]",
            "[4,[5,5,0.5],[[3,4],[2,4]]]"
        };

        GiraphConfiguration conf = new GiraphConfiguration();

        conf.setComputationClass(LabelPropagationComputation.class);
        conf.setVertexInputFormatClass(JsonLongTwoVectorDoubleVectorInputFormat.class);
        conf.setVertexOutputFormatClass(JsonLongIDVectorValueOutputFormat.class);
        conf.set("lp.maxSupersteps", "10");

        // run internally
        Iterable<String> results = InternalVertexRunner.run(conf, graph);

        Map<Long, Double[]> vertexValues = parseVertexValues(results);

        // verify results
        assertNotNull(vertexValues);
        assertEquals(5, vertexValues.size());
        assertEquals(3, vertexValues.get(1L).length);
        for (long i = 0; i < 5; i++) {
            assertTrue(vertexValues.get(i)[1] > 0.42d);
        }
    }
  
    private Map<Long, Double[]> parseVertexValues(Iterable<String> results) {
        Map<Long, Double[]> vertexValues = Maps.newHashMapWithExpectedSize(Iterables.size(results));
        for (String line : results) {
            try {
                JSONArray jsonVertex = new JSONArray(line);
                if (jsonVertex.length() != 2) {
                    throw new IllegalArgumentException("Wrong vertex output format!");
                }
                long id = jsonVertex.getLong(0);
                JSONArray valueArray = jsonVertex.getJSONArray(1);
                if (valueArray.length() != 3) {
                    throw new IllegalArgumentException("Wrong vertex output value format!");
                }
                Double[] values = new Double[3];
                for (int i = 0; i < 3; i++) {
                    values[i] = valueArray.getDouble(i);
                }
                vertexValues.put(id, values);
                } catch (JSONException e) {
                    throw new IllegalArgumentException("Couldn't get vertex from line " + line, e);
                }
        }
        return vertexValues;
    }
}
