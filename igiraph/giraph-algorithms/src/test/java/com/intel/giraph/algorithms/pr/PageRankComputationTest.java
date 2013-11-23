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
package com.intel.giraph.algorithms.pr;


import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import static junit.framework.Assert.assertEquals;

/**
 * Test Page Rank Algorithm
 */
public class PageRankComputationTest {
    /**
     * LOG class
     */
    private static final Logger LOG = Logger
            .getLogger(PageRankComputationTest.class);
    private String[] inputGraph = null;
    private double[] expectedValues = null;
    private GiraphConfiguration giraphConf = null;

    @Before
    public void setUp() throws Exception {
        inputGraph = new String[] {
                "[0,0,[[1,1],[3,3]]]",
                "[1,0,[[0,1],[2,2],[3,1]]]",
                "[2,0,[[1,2],[4,4]]]",
                "[3,0,[[0,3],[1,1],[4,4]]]",
                "[4,0,[[3,4],[2,4]]]"
        };

        expectedValues = new double[]{
                0.16682289373110673,
                0.24178880797750443,
                0.17098446073203238,
                0.24178880797750443,
                0.17098446073203238
        };

        giraphConf = new GiraphConfiguration();
        giraphConf.setComputationClass(PageRankComputation.class);
        giraphConf.setMasterComputeClass(PageRankComputation.PageRankMasterCompute.class);
        giraphConf.setAggregatorWriterClass(PageRankComputation.PageRankAggregatorWriter.class);
        giraphConf.setVertexInputFormatClass(JsonLongDoubleFloatDoubleVertexInputFormat.class);
        giraphConf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
        giraphConf.set("pr.maxSupersteps", "30");
        giraphConf.set("pr.resetProbability", "0.15");
        giraphConf.set("pr.convergenceThreshold", "0.0001");
    }

    //@Ignore
    @Test
    public void PageRankTest() throws Exception {
        // run internally
        Iterable<String> results = InternalVertexRunner.run(giraphConf, inputGraph);
        Assert.assertNotNull(results);
        Iterator<String> result = results.iterator();
        while (result.hasNext()) {
            String resultLine = result.next();
            LOG.info(" got: " + resultLine);
        }

        Map<Long, Double> vertexValues = parseVertexValues(results);
        assertEquals(5, vertexValues.size());
        // check the results with the expected results
        for (Map.Entry<Long, Double> entry : vertexValues.entrySet()) {
            Double vertexValue = entry.getValue();
            assertEquals(expectedValues[(int) (entry.getKey().longValue())], vertexValue.doubleValue(), 0.01d);
        }
    }

    @After
    public void done() throws IOException {
        LOG.info("***Done with PageRankComputationTest****");
    }

    private Map<Long, Double> parseVertexValues(Iterable<String> results) {
        Map<Long, Double> vertexValues = Maps.newHashMapWithExpectedSize(Iterables.size(results));
        for (String line : results) {
            String[] tokens = line.split("\\s+");
            long id = Long.valueOf(tokens[0]);
            double vertexValue = Double.valueOf(tokens[1]);
            vertexValues.put(id, vertexValue);
        }
        return vertexValues;
    }
}
