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

package com.intel.giraph.algorithms.pr;


import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.intel.giraph.io.formats.LongNullTextEdgeInputFormat;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;

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
            /*
                "[0,0,[[1,1],[3,3]]]",
                "[1,0,[[0,1],[2,2],[3,1]]]",
                "[2,0,[[1,2],[4,4]]]",
                "[3,0,[[0,3],[1,1],[4,4]]]",
                "[4,0,[[3,4],[2,4]]]"
                */
            "0 1",
            "0 3",
            "1 0",
            "1 2",
            "1 3",
            "2 1",
            "2 4",
            "3 0",
            "3 1",
            "3 4",
            "4 3",
            "4 2"
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
        giraphConf.setEdgeInputFormatClass(LongNullTextEdgeInputFormat.class);
        giraphConf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
        giraphConf.set("pr.maxSupersteps", "30");
        giraphConf.set("pr.resetProbability", "0.15");
        giraphConf.set("pr.convergenceThreshold", "0.0001");
    }

    //@Ignore
    @Test
    public void PageRankTest() throws Exception {
        // run internally
        Iterable<String> results = InternalVertexRunner.run(giraphConf, null, inputGraph);
        Assert.assertNotNull(results);
        for (String resultLine : results) {
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
