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

package com.intel.giraph.algorithms.cc;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.intel.giraph.combiner.MinimumLongCombiner;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.ByteArrayEdges;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.io.formats.LongLongNullTextInputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/**
 * Tests for {@link ConnectedComponentsComputation}
 */
public class ConnectedComponentsComputationTest {
    /**
     * LOG class
     */
    private static final Logger LOG = Logger
        .getLogger(ConnectedComponentsComputationTest.class);
    private String[] inputGraph = null;
    private GiraphConfiguration giraphConf = null;
    private Set<Long> expectedComponentIds = new HashSet<Long>() {{
        add((long) 1);
        add((long) 6);
        add((long) 9);
    }};
    private Set<Long> expectedComponent1 = new HashSet<Long>() {{
        add((long) 1);
        add((long) 2);
        add((long) 3);
        add((long) 4);
        add((long) 5);
    }};
    private Set<Long> expectedComponent2 = new HashSet<Long>() {{
        add((long) 6);
        add((long) 7);
        add((long) 8);
    }};
    private Set<Long> expectedComponent3 = new HashSet<Long>() {{
        add((long) 9);
        add((long) 10);
    }};

    @Before
    public void setUp() throws Exception {
        inputGraph = new String[]{
            "1 3 5",
            "2 4 ",
            "3 1 4",
            "4 2 3 5",
            "5 1 4",

            "6 7 8",
            "7 6 8",
            "8 6 7",

            "9 10",
            "10 9"
        };

        giraphConf = new GiraphConfiguration();
        giraphConf.setComputationClass(ConnectedComponentsComputation.class);
        giraphConf.setMasterComputeClass(ConnectedComponentsComputation.
            ConnectedComponentsMasterCompute.class);
        giraphConf.setAggregatorWriterClass(ConnectedComponentsComputation.
            ConnectedComponentsAggregatorWriter.class);
        giraphConf.setOutEdgesClass(ByteArrayEdges.class);
        giraphConf.setMessageCombinerClass(MinimumLongCombiner.class);
        giraphConf.setVertexInputFormatClass(LongLongNullTextInputFormat.class);
        giraphConf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
    }

    @Test
    public void TestConnectedComponents() throws Exception {
        Iterable<String> results = InternalVertexRunner.run(giraphConf, inputGraph);
        Assert.assertNotNull(results);
        for (String resultLine : results) {
            LOG.info(" got: " + resultLine);
        }

        SetMultimap<Long, Long> components = parseResults(results);

        Set<Long> componentIDs = components.keySet();
        assertEquals(3, componentIDs.size());
        assertTrue(componentIDs.equals(expectedComponentIds));

        Set<Long> component1 = components.get((long) 1);
        assertEquals(5, component1.size());
        assertTrue(component1.equals(expectedComponent1));

        Set<Long> component2 = components.get((long) 6);
        assertEquals(3, component2.size());
        assertTrue(component2.equals(expectedComponent2));

        Set<Long> component3 = components.get((long) 9);
        assertEquals(2, component3.size());
        assertTrue(component3.equals(expectedComponent3));
    }

    private SetMultimap<Long, Long> parseResults(Iterable<String> results) {
        SetMultimap<Long, Long> components = HashMultimap.create();
        for (String line : results) {
            String[] tokens = line.split("\\t+");
            long vertex = Long.valueOf(tokens[0]);
            long component = Long.valueOf(tokens[1]);
            components.put(component, vertex);
        }
        return components;
    }

    @After
    public void done() throws IOException {
        LOG.info("***Done with " + getClass().getName() + "****");
    }
}
