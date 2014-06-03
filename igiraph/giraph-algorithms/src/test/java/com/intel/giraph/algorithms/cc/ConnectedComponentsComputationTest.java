//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
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
