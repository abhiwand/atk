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
package com.intel.giraph.io.titan.hbase;

import com.intel.giraph.io.titan.TitanTestBase;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.intel.giraph.algorithms.lp.LabelPropagationComputation;
import com.intel.giraph.io.formats.JsonLongIDVectorValueOutputFormat;
import com.intel.giraph.io.titan.GiraphToTitanGraphFactory;
import com.intel.giraph.io.titan.TitanTestGraph;
import com.intel.mahout.math.DoubleWithVectorWritable;
import com.intel.mahout.math.TwoVectorWritable;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN_AUTOTYPE;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_BACKEND;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_HOSTNAME;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_PORT;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_READ_ONLY;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_TABLENAME;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.INPUT_EDGE_LABEL_LIST;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.INPUT_EDGE_PROPERTY_KEY_LIST;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.INPUT_VERTEX_PROPERTY_KEY_LIST;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

//import org.junit.Ignore;

/**
 * Test TitanHBaseVertexInputFormatLongTwoVectorDoubleTwoVector
 * which loads vertex with <code>long</code> vertex ID's,
 * <code>TwoVector</code> vertex values: one for prior and
 * one for posterior,
 * and <code>DoubleVector</code> edge weights.
 * <p/>
 * This test firstly load a graph to Titan/HBase, then read out the graph from
 * TitanHBaseVertexInputFormat. Then run algorithm with input data.
 */

public class TitanHBaseVertexInputFormatLongTwoVectorDoubleVectorTest
    extends TitanTestBase<LongWritable, TwoVectorWritable, DoubleWithVectorWritable> {

    @Override
    protected void configure() throws Exception {
        giraphConf.setComputationClass(LabelPropagationComputation.class);
        giraphConf.setVertexInputFormatClass(TitanHBaseVertexInputFormatLongTwoVectorDoubleVector.class);
        giraphConf.setVertexOutputFormatClass(JsonLongIDVectorValueOutputFormat.class);
        giraphConf.set("lp.maxSupersteps", "10");

        INPUT_VERTEX_PROPERTY_KEY_LIST.set(giraphConf, "red,blue,yellow");
        INPUT_EDGE_PROPERTY_KEY_LIST.set(giraphConf, "weight");
        INPUT_EDGE_LABEL_LIST.set(giraphConf, "friend");
    }

    @Test
    public void VertexInputFormatLongTwoVectorDoubleVectorTest() throws Exception {
        /* a small four vertex graph
        String[] graph = new String[] {
            "[0,[1,0.1,0.1],[[1,1],[3,3]]]",
            "[1,[0.2,2,2],[[0,1],[2,2],[3,1]]]",
            "[2,[0.3,0.3,3],[[1,2],[4,4]]]",
            "[3,[0.4,4,0.4],[[0,3],[1,1],[4,4]]]",
            "[4,[5,5,0.5],[[3,4],[2,4]]]"
        };
         */

        TitanKey red = tx.makeKey("red").dataType(String.class).make();
        TitanKey blue = tx.makeKey("blue").dataType(String.class).make();
        TitanKey yellow = tx.makeKey("yellow").dataType(String.class).make();
        TitanKey weight = tx.makeKey("weight").dataType(String.class).make();
        TitanLabel friend = tx.makeLabel("friend").make();

        TitanVertex n0 = tx.addVertex();
        n0.addProperty(red, "1");
        n0.addProperty(blue, "0.1");
        n0.addProperty(yellow, "0.1");
        TitanVertex n1 = tx.addVertex();
        n1.addProperty(red, "0.2");
        n1.addProperty(blue, "2");
        n1.addProperty(yellow, "2");
        TitanVertex n2 = tx.addVertex();
        n2.addProperty(red, "0.3");
        n2.addProperty(blue, "0.3");
        n2.addProperty(yellow, "3");
        TitanVertex n3 = tx.addVertex();
        n3.addProperty(red, "0.4");
        n3.addProperty(blue, "4");
        n3.addProperty(yellow, "0.4");
        TitanVertex n4 = tx.addVertex();
        n4.addProperty(red, "5");
        n4.addProperty(blue, "5");
        n4.addProperty(yellow, "0.5");

        TitanEdge e0 = n0.addEdge(friend, n1);
        e0.setProperty(weight, "1.0");
        TitanEdge e1 = n0.addEdge(friend, n3);
        e1.setProperty(weight, "3.0");
        TitanEdge e2 = n1.addEdge(friend, n0);
        e2.setProperty(weight, "1.0");
        TitanEdge e3 = n1.addEdge(friend, n2);
        e3.setProperty(weight, "2.0");
        TitanEdge e4 = n1.addEdge(friend, n3);
        e4.setProperty(weight, "1.0");
        TitanEdge e5 = n2.addEdge(friend, n1);
        e5.setProperty(weight, "2.0");
        TitanEdge e6 = n2.addEdge(friend, n4);
        e6.setProperty(weight, "4.0");
        TitanEdge e7 = n3.addEdge(friend, n0);
        e7.setProperty(weight, "3.0");
        TitanEdge e8 = n3.addEdge(friend, n1);
        e8.setProperty(weight, "1.0");
        TitanEdge e9 = n3.addEdge(friend, n4);
        e9.setProperty(weight, "4.0");
        TitanEdge e10 = n4.addEdge(friend, n3);
        e10.setProperty(weight, "4.0");
        TitanEdge e11 = n4.addEdge(friend, n2);
        e11.setProperty(weight, "4.0");

        tx.commit();


        Iterable<String> results = InternalVertexRunner.run(giraphConf, new String[0], new String[0]);
        Assert.assertNotNull(results);
        Iterator<String> result = results.iterator();
        while (result.hasNext()) {
            String resultLine = result.next();
            LOG.info(" got: " + resultLine);
        }

        Map<Long, Double[]> vertexValues = parseVertexValues(results);
        assertEquals(5, vertexValues.size());
        for (Map.Entry<Long, Double[]> entry : vertexValues.entrySet()) {
            Double[] vertexValue = entry.getValue();
            assertEquals(3, vertexValue.length);
            assertTrue(vertexValue[1].doubleValue() > 0.42d);
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
