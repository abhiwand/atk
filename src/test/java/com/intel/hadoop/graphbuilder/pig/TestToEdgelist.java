/* Copyright (C) 2013 Intel Corporation.
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 * For more about this software visit:
 *      http://www.01.org/GraphBuilder
 */
package com.intel.hadoop.graphbuilder.pig;

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElementStringTypeVids;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.types.StringType;
import com.intel.pig.data.PropertyGraphElementTuple;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;

public class TestToEdgelist {
    EvalFunc<?> toEdgelistUdf;

    @Before
    public void setup() throws Exception {
        System.out.println("*** Starting TO_EDGELIST test cases ***");
        toEdgelistUdf = (EvalFunc<?>) PigContext
                .instantiateFuncFromSpec("com.intel.pig.udf.eval.TO_EDGELIST()");
    }

    @Test
    public void runTests() throws IOException {
        PropertyGraphElementStringTypeVids graphElement = new PropertyGraphElementStringTypeVids();
        Vertex<StringType> vertex = new Vertex<StringType>(new StringType("test_vertex"));
        graphElement.init(PropertyGraphElement.GraphElementType.VERTEX, vertex);
        vertex.setProperty("p-1", new StringType("v-1"));
        vertex.setVertexLabel(new StringType("vertex_label"));

        PropertyGraphElementTuple t = new PropertyGraphElementTuple(1);
        t.set(0, graphElement);

        Tuple result = (Tuple) toEdgelistUdf.exec(t);
        String statement = (String) result.get(0);
        assertEquals(
                    "Edgelist mismatch",
                    statement,
                    "test_vertex\tvertex_label\tp-1:v-1\t");

        graphElement = new PropertyGraphElementStringTypeVids();
        Edge<StringType> edge = new Edge<StringType>(new StringType("src"),
                new StringType("target"), new StringType("edge_label"));

        graphElement.init(PropertyGraphElement.GraphElementType.EDGE, edge);
        edge.setProperty("p-1", new StringType("v-1"));

        t = new PropertyGraphElementTuple(1);
        t.set(0, graphElement);

        result = (Tuple) toEdgelistUdf.exec(t);

        statement = (String) result.get(0);
        if (statement.contains("#p-1")) {
            assertEquals(
                        "Edgelist mismatch",
                        statement,
                        "edge_label\tp-1:v-1");
        } else if (statement.contains("#target")) {
            assertEquals(
                        "Edgelist mismatch",
                        statement,
                        "edge_label\ttarget\ttarget");
        } else if (statement.contains("#source")) {
            assertEquals(
                        "Edgelist mismatch",
                        statement,
                        "edge_label\tsource\tsrc");
        }
    }

    @After
    public void done() {
        System.out.println("*** Done with the TO_EDGELIST tests ***");
    }

}