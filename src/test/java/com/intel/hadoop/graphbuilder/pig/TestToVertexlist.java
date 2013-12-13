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
import com.intel.hadoop.graphbuilder.types.PropertyMap;
import com.intel.hadoop.graphbuilder.types.StringType;
import com.intel.pig.data.PropertyGraphElementTuple;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestToVertexlist {
    EvalFunc<?> toEdgelistUdf0;
    EvalFunc<?> toEdgelistUdf1;

    @Before
    public void setup() throws Exception {
        System.out.println("*** Starting TO_VERTEXLIST test cases ***");
        toEdgelistUdf0 = (EvalFunc<?>) PigContext
                .instantiateFuncFromSpec("com.intel.pig.udf.eval.TO_VERTEXLIST('false')");
        toEdgelistUdf1 = (EvalFunc<?>) PigContext
                .instantiateFuncFromSpec("com.intel.pig.udf.eval.TO_VERTEXLIST('true')");
    }

    @Test
    public void runTests() throws IOException {
        PropertyGraphElementStringTypeVids graphElement = new PropertyGraphElementStringTypeVids();

        PropertyMap map0 = new PropertyMap();
        map0.setProperty("name", new StringType("Alice"));
        map0.setProperty("age", new StringType("30"));

        Vertex<StringType> vertex = new Vertex<StringType>();
        vertex.configure(
                new StringType("Employee001"),
                map0);
        vertex.setVertexLabel(new StringType("HAWK.People"));

        graphElement.init(PropertyGraphElement.GraphElementType.VERTEX, vertex);

        PropertyGraphElementTuple t = new PropertyGraphElementTuple(1);
        t.set(0, graphElement);

        Tuple result = (Tuple) toEdgelistUdf0.exec(t);
        String statement0 = (String) result.get(0);
        assertEquals(
                    "Vertex tuple mismatch",
                    statement0,
                    "Employee001\tHAWK.People");

        result = (Tuple) toEdgelistUdf1.exec(t);
        String statement1 = (String) result.get(0);
        boolean flag = statement1.contains("Employee001\tHAWK.People");
        assertTrue("Vertex tuple mismatch", flag);
        flag = statement1.contains("name:Alice");
        assertTrue("Vertex tuple mismatch", flag);
        flag = statement1.contains("age:30");
        assertTrue("Vertex tuple mismatch", flag);
    }

    @After
    public void done() {
        System.out.println("*** Done with the TO_EDGELIST tests ***");
    }

}