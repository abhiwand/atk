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
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestToEdgelist {
    EvalFunc<?> toEdgelistUdf0;
    EvalFunc<?> toEdgelistUdf1;

    @Before
    public void setup() throws Exception {
        System.out.println("*** Starting TO_EDGELIST test cases ***");
        toEdgelistUdf0 = (EvalFunc<?>) PigContext
                .instantiateFuncFromSpec("com.intel.pig.udf.eval.TO_EDGELIST('false')");
        toEdgelistUdf1 = (EvalFunc<?>) PigContext
                .instantiateFuncFromSpec("com.intel.pig.udf.eval.TO_EDGELIST('true')");
    }

    @Test
    public void runTests() throws IOException {
        PropertyGraphElementStringTypeVids graphElement = new PropertyGraphElementStringTypeVids();

        PropertyMap map0 = new PropertyMap();
        map0.setProperty("name", new StringType("Alice"));
        map0.setProperty("age", new StringType("30"));

        Edge<StringType> edge = new Edge<StringType>();
        edge.configure(
                new StringType("Employee001"),
                new StringType("Employee002"),
                new StringType("worksWith"),
                map0);
        graphElement.init(PropertyGraphElement.GraphElementType.EDGE, edge);

        PropertyGraphElementTuple t = new PropertyGraphElementTuple(1);
        t.set(0, graphElement);

        String statement0 = (String) toEdgelistUdf0.exec(t);
        assertEquals(
                "Edge tuple mismatch",
                statement0,
                "Employee001\tEmployee002\tworksWith");

        String statement1 = (String) toEdgelistUdf1.exec(t);

        // property maps write in different orders e.g.
        // Expected :Employee001	Employee002	worksWith	age:30	name:Alice
        // Actual   :Employee001	Employee002	worksWith	name:Alice	age:30
        // Hence, we search for the appropriate edge properties
        boolean flag = statement1.contains("Employee001\tEmployee002\tworksWith");
        assertTrue("Edge tuple mismatch", flag);
        flag = statement1.contains("name:Alice");
        assertTrue("Edge tuple mismatch", flag);
        flag = statement1.contains("age:30");
        assertTrue("Edge tuple mismatch", flag);
    }

    @After
    public void done() {
        System.out.println("*** Done with the TO_EDGELIST tests ***");
    }

}