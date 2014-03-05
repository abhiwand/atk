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
package com.intel.hadoop.graphbuilder.pig.rules;


import com.intel.pig.rules.EdgeRule;
import com.intel.pig.udf.util.InputTupleInProgress;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.junit.Test;

import static java.util.Arrays.asList;
import static junit.framework.Assert.*;
import org.apache.pig.data.Tuple;

import java.io.IOException;
import java.util.List;

public class EdgeRuleTest {

    @Test
    public void test_constructor_static_edge_label() {
        EdgeRule edgeRule = new EdgeRule("source", "destination", false, "KNOWS");

        assertEquals(edgeRule.getSrcFieldName(), "source");
        assertEquals(edgeRule.getDstFieldName(), "destination");
        assertNotNull(edgeRule.getPropertyFieldNames());
        assertFalse(edgeRule.isBiDirectional());
        assertEquals(edgeRule.getEdgeLabelType(), EdgeRule.EdgeLabelType.STATIC);
    }

    @Test
    public void test_constructor_dynamic_edge_label() {
        EdgeRule edgeRule = new EdgeRule("source", "destination", false, "dynamic:dynaELType");

        assertEquals(edgeRule.getSrcFieldName(), "source");
        assertEquals(edgeRule.getDstFieldName(), "destination");
        assertNotNull(edgeRule.getPropertyFieldNames());
        assertFalse(edgeRule.isBiDirectional());
        assertEquals(edgeRule.getEdgeLabelType(), EdgeRule.EdgeLabelType.DYNAMIC);
        assertEquals(edgeRule.getEdgeLabelFieldName(), "dynaELType");
    }

    @Test
    public void test_constructor_bidirectional() {
        EdgeRule edgeRule = new EdgeRule("source", "destination", true, "dynamic:dynaELType");

        assertTrue(edgeRule.isBiDirectional());
    }

    @Test
    public void test_determine_edge_label_type_dynamic() {
        String edgeLabel = "dynamic:dynaELType";
        EdgeRule edgeRule = new EdgeRule("source", "destination", false, edgeLabel);

        assertEquals(edgeRule.determineEdgeLabelType(edgeLabel), EdgeRule.EdgeLabelType.DYNAMIC);
    }

    @Test
    public void test_determine_edge_label_type_static() {
        String edgeLabel = "staticELType";
        EdgeRule edgeRule = new EdgeRule("source", "destination", false, edgeLabel);

        assertEquals(edgeRule.determineEdgeLabelType(edgeLabel), EdgeRule.EdgeLabelType.STATIC);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_parse_null_edge_label_rule() {
        String edgeLabel = null;
        EdgeRule edgeRule = new EdgeRule("source", "destination", false, edgeLabel);

        assertNull(edgeRule.getEdgeLabelFieldName());
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_edge_label_without_field_name() {
        String edgeLabel = "dynamic:";
        EdgeRule edgeRule = new EdgeRule("source", "destination", false, edgeLabel);
    }

    @Test
    public void test_get_label() throws IOException {

        Schema.FieldSchema userid     = new Schema.FieldSchema("userid", DataType.CHARARRAY);
        Schema.FieldSchema name       = new Schema.FieldSchema("name", DataType.CHARARRAY);
        Schema.FieldSchema department = new Schema.FieldSchema("department", DataType.CHARARRAY);

        List fsList = asList(userid,name,department);

        Schema schema = new Schema(fsList);

        Tuple tuple = TupleFactory.getInstance().newTuple(4);

        String userid_s     = "001";
        String name_s       = "Alice";
        String department_s = "Sales And Distribution";

        tuple.set(0, userid_s);
        tuple.set(1, name_s);
        tuple.set(2, department_s);

        InputTupleInProgress inputTupleInProgress = new InputTupleInProgress(tuple, schema);

        EdgeRule edgeRule = new EdgeRule("userid", "department", false, "dynamic:department");
        assertEquals(edgeRule.getLabel(inputTupleInProgress), department_s);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_get_label_from_null() throws IOException {

        Schema.FieldSchema userid     = new Schema.FieldSchema("userid", DataType.CHARARRAY);
        Schema.FieldSchema name       = new Schema.FieldSchema("name", DataType.CHARARRAY);
        Schema.FieldSchema department = new Schema.FieldSchema("department", DataType.CHARARRAY);

        List fsList = asList(userid,name,department);

        Schema schema = new Schema(fsList);

        Tuple tuple = TupleFactory.getInstance().newTuple(4);

        String userid_s     = "001";
        String name_s       = "Alice";
        String department_s = "";

        tuple.set(0, userid_s);
        tuple.set(1, name_s);
        //tuple.set(2, department_s);

        InputTupleInProgress inputTupleInProgress = new InputTupleInProgress(tuple, schema);

        EdgeRule edgeRule = new EdgeRule("userid", "department", false, "dynamic:department");
        edgeRule.getLabel(inputTupleInProgress);
    }
}
