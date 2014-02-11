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

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;

public class TestCreatePropGraphElements
{
    EvalFunc<?> createPropGraphElementsUDF_0;
    EvalFunc<?> createPropGraphElementsUDF_1;

    @Before
    public void setup() throws Exception {
        System.out.println("*** Starting CreatePropGraphElements tests. ***");
        createPropGraphElementsUDF_0 = (EvalFunc<?>) PigContext.instantiateFuncFromSpec(
                new FuncSpec("com.intel.pig.udf.eval.CreatePropGraphElements",
                "-v name=age,managerId department -e name,department,worksAt,tenure"));
        createPropGraphElementsUDF_1 = (EvalFunc<?>) PigContext.instantiateFuncFromSpec(
                new FuncSpec("com.intel.pig.udf.eval.CreatePropGraphElements",
                        "-v id1=vp0,vp1,vp2,vp3,vp4,vp5,vp6,vp7,vp8,vp9 id2=vp10,vp11,vp12 " +
                        "id3=vp13 id4 id5 id6 " +
                        "-e id1,id2,edgeType0,ep0,ep1,ep2 id1,id3,edgeType1 id2,id3,edgeType2 id1,id6,edgeType3 " +
                        "id4,id5,edgeType4,ep3,ep4 id2,id5,edgeType5,ep4 -x"));
    }

    @Test
    public void runTests() throws IOException, IllegalAccessException {

        Schema.FieldSchema idField         = new Schema.FieldSchema("id", DataType.INTEGER);
        Schema.FieldSchema nameField       = new Schema.FieldSchema("name", DataType.CHARARRAY);
        Schema.FieldSchema ageField        = new Schema.FieldSchema("age", DataType.INTEGER);
        Schema.FieldSchema managerIdField  = new Schema.FieldSchema("managerId", DataType.CHARARRAY);
        Schema.FieldSchema tenureField     = new Schema.FieldSchema("tenure", DataType.CHARARRAY);
        Schema.FieldSchema departmentField = new Schema.FieldSchema("department", DataType.CHARARRAY);

        List fsList = asList(idField,
                             nameField,
                             ageField,
                             managerIdField,
                             tenureField,
                             departmentField);

        Schema schema = new Schema(fsList);

        createPropGraphElementsUDF_0.setInputSchema(schema);

        Tuple t = TupleFactory.getInstance().newTuple(6);

        Integer id = 1;
        String name = "Haywood Y. Buzzov";
        int age = 33;
        String managerId = "Ivanna B. Onatop";
        String tenure =  "Four score and seven years";
        String department = "Overoptimized Commodities";

        t.set(0, id);
        t.set(1, name);
        t.set(2, age);
        t.set(3, managerId);
        t.set(4, tenure);
        t.set(5, department);

        DataBag result = (DataBag) createPropGraphElementsUDF_0.exec(t);

        assert(result.size() == 6);
    }

    @Test
    public void retainDanglingEdges() throws IOException, IllegalAccessException {

        Schema.FieldSchema id1         = new Schema.FieldSchema("id1", DataType.CHARARRAY);
        Schema.FieldSchema id2         = new Schema.FieldSchema("id2", DataType.CHARARRAY);
        Schema.FieldSchema id3         = new Schema.FieldSchema("id3", DataType.CHARARRAY);
        Schema.FieldSchema id4         = new Schema.FieldSchema("id4", DataType.CHARARRAY);
        Schema.FieldSchema id5         = new Schema.FieldSchema("id5", DataType.CHARARRAY);
        Schema.FieldSchema id6         = new Schema.FieldSchema("id6", DataType.CHARARRAY);
        Schema.FieldSchema vp0         = new Schema.FieldSchema("vp0", DataType.CHARARRAY);
        Schema.FieldSchema vp1         = new Schema.FieldSchema("vp1", DataType.CHARARRAY);
        Schema.FieldSchema vp2         = new Schema.FieldSchema("vp2", DataType.CHARARRAY);
        Schema.FieldSchema vp3         = new Schema.FieldSchema("vp3", DataType.CHARARRAY);
        Schema.FieldSchema vp4         = new Schema.FieldSchema("vp4", DataType.CHARARRAY);
        Schema.FieldSchema vp5         = new Schema.FieldSchema("vp5", DataType.CHARARRAY);
        Schema.FieldSchema vp6         = new Schema.FieldSchema("vp6", DataType.CHARARRAY);
        Schema.FieldSchema vp7         = new Schema.FieldSchema("vp7", DataType.CHARARRAY);
        Schema.FieldSchema vp8         = new Schema.FieldSchema("vp8", DataType.CHARARRAY);
        Schema.FieldSchema vp9         = new Schema.FieldSchema("vp9", DataType.CHARARRAY);
        Schema.FieldSchema vp10        = new Schema.FieldSchema("vp10", DataType.CHARARRAY);
        Schema.FieldSchema vp11        = new Schema.FieldSchema("vp11", DataType.CHARARRAY);
        Schema.FieldSchema vp12        = new Schema.FieldSchema("vp12", DataType.CHARARRAY);
        Schema.FieldSchema vp13        = new Schema.FieldSchema("vp13", DataType.CHARARRAY);
        Schema.FieldSchema ep0         = new Schema.FieldSchema("ep0", DataType.CHARARRAY);
        Schema.FieldSchema ep1         = new Schema.FieldSchema("ep1", DataType.CHARARRAY);
        Schema.FieldSchema ep2         = new Schema.FieldSchema("ep2", DataType.CHARARRAY);
        Schema.FieldSchema ep3         = new Schema.FieldSchema("ep3", DataType.CHARARRAY);
        Schema.FieldSchema ep4         = new Schema.FieldSchema("ep4", DataType.CHARARRAY);

        List fsList = asList(id1,id2,id3,id4,id5,id6,vp0,vp1,vp2,vp3,vp4,vp5,vp6,vp7,vp8,vp9,vp10,vp11,vp12,vp13,
                ep0,ep1,ep2,ep3,ep4);

        Schema schema = new Schema(fsList);

        createPropGraphElementsUDF_1.setInputSchema(schema);

        Tuple t = TupleFactory.getInstance().newTuple(26);

        String id1_s  = "vertexId1";
        String id2_s  = "vertexId2";
        String id3_s  = "vertexId3";
        String id4_s  = "vertexId4";
        String id5_s  = "vertexId5";
        String id6_s  = "vertexId6";
        String vp0_s  = "vertexProperty0";
        String vp1_s  = "vertexProperty1";
        String vp2_s  = "vertexProperty2";
        String vp3_s  = "vertexProperty3";
        String vp4_s  = "vertexProperty4";
        String vp5_s  = "vertexProperty5";
        String vp6_s  = "vertexProperty6";
        String vp7_s  = "vertexProperty7";
        String vp8_s  = "vertexProperty8";
        String vp9_s  = "vertexProperty9";
        String vp10_s = "vertexProperty10";
        String vp11_s = "vertexProperty11";
        String vp12_s = "vertexProperty12";
        String vp13_s = "vertexProperty13";
        String ep0_s  = "edgeProperty0";
        String ep1_s  = "edgeProperty1";
        String ep2_s  = "edgeProperty2";
        String ep3_s  = "edgeProperty3";
        String ep4_s  = "edgeProperty4";

        t.set(0, id1_s);
        t.set(1, id2_s);
        t.set(2, id3_s);
        t.set(3, id4_s);
        t.set(4, id5_s);
        t.set(5, id6_s);
        t.set(6, vp0_s);
        t.set(7, vp1_s);
        t.set(8, vp2_s);
        t.set(9, vp3_s);
        t.set(10, vp4_s);
        t.set(11, vp5_s);
        t.set(12, vp6_s);
        t.set(13, vp7_s);
        t.set(14, vp8_s);
        t.set(15, vp9_s);
        t.set(16, vp10_s);
        t.set(17, vp11_s);
        t.set(18, vp12_s);
        t.set(19, vp13_s);
        t.set(20, ep0_s);
        t.set(21, ep1_s);
        t.set(22, ep2_s);
        t.set(23, ep3_s);
        t.set(24, ep4_s);

        DataBag result = (DataBag) createPropGraphElementsUDF_1.exec(t);

        assert(result.size() == 13);
    }

    @After
    public void done() {
        System.out.println("*** Done with the CreatePropGraphElements tests ***");
    }

}