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
package com.intel.pig.udf.util;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.junit.Test;

import java.util.List;

import static java.util.Arrays.asList;
import static junit.framework.Assert.assertEquals;

public class InputTupleInProgressTest {

    @Test
    public void test_constructor() throws ExecException {
        Schema.FieldSchema userid     = new Schema.FieldSchema("userid", DataType.INTEGER);
        Schema.FieldSchema name       = new Schema.FieldSchema("name", DataType.CHARARRAY);
        Schema.FieldSchema department = new Schema.FieldSchema("department", DataType.CHARARRAY);

        List fsList = asList(userid,name,department);

        Schema schema = new Schema(fsList);

        Tuple tuple = TupleFactory.getInstance().newTuple(4);

        Integer userid_s    = 001;
        String name_s       = "Alice";
        String department_s = "";

        tuple.set(0, userid_s);
        tuple.set(1, name_s);
        //tuple.set(2, department_s);

        InputTupleInProgress inputTupleInProgress = new InputTupleInProgress(tuple, schema);
        assertEquals(inputTupleInProgress.getTuple(), tuple);
        assertEquals(inputTupleInProgress.getInputSchema(), schema);
        assertEquals(inputTupleInProgress.getType("userid"), (Byte) DataType.INTEGER);
        assertEquals(inputTupleInProgress.getType("name"), (Byte) DataType.CHARARRAY);
        assertEquals(inputTupleInProgress.getType("department"), (Byte) DataType.CHARARRAY);
    }
}
