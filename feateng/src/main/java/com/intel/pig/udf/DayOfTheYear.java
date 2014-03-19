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
package com.intel.pig.udf;


import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * UDF for getting the day of the year from a datetime object
 */
public class DayOfTheYear extends EvalFunc<Integer> {
    /**
     * Get day of the year
     * @param objects datetime object
     * @return day of the year
     * @throws IOException
     */
    @Override
    public Integer exec(Tuple objects) throws IOException {
        if(objects == null || objects.size() != 1 || objects.get(0) == null)
            throw new IOException("Have to pass a single valid DateTime object");

        DateTime date = DateTimeUtils.getDateTime(objects.get(0));
        return date.dayOfYear().get();
    }


    public List<FuncSpec> getArgToFuncMapping() {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        List<Schema.FieldSchema> dateTimefields = new ArrayList<Schema.FieldSchema>();
        dateTimefields.add(new Schema.FieldSchema(null, DataType.DATETIME));

        Schema oneDateTimeArg = new Schema(dateTimefields);
        funcList.add(new FuncSpec(this.getClass().getName(), oneDateTimeArg));

        List<Schema.FieldSchema> byteArrayfields = new ArrayList<Schema.FieldSchema>();
        byteArrayfields.add(new Schema.FieldSchema(null, DataType.BYTEARRAY));
        Schema oneByteArrayArg = new Schema(byteArrayfields);
        funcList.add(new FuncSpec(this.getClass().getName(), oneByteArrayArg));
        return funcList;
    }
}
