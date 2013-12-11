package com.intel.pig.udf.eval;

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

import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;
import com.intel.pig.data.GBTupleFactory;
import com.intel.pig.data.PropertyGraphElementTuple;
import com.intel.pig.udf.GBUdfExceptionHandler;
import org.apache.pig.EvalFunc;
import org.apache.pig.builtin.MonitoredUDF;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;

/**
 * \brief GetPropGraphEltID ... prepends tuple with the objectID of the propertygraph element...
 *
 * We could do some hashing to avoid bloat and speed up shuffles, but for now, this is the clearer thing to do.
 */
@MonitoredUDF(errorCallback = GBUdfExceptionHandler.class)
public class MergeDuplicateGraphElements extends EvalFunc<Tuple>  {


    @Override
    public Tuple exec(Tuple input) throws IOException {

        DataBag values = (DataBag) input.get(0);

        PropertyGraphElementTuple t = (PropertyGraphElementTuple) new GBTupleFactory()
                .newTuple(1);



        return t;
    }
}
