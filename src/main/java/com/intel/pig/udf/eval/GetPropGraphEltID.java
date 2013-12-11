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
import com.intel.pig.udf.GBUdfExceptionHandler;
import org.apache.pig.EvalFunc;
import org.apache.pig.builtin.MonitoredUDF;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;

/**
 * \brief GetPropGraphEltID ...returns string objectID of its property graph element.
 */
@MonitoredUDF(errorCallback = GBUdfExceptionHandler.class)
public class GetPropGraphEltID extends EvalFunc<String>  {

    /**
     * Get the string representation of the ID of a property graph element
     *
     * @param input tuple containing a property graph element
     * @return string objectID of its property graph element
     * @throws IOException
     */
    @Override
    public String exec(Tuple input) throws IOException {
        Object graphElement =  input.get(0);

        PropertyGraphElement e = (PropertyGraphElement) graphElement;

        String objectId = null;

        if (e.graphElementType() == PropertyGraphElement.GraphElementType.VERTEX) {
            objectId = "VERTEX " + e.vertex().getVertexId().toString();
        } else {
            objectId = "EDGE " + e.edge().getEdgeID().toString();
        }

        return objectId;
    }
}
