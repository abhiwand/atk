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

package com.intel.pig.udf.eval;

import com.intel.hadoop.graphbuilder.graphelements.GraphElement;
import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.graphelements.serializers.VertexListFormat;
import com.intel.pig.udf.GBUdfExceptionHandler;
import com.intel.pig.udf.util.BooleanUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.builtin.MonitoredUDF;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;

/**
 * VertexList UDF intakes property graph elements and spits out
 * vertex list elements which are tuples of (vertex id, vertex label).
 * For example,
 * (Employee001, OWL.People)
 * If the constructor parameter is set to "TRUE", "true" or "1",
 * the vertex list element also contains edge properties returned
 * as key value pair, e.g.
 * (Employee002, OWL.People, name:Alice, age:30)
 *
 */
@MonitoredUDF(errorCallback = GBUdfExceptionHandler.class)
public class VertexList extends EvalFunc<String> {

    private boolean printProperties;

    private VertexListFormat vertexListFormat = new VertexListFormat();

    /**
     * Constructor of VertexList
     * @param printProperties Print vertex properties
     *                        if set to "1", "TRUE" or "true",
     *                        does not print vertex properties
     *                        if set to "0", "FALSE", "false"
     */
    public VertexList(String printProperties) {
        this.printProperties = BooleanUtils.toBoolean(printProperties);
	}

	@Override
	public String exec(Tuple input) throws IOException {

		SerializedGraphElement serializedGraphElement = (SerializedGraphElement) input.get(0);
        GraphElement graphElement = serializedGraphElement.graphElement();

        if (graphElement == null) {
            warn("Null property graph element", PigWarning.UDF_WARNING_1);
			return null;
        }

        // Only print vertices, skip edges
		if (graphElement.isVertex()) {
            return vertexListFormat.toString((Vertex) graphElement, printProperties);
		}

		return null;
	}

	/**
	 * VertexList UDF returns a string representation of a vertex property graph
	 * element
	 */
    @Override
    public Schema outputSchema(Schema input) {
        try {
            return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
        } catch (Exception e) {
            throw new RuntimeException("Exception while creating output schema for VertexList udf", e);
        }
    }
}
