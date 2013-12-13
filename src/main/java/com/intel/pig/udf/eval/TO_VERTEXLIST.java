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

import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement.GraphElementType;
import com.intel.pig.udf.GBUdfExceptionHandler;
import org.apache.pig.EvalFunc;
import org.apache.pig.builtin.MonitoredUDF;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;

/**
 * TO_VERTEXLIST UDF intakes property graph elements and spits out
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
public class TO_VERTEXLIST extends EvalFunc<Tuple> {
	private String printProperties;

    /**
     * Constructor of TO_VERTEXLIST
     * @param printProperties Print vertex properties
     *                        if set to "1", "TRUE" or "true",
     *                        does not print vertex properties
     *                        if set to "0", "FALSE", "false"
     */	public TO_VERTEXLIST(String printProperties) {
		this.printProperties = printProperties;
	}

	@Override
	public Tuple exec(Tuple input) throws IOException {
		Tuple tuple = TupleFactory.getInstance().newTuple(1);

		Object graphElementTuple = input.get(0);

		PropertyGraphElement graphElement = (PropertyGraphElement) graphElementTuple;

		// Only print vertices, skip edges
		if (graphElement.graphElementType().equals(GraphElementType.VERTEX)) {
			Vertex vertex = graphElement.vertex();

            String vertexString = new String("");
            if (this.printProperties.equals("1") ||
                    this.printProperties.equals("TRUE") ||
                    this.printProperties.equals("true")) {
			    vertexString = vertex.toString();
            } else if (this.printProperties.equals("0") ||
                    this.printProperties.equals("FALSE") ||
                    this.printProperties.equals("false")) {
                vertexString = vertex.getVertexId().toString();
                if (vertex.getVertexLabel() != null) {
                    vertexString += "\t" + vertex.getVertexLabel().toString();
                }
            }
			tuple.set(0, vertexString);
		}

		return tuple;
	}

	@Override
	public Schema outputSchema(Schema input) {
		try {
			return new Schema(new Schema.FieldSchema("vertex_tuple", DataType.CHARARRAY));
		} catch (Exception e) {
			throw new RuntimeException("Exception while creating output schema for TO_VERTEXLIST udf", e);
		}
	}

}
