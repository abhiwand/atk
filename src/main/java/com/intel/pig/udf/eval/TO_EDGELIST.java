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

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement.GraphElementType;
import com.intel.pig.data.PropertyGraphElementTuple;
import com.intel.pig.udf.GBUdfExceptionHandler;
import org.apache.pig.EvalFunc;
import org.apache.pig.builtin.MonitoredUDF;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.util.List;

import static org.apache.pig.data.DataType.BOOLEAN;

/**
 * TO_EDGELIST UDF intakes property graph elements and spits out
 * edge list elements which are tuples of (edge source vertex id,
 * edge target vertex id, label), for example,
 * (Employee001, Employee002, worksWith)
 * If the constructor parameter is set to "TRUE", "true" or "1",
 * the edge list element also contains edge properties returned
 * as key value pair, e.g.
 * (Employee001, Employee002, worksWith, workedFor:11yrs)
 *
 */
@MonitoredUDF(errorCallback = GBUdfExceptionHandler.class)
public class TO_EDGELIST extends EvalFunc<Tuple> {
	private String printProperties;

    /**
     * Constructor of TO_EDGELIST
     * @param printProperties Prints edge properties
     *                        if set to "1", "TRUE" or "true",
     *                        does not print edge properties
     *                        if set to "0", "FALSE", "false"
     */
	public TO_EDGELIST(String printProperties) {
		this.printProperties = printProperties;
	}

	@Override
	public Tuple exec(Tuple input) throws IOException {
		Tuple tuple = TupleFactory.getInstance().newTuple(1);

		Object graphElementTuple = input.get(0);

		PropertyGraphElement graphElement = (PropertyGraphElement) graphElementTuple;

		// Print edges, skip vertices
		if (graphElement.graphElementType().equals(GraphElementType.EDGE)) {
			Edge edge = graphElement.edge();
            String edgeString = "";
            if (this.printProperties.equals("1") ||
                    this.printProperties.equals("TRUE") ||
                    this.printProperties.equals("true")) {
                edgeString = edge.toString();
            } else if (this.printProperties.equals("0") ||
                    this.printProperties.equals("FALSE") ||
                    this.printProperties.equals("false")) {
			    edgeString = edge.getSrc().toString() + "\t" +
                             edge.getDst().toString() + "\t" +
                             edge.getEdgeLabel().toString();
            }
			tuple.set(0, edgeString);
		}

		return tuple;
	}

	@Override
	public Schema outputSchema(Schema input) {
		try {
			return new Schema(new Schema.FieldSchema("edge_tuple", DataType.CHARARRAY));
		} catch (Exception e) {
			throw new RuntimeException("Exception while creating output schema for TO_EDGELIST udf", e);
		}
	}

}
