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
 */package com.intel.pig.udf.eval;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.builtin.MonitoredUDF;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement.GraphElementType;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.util.RDFUtils;
import com.intel.pig.udf.GBUdfExceptionHandler;

/**
 * \brief UDF for converting property graph elements to RDF triples
 * 
 * TORDF UDF converts given property graph elements to RDF triples.
 * 
 * <p/>
 * <b>Example:</b>
 * <p/>
 * Assume that "tshirts.json" file has a record: <br/>
 * { "Name": "T-Shirt 1", "Sizes": [ { "Size": "Large", "Price": 20.50 }, {
 * "Size": "Medium", "Price": 10.00 } ], "Colors": [ "Red", "Green", "Blue" ]} <br/>
 * <br/>
 * Then here is the corresponding Pig script:
 * 
 * <pre>
 * {@code
 * json_data = LOAD 'tutorial/data/tshirts.json' USING TextLoader() AS (json: chararray);
 * extracted_first_tshirts_price = FOREACH json_data GENERATE *, ExtractJSON(json, 'Sizes[0].Price') AS price: double;
 * }
 * </pre>
 */
@MonitoredUDF(errorCallback = GBUdfExceptionHandler.class)
public class TORDF extends EvalFunc<DataBag> {
	private String rdfNamespace;

	public TORDF(String rdfNamespace) {
		this.rdfNamespace = rdfNamespace;
	}

	@Override
	public DataBag exec(Tuple input) throws IOException {
		DataBag rdfBag = DefaultBagFactory.getInstance().newDefaultBag();
		PropertyGraphElement e = (PropertyGraphElement) input.get(0);
		Resource resource = null;

		if (e.graphElementType().equals(GraphElementType.EDGE)) {
			Edge edge = e.edge();
			// create a Resource from the edge
			resource = RDFUtils.createResourceFromEdge(rdfNamespace, edge
					.getSrc().toString(), edge.getDst().toString(), edge
					.getEdgeLabel().get(), edge.getProperties());
		} else if (e.graphElementType().equals(GraphElementType.VERTEX)) {
			Vertex vertex = e.vertex();
			// TODO: what's the key we need to pass here?
			// TODO is vertex.getVertexId() null before we load them to
			// titan?
			// create a Resource from the vertex
			resource = RDFUtils.createResourceFromVertex(rdfNamespace, vertex
					.getVertexId().toString(), vertex.getProperties());
		}

		// list the statements in the model
		StmtIterator iterator = resource.getModel().listStatements();
		// print out the predicate, subject and object of each statement
		while (iterator.hasNext()) {
			Statement stmt = iterator.nextStatement();
			Resource subject = stmt.getSubject();
			Property predicate = stmt.getPredicate();
			RDFNode object = stmt.getObject();

			// Text text = new Text(subject.toString() + " "
			// + predicate.toString() + " " + object.toString() + " .");

			Tuple rdfTuple = TupleFactory.getInstance().newTuple(1);
			String rdfTripleAsString = subject.toString() + " "
					+ predicate.toString() + " " + object.toString() + " .";

			rdfTuple.set(0, rdfTripleAsString);
			rdfBag.add(rdfTuple);
		}
		return rdfBag;
	}

	// @Override
	// public Schema outputSchema(Schema input) {
	// Schema tuple = new Schema();
	// FieldSchema f1 = new FieldSchema("gb_tuple",
	// DataType.GENERIC_WRITABLECOMPARABLE);
	// tuple.add(f1);
	// return tuple;
	// // try {
	// // return new Schema(new Schema.FieldSchema(null, tuple, DataType.BAG));
	// // } catch (Exception e) {
	// // return null;
	// // }
	// }

}
