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
package com.intel.hadoop.graphbuilder.util;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.vocabulary.*;
import com.intel.hadoop.graphbuilder.types.PropertyMap;
import org.apache.hadoop.io.Writable;

import java.util.HashMap;
import java.util.Map;

public class RDFUtils {

	public static final Map<String, String> RDFNamespaceMap;
	static {
		RDFNamespaceMap = new HashMap<String, String>();
		RDFNamespaceMap.put("OWL", OWL.NS);
		RDFNamespaceMap.put("DC", DC.NS);
		RDFNamespaceMap.put("LOCMAP", LocationMappingVocab.NS);
		RDFNamespaceMap.put("ONTDOC", OntDocManagerVocab.NS);
		RDFNamespaceMap.put("ONTEVENTS", OntDocManagerVocab.NS);
		RDFNamespaceMap.put("OWL2", OWL2.NS);
		RDFNamespaceMap.put("RDFS", RDFS.getURI());

		// TODO We will not support XMLSchema in Graphbuilder2.0
		// RDFNamespaceMap.put("XMLSchema",
		// "http://www.w3.org/2001/XMLSchema#");
	}

	public static final Map<String, Property> RDFTagMap;
	static {
		RDFTagMap = new HashMap<String, Property>();
		RDFTagMap.put("DC.contributor", DC.contributor);
		RDFTagMap.put("DC.coverage", DC.coverage);
		RDFTagMap.put("DC.creator", DC.creator);
		RDFTagMap.put("DC.date", DC.date);
		RDFTagMap.put("DC.description", DC.description);
		RDFTagMap.put("DC.format", DC.format);
		RDFTagMap.put("DC.identifier", DC.identifier);
		RDFTagMap.put("DC.language", DC.language);
		RDFTagMap.put("DC.publisher", DC.publisher);
		RDFTagMap.put("DC.relation", DC.relation);
		RDFTagMap.put("DC.rights", DC.rights);
		RDFTagMap.put("DC.source", DC.source);
		RDFTagMap.put("DC.subject", DC.subject);
		RDFTagMap.put("RDFS.comment", RDFS.comment);
		RDFTagMap.put("RDFS.domain", RDFS.domain);
	}

	public static Resource createResourceFromVertex(String rdfNamespace,
			String vertexKey, PropertyMap vertexPropertyMap) {
		// Namespace can be DC, DB, RDF, OWL, or OWL2
		String namespace = RDFNamespaceMap.get(rdfNamespace);

		// create an empty Model
		Model model = ModelFactory.createDefaultModel();

		// create the resource
		Resource vertexRdf = model.createResource(namespace + vertexKey);

		for (Writable property : vertexPropertyMap.getPropertyKeys()) {
			Property vertexRDFProperty = model.getProperty(namespace
					+ property.toString());
			vertexRdf.addProperty(vertexRDFProperty, vertexPropertyMap
					.getProperty(property.toString()).toString());
		}

		return vertexRdf;
	}

	public static Resource createResourceFromEdge(String rdfNamespace,
			String source, String target, String edgeLabel,
			PropertyMap edgePropertyMap) {
		// Namespace can be DC, DB, RDF, OWL, or OWL2
		String namespace = RDFNamespaceMap.get(rdfNamespace);

		// create an empty Model
		Model model = ModelFactory.createDefaultModel();

		// create the edge resource
		Resource edgeRdf = model.createResource(namespace + edgeLabel);

		Property sourceRDF = model.getProperty(namespace + "source");
		Property targetRDF = model.getProperty(namespace + "target");
		edgeRdf.addProperty(sourceRDF, source);
		edgeRdf.addProperty(targetRDF, target);
		for (Writable property : edgePropertyMap.getPropertyKeys()) {
			Property edgeRDFProperty = model.getProperty(namespace
					+ property.toString());
			edgeRdf.addProperty(edgeRDFProperty,
					edgePropertyMap.getProperty(property.toString()).toString());
		}
		return edgeRdf;
	}
}
