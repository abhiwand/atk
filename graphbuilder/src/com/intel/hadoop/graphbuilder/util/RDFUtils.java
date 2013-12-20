/**
 * Copyright (C) 2013 Intel Corporation.
 *     All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * For more about this software visit:
 *     http://www.01.org/GraphBuilder
 */
package com.intel.hadoop.graphbuilder.util;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.vocabulary.*;
import com.intel.hadoop.graphbuilder.types.PropertyMap;
import org.apache.hadoop.io.Writable;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

/**
 * RDFUtils class contains the mappings of user entered namespaces
 * to Apache Jena namespaces. Note that different namespace for
 * each vertex or edge is allowed. This helps in reification of the RDF
 * graph elements
 */
public class RDFUtils {

	public static final Map<String, String> RDFNamespaceMap;
	static {
        RDFNamespaceMap = new Hashtable<String, String>();
        RDFNamespaceMap.put("DB",         DB.getURI());
        RDFNamespaceMap.put("DC",         DC.NS);
        RDFNamespaceMap.put("LOCMAP",     LocationMappingVocab.NS);
        RDFNamespaceMap.put("ONTDOC",     OntDocManagerVocab.NS);
        RDFNamespaceMap.put("ONTEVENTS",  OntDocManagerVocab.NS);
        RDFNamespaceMap.put("OWL",        OWL.NS);
        RDFNamespaceMap.put("OWL2",       OWL2.NS);
        RDFNamespaceMap.put("RDF",        RDF.getURI());
        RDFNamespaceMap.put("RDFS",       RDFS.getURI());
        RDFNamespaceMap.put("RDFSYNTAX",  RDFSyntax.getURI());
        RDFNamespaceMap.put("RSS",        RSS.getURI());
        RDFNamespaceMap.put("VCARD",      VCARD.getURI());
        RDFNamespaceMap.put("XMLSchema",  XSD.getURI());
	}

    public static final ArrayList<String> RDFNamespaces;
    static {
        RDFNamespaces = new ArrayList<String>();
        RDFNamespaces.add("DB");
        RDFNamespaces.add("DC");
        RDFNamespaces.add("LOCMAP");
        RDFNamespaces.add("ONTDOC");
        RDFNamespaces.add("ONTEVENTS");
        RDFNamespaces.add("OWL");
        RDFNamespaces.add("OWL2");
        RDFNamespaces.add("RDF");
        RDFNamespaces.add("RDFS");
        RDFNamespaces.add("RDFSYNTAX");
        RDFNamespaces.add("RSS");
        RDFNamespaces.add("VCARD");
        RDFNamespaces.add("XMLSchema");
    }

    public static boolean isValidNamespace(String namespace) {
        if (RDFNamespaces.contains((String) namespace)) return true;
        else return false;
    }

}
