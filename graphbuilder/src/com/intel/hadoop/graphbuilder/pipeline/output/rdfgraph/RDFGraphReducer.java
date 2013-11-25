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

package com.intel.hadoop.graphbuilder.pipeline.output.rdfgraph;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import com.hp.hpl.jena.ontology.OntClass;
import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.ontology.OntProperty;
import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.vocabulary.*;
import com.intel.hadoop.graphbuilder.graphelements.EdgeID;
import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.types.PropertyMap;
import com.intel.hadoop.graphbuilder.util.GraphBuilderExit;
import com.intel.hadoop.graphbuilder.util.StatusCode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import com.intel.hadoop.graphbuilder.util.Functional;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

import org.apache.jena.riot.RDFDataMgr;
import org.openrdf.rio.RDFFormat;

/**
 * The Reducer class applies user defined {@code Functional}s to reduce
 * duplicate edges and vertices. If no such {@code Functional} is provide, it
 * outputs the first instance and discards the rest with the same identifier. It
 * also discards self edges: v - > v. An option for discard bidirectional edge
 * is provided by {@code setCleanBidirectionalEdges(boolean)}.
 * <p>
 * Output directory structure:
 * <ul>
 * <li>$outputdir/edata contains edge data output</li>
 * <li>$outputdir/vdata contains vertex data output</li>
 * </ul>
 * </p>
 */

public class RDFGraphReducer extends Reducer<IntWritable, PropertyGraphElement, NullWritable, Text> {

    private static final Logger LOG = Logger.getLogger(RDFGraphReducer.class);

    private MultipleOutputs<NullWritable, Text> multipleOutputs;

    private boolean    noBiDir;
    private Functional edgeReducerFunction;
    private Functional vertexReducerFunction;
    private String     rdfNamespace;

    private static enum Counters {
        NUM_VERTICES,
        NUM_EDGES
    }

    private static final Map<String, String> RDFNamespaceMap;
    static {
        RDFNamespaceMap = new HashMap<String, String>();
        RDFNamespaceMap.put("OWL",        OWL.NS);
        RDFNamespaceMap.put("DC",         DC.NS);
        RDFNamespaceMap.put("LOCMAP",     LocationMappingVocab.NS);
        RDFNamespaceMap.put("ONTDOC",     OntDocManagerVocab.NS);
        RDFNamespaceMap.put("ONTEVENTS",  OntDocManagerVocab.NS);
        RDFNamespaceMap.put("OWL2",       OWL2.NS);
        RDFNamespaceMap.put("RDFS",       RDFS.getURI());

        // TODO We will not support XMLSchema in Graphbuilder2.0
//        RDFNamespaceMap.put("XMLSchema",  "http://www.w3.org/2001/XMLSchema#");
    }

    @Override
    public void setup(Context context) {

        Configuration conf = context.getConfiguration();

        this.noBiDir = conf.getBoolean("noBiDir", false);

        this.multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);

        try {
            if (conf.get("edgeReducerFunction") != null) {
                this.edgeReducerFunction =
                        (Functional) Class.forName(conf.get("edgeReducerFunction")).newInstance();

                this.edgeReducerFunction.configure(conf);
            }

            if (conf.get("vertexReducerFunction") != null) {
                this.vertexReducerFunction =
                        (Functional) Class.forName(conf.get("vertexReducerFunction")).newInstance();

                this.vertexReducerFunction.configure(conf);
            }

            if (conf.get("rdfNamespace") != null) {
                this.rdfNamespace = conf.get("rdfNamespace");
            }

        } catch (InstantiationException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                    "Could not instantiate reducer functions", LOG, e);
        } catch (IllegalAccessException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                    "Illegal access exception when instantiating reducer functions", LOG, e);
        } catch (ClassNotFoundException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                    "Class not found exception when instantiating reducer functions", LOG, e);
        } catch (Functional.FunctionalConfigurationError e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                    "Configuration error when configuring reducer functionals.", LOG, e);
        }
    }

    @Override
    public void reduce(IntWritable key, Iterable<PropertyGraphElement> values, Context context)
            throws IOException, InterruptedException {

        HashMap<EdgeID, Writable>  edgePropertiesMap   = new HashMap();
        HashMap<Object, Writable> vertexPropertiesMap  = new HashMap();
        Iterator<PropertyGraphElement> valueIterator   = values.iterator();

        while (valueIterator.hasNext()) {

            PropertyGraphElement next = valueIterator.next();

            // Apply reduce on vertex

            if (next.graphElementType() == PropertyGraphElement.GraphElementType.VERTEX) {

                Object vertexId = next.vertex().getVertexId();
                Vertex vertex   = next.vertex();

                if (vertexPropertiesMap.containsKey(vertexId)) {

                    // vertexId denotes a duplicate vertex

                    if (vertexReducerFunction != null) {
                        vertexPropertiesMap.put(vertexId,
                                vertexReducerFunction.reduce(vertex.getProperties(),
                                        vertexPropertiesMap.get(vertexId)));
                    } else {

                        /**
                         * default behavior is to merge the property maps of duplicate vertices
                         * conflicting key/value pairs get overwritten
                         */

                        PropertyMap existingPropertyMap = (PropertyMap) vertexPropertiesMap.get(vertexId);
                        existingPropertyMap.mergeProperties(vertex.getProperties());
                    }
                } else {

                    // vertexId denotes a NON-duplicate vertex

                    if (vertexReducerFunction != null) {
                        vertexPropertiesMap.put(vertexId,
                                                vertexReducerFunction.reduce(vertex.getProperties(),
                                                vertexReducerFunction.identityValue()));
                    } else {
                        vertexPropertiesMap.put(vertexId, vertex.getProperties());
                    }
                }
            } else {

                // Apply reduce on edges, remove self and (or merge) duplicate edges.
                // Optionally remove bidirectional edge.

                Edge<?> edge    = next.edge();
                EdgeID edgeID = new EdgeID(edge.getSrc(), edge.getDst(), edge.getEdgeLabel());

                if (edge.isSelfEdge()) {
                    // self edges are omitted
                    continue;
                }

                if (edgePropertiesMap.containsKey(edgeID)) {

                    // edge is a duplicate
                    // default behavior is to not process the duplicate edge,
                    // but if there is an edge reducer function supplied, it used to combine the edge

                    if (edgeReducerFunction != null) {
                        edgePropertiesMap.put(edgeID,
                                              edgeReducerFunction.reduce(edge.getProperties(),
                                              edgePropertiesMap.get(edgeID)));
                    } else {
                        /**
                         * default behavior is to merge the property maps of duplicate edges
                         * conflicting key/value pairs get overwritten
                         */

                        PropertyMap existingPropertyMap = (PropertyMap) edgePropertiesMap.get(edgeID);
                        existingPropertyMap.mergeProperties(edge.getProperties());

                    }
                } else {

                    // edge is a NON-duplicate

                    if (noBiDir && edgePropertiesMap.containsKey(edgeID.reverseEdge())) {
                        // in this case, skip the bi-directional edge
                    } else {

                        // edge is either not bi-directional, or we are keeping bi-directional edges

                        if (edgeReducerFunction != null) {
                            edgePropertiesMap.put(edgeID,
                                                  edgeReducerFunction.reduce(edge.getProperties(),
                                                  edgeReducerFunction.identityValue()));
                        } else {
                            edgePropertiesMap.put(edgeID, edge.getProperties());
                        }
                    }
                }
            }
        }

        int vertexCount = 0;
        int edgeCount   = 0;
        String outPath  = null;

        // Output vertex records

        Iterator<Entry<Object, Writable>> vertexIterator = vertexPropertiesMap.entrySet().iterator();

        outPath = new String("vdata/rdftriples");

        while (vertexIterator.hasNext()) {

            Entry v     = vertexIterator.next();
            //Text  text = new Text(v.getKey().toString() + "\t" + v.getValue().toString());
            vertexToRdf(v.getKey().toString(), "People", (PropertyMap) v.getValue(), outPath);
            vertexCount++;
        }

        context.getCounter(Counters.NUM_VERTICES).increment(vertexCount);

        // Output edge records

        Iterator<Entry<EdgeID, Writable>> edgeIterator = edgePropertiesMap.entrySet().iterator();

        outPath = new String("edata/rdftriples");

        while (edgeIterator.hasNext()) {

            Entry<EdgeID, Writable> e = edgeIterator.next();

            //Text text = new Text(e.getKey().getSrc() + "\t" + e.getKey().getDst() + "\t" + e.getKey().getLabel()
            //        + "\t" + e.getValue().toString());
            edgeToRdf(e.getKey().getSrc().toString(),
                      e.getKey().getDst().toString(),
                      e.getKey().getLabel().toString(),
                      (PropertyMap) e.getValue(),
		      outPath);

            edgeCount++;
        }

        context.getCounter(Counters.NUM_EDGES).increment(edgeCount);
    }

    /**
     * @param key Vertex key
     * @param propertyMap
     *
     */
    void vertexToRdf(String key, String label, PropertyMap propertyMap, String outPath)
		throws IOException, InterruptedException {

        // Namespace can be DC, DB, RDF, OWL, or OWL2
        String namespace = RDFNamespaceMap.get(this.rdfNamespace);
        // create an empty Model
        Model model = ModelFactory.createDefaultModel();
        // create the vertex resource
        Resource vertexRdf = model.createResource(namespace + key);
        Set<Writable> vpkeys = propertyMap.getPropertyKeys();
        for (Writable property : vpkeys) {
            vertexRdf.addProperty(DC.title, propertyMap.getProperty(property.toString()).toString());
        }

        // list the statements in the model
        StmtIterator iter = model.listStatements();
        // print out the predicate, subject and object of each statement
        while (iter.hasNext()) {
            Statement stmt      = iter.nextStatement();         // get next statement
            Resource  subject   = stmt.getSubject();   // get the subject
            Property  predicate = stmt.getPredicate(); // get the predicate
            RDFNode   object    = stmt.getObject();    // get the object
            Text text = new Text(subject.toString() + " " + predicate.toString() + " " + object.toString() + " .");
            this.multipleOutputs.write(NullWritable.get(), text, outPath);
        }   // End of while
    }

    /**
     * @param source
     * @param target
     * @param label
     * @param propertyMap
     *
     */
    void edgeToRdf(String source, String target, String label, PropertyMap propertyMap, String outPath)
		throws IOException, InterruptedException {

        // Namespace can be DC, DB, RDF, OWL, or OWL2
        String namespace = RDFNamespaceMap.get(this.rdfNamespace);
        // create an empty Model
        Model model = ModelFactory.createDefaultModel();
        // create the edge resource
        Resource edgeRdf = model.createResource(namespace + label);
	edgeRdf.addProperty(DC.title, source);
	edgeRdf.addProperty(DC.title, target);
        Set<Writable> epkeys = propertyMap.getPropertyKeys();
        for (Writable property : epkeys) {
            edgeRdf.addProperty(DC.title, propertyMap.getProperty(property.toString()).toString());
        }

        // list the statements in the model
        StmtIterator iter = model.listStatements();
        // print out the predicate, subject and object of each statement
        while (iter.hasNext()) {
            Statement stmt      = iter.nextStatement();         // get next statement
            Resource  subject   = stmt.getSubject();   // get the subject
            Property  predicate = stmt.getPredicate(); // get the predicate
            RDFNode   object    = stmt.getObject();    // get the object
            Text text = new Text(subject.toString() + " " + predicate.toString() + " " + object.toString() + " .");
            this.multipleOutputs.write(NullWritable.get(), text, outPath);
        }   // End of while
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}
