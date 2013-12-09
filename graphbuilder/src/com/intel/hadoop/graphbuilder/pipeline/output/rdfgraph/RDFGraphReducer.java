/**
 * Copyright (C) 2012 Intel Corporation.
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

package com.intel.hadoop.graphbuilder.pipeline.output.rdfgraph;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.vocabulary.*;
import com.intel.hadoop.graphbuilder.graphelements.*;
import com.intel.hadoop.graphbuilder.pipeline.mergeduplicates.propertygraphelement.PropertyGraphElements;
import com.intel.hadoop.graphbuilder.pipeline.output.titan.TitanMergedGraphElementWrite;
import com.intel.hadoop.graphbuilder.types.PropertyMap;
import com.intel.hadoop.graphbuilder.types.StringType;
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

public class RDFGraphReducer extends Reducer<IntWritable, SerializedPropertyGraphElement, NullWritable, Text> {

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

    private PropertyGraphElements propertyGraphElements;

    protected static final Map<String, String> RDFNamespaceMap;
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

    protected static final Map<String, Property> RDFTagMap;
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
                    "GRAPHBUILDER_ERROR: Could not instantiate reducer functions", LOG, e);
        } catch (IllegalAccessException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                    "GRAPHBUILDER_ERROR: Illegal access exception when instantiating reducer functions", LOG, e);
        } catch (ClassNotFoundException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                    "GRAPHBUILDER_ERROR: Class not found exception when instantiating reducer functions", LOG, e);
        } catch (Functional.FunctionalConfigurationError e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                    "GRAPHBUILDER_ERROR: Configuration error when configuring reducer functionals.", LOG, e);
        }

        initPropertyGraphElements(context);
    }

    @Override
    public void reduce(IntWritable key, Iterable<SerializedPropertyGraphElement> values, Context context)
            throws IOException, InterruptedException {

        HashMap<EdgeID, Writable>      edgePropertiesMap    = new HashMap();
        HashMap<Object, Writable>      vertexPropertiesMap  = new HashMap();
        HashMap<Object, StringType>    vertexLabelMap       = new HashMap();
        //Iterator<PropertyGraphElement> valueIterator        = values.iterator();

        propertyGraphElements.mergeDuplicates(values);
        propertyGraphElements.write();
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

        for (Writable property : propertyMap.getPropertyKeys()) {
            Property vertexRDFProperty = model.getProperty(namespace + property.toString());
            vertexRdf.addProperty(vertexRDFProperty, propertyMap.getProperty(property.toString()).toString());
        }

        // list the statements in the model
        StmtIterator iterator = model.listStatements();
        // print out the predicate, subject and object of each statement
        while (iterator.hasNext()) {
            Statement stmt      = iterator.nextStatement();         // get next statement
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

        Property sourceRDF = model.getProperty(namespace + "source");
        Property targetRDF = model.getProperty(namespace + "target");
	    edgeRdf.addProperty(sourceRDF, source);
    	edgeRdf.addProperty(targetRDF, target);
        for (Writable property : propertyMap.getPropertyKeys()) {
            Property edgeRDFProperty = model.getProperty(namespace + property.toString());
            edgeRdf.addProperty(edgeRDFProperty, propertyMap.getProperty(property.toString()).toString());
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

    private void initPropertyGraphElements(Context context){
        propertyGraphElements = new PropertyGraphElements(new RDFGraphMergedGraphElementWrite(),vertexReducerFunction,
                edgeReducerFunction, context, null, null, Counters.NUM_EDGES, Counters.NUM_VERTICES);

    }
}
