package com.intel.hadoop.graphbuilder.pipeline.output.rdfgraph;


import com.hp.hpl.jena.rdf.model.*;
import com.intel.hadoop.graphbuilder.graphelements.EdgeID;
import com.intel.hadoop.graphbuilder.graphelements.SerializedPropertyGraphElement;
import com.intel.hadoop.graphbuilder.pipeline.mergeduplicates.MergedGraphElementWrite;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction.KeyFunction;
import com.intel.hadoop.graphbuilder.types.PropertyMap;
import com.intel.hadoop.graphbuilder.types.StringType;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class RDFGraphMergedGraphElementWrite implements MergedGraphElementWrite {
    private String     rdfNamespace;

    HashMap<Object, StringType> vertexLabelMap;

    private MultipleOutputs<NullWritable, Text> multipleOutputs;

    @Override
    public void write(HashMap<EdgeID, Writable> edgeSet, HashMap<Object, Writable> vertexSet,
                      HashMap<Object, StringType> vertexLabelMap,Enum vertexCounter,
                      Enum edgeCounter, Reducer.Context context, TitanGraph graph,
                      SerializedPropertyGraphElement outValue, IntWritable outKey, KeyFunction keyFunction)
            throws IOException, InterruptedException {

        this.multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);

        Configuration conf = context.getConfiguration();

        if (conf.get("rdfNamespace") != null) {
            this.rdfNamespace = conf.get("rdfNamespace");
        }

        this.vertexLabelMap = vertexLabelMap;

        vertexWrite(vertexSet, vertexCounter, context, graph, outValue, outKey, keyFunction);

        edgeWrite(edgeSet, edgeCounter, context, graph, outValue, outKey, keyFunction);
    }

    @Override
    public void vertexWrite(HashMap<Object, Writable> vertexSet, Enum counter, Reducer.Context context, TitanGraph graph,
                            SerializedPropertyGraphElement outValue, IntWritable outKey, KeyFunction keyFunction)
            throws IOException, InterruptedException {

        int vertexCount = 0;
        String outPath = new String("vdata/rdftriples");

        // Output vertex records

        Iterator<Map.Entry<Object, Writable>> vertexIterator = vertexSet.entrySet().iterator();

        for (Map.Entry<Object, Writable> vertex: vertexSet.entrySet()) {
            vertexToRdf(vertex.getKey().toString(), vertexLabelMap.get(vertex.getKey()).toString(), (PropertyMap) vertex.getValue(), outPath);
            vertexCount++;
        }

        context.getCounter(counter).increment(vertexCount);
    }

    /**
     * @param key Vertex key
     * @param propertyMap
     *
     */
    void vertexToRdf(String key, String label, PropertyMap propertyMap, String outPath)
            throws IOException, InterruptedException {

        // Namespace can be DC, DB, RDF, OWL, or OWL2

        String namespace = RDFGraphReducer.RDFNamespaceMap.get(this.rdfNamespace);

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

    @Override
    public void edgeWrite(HashMap<EdgeID, Writable> edgeSet, Enum counter, Reducer.Context context, TitanGraph graph,
                          SerializedPropertyGraphElement outValue, IntWritable outKey, KeyFunction keyFunction)
            throws IOException, InterruptedException {
        int edgeCount   = 0;
        String outPath  = new String("edata/rdftriples");

        // Output edge records

        Iterator<Map.Entry<EdgeID, Writable>> edgeIterator = edgeSet.entrySet().iterator();

        for(Map.Entry<EdgeID, Writable> edge: edgeSet.entrySet()) {

            edgeToRdf(edge.getKey().getSrc().toString(),
                    edge.getKey().getDst().toString(),
                    edge.getKey().getLabel().toString(),
                    (PropertyMap) edge.getValue(),
                    outPath);

            edgeCount++;
        }

        context.getCounter(counter).increment(edgeCount);
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

        String namespace = RDFGraphReducer.RDFNamespaceMap.get(this.rdfNamespace);

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
}
