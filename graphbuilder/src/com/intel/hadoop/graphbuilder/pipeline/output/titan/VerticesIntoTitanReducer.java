
package com.intel.hadoop.graphbuilder.pipeline.output.titan;

import com.intel.hadoop.graphbuilder.graphelements.EdgeID;
import com.intel.hadoop.graphbuilder.graphelements.SerializedPropertyGraphElement;
import com.intel.hadoop.graphbuilder.pipeline.mergeduplicates.propertygraphelement.PropertyGraphElements;
import com.intel.hadoop.graphbuilder.pipeline.mergeduplicates.propertygraphelement.TitanMergedGraphElementWrite;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction.DestinationVertexKeyFunction;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction.KeyFunction;
import com.intel.hadoop.graphbuilder.util.GraphBuilderExit;
import com.intel.hadoop.graphbuilder.util.GraphDatabaseConnector;
import com.intel.hadoop.graphbuilder.util.StatusCode;
import com.thinkaurelius.titan.core.TitanGraph;

import com.intel.hadoop.graphbuilder.util.Functional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.commons.configuration.BaseConfiguration;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import org.apache.log4j.Logger;

/**
 * This reducer performs the following tasks:
 * - edges are gathered with the source vertices
 * - duplicate edges and vertices are removed
 * - each vertex is loaded into Titan and is tagged with its Titan ID and passed to the next MR job
 *   through the temp file
 * - each edge is tagged with the Titan ID of its source vertex and passed to the next MR job
 */

public class VerticesIntoTitanReducer extends Reducer<IntWritable, SerializedPropertyGraphElement, IntWritable, SerializedPropertyGraphElement> {

    private static final Logger LOG = Logger.getLogger(VerticesIntoTitanReducer.class);

    private boolean    noBiDir;
    private Functional edgeReducerFunction;
    private Functional vertexReducerFunction;
    private TitanGraph graph;

    private HashMap<Object, Long>  vertexNameToTitanID;
    private IntWritable            outKey;
    private SerializedPropertyGraphElement outValue;
    private Class                  outClass;

    private final KeyFunction keyFunction = new DestinationVertexKeyFunction();

    private static enum Counters {
        NUM_VERTICES,
        NUM_EDGES
    }

    private PropertyGraphElements propertyGraphElements;

    /**
     * create the titan graph for saving edges and remove the static open method from setup so it can be mocked
     *
     * @return TitanGraph for saving edges
     * @throws IOException
     */
    protected TitanGraph tribecaGraphFactoryOpen(Context context) throws IOException {
        BaseConfiguration titanConfig = new BaseConfiguration();
        return GraphDatabaseConnector.open("titan", titanConfig, context.getConfiguration());
    }

    @Override
    public void setup(Context context)  throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();

        outClass = context.getMapOutputValueClass();
        outKey   = new IntWritable();

        try {
            outValue   = (SerializedPropertyGraphElement) outClass.newInstance();
        } catch (InstantiationException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                    "Cannot instantiate new reducer output value ( " + outClass.getName() + ")", LOG, e);
        } catch (IllegalAccessException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                    "Illegal access exception when instantiating reducer output value ( " + outClass.getName() + ")",
                    LOG, e);
        }


        this.vertexNameToTitanID = new HashMap<Object, Long>();

        this.graph = tribecaGraphFactoryOpen(context);
        assert (null != this.graph);

        this.noBiDir = conf.getBoolean("noBiDir", false);

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
                    "Functional error configuring reducer function", LOG, e);
        }

        initPropertyGraphElements(context);
    }

    @Override
    public void reduce(IntWritable key, Iterable<SerializedPropertyGraphElement> values, Context context)
            throws IOException, InterruptedException {

        propertyGraphElements.mergeDuplicates(values);
        propertyGraphElements.write();

    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        this.graph.shutdown();
    }

    private void initPropertyGraphElements(Context context){
        propertyGraphElements = new PropertyGraphElements(new TitanMergedGraphElementWrite(),vertexReducerFunction,
                edgeReducerFunction, context, graph, outValue, Counters.NUM_EDGES, Counters.NUM_VERTICES);

    }
}
