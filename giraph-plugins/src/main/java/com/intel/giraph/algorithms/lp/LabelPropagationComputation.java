//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

package com.intel.giraph.algorithms.lp;

import com.intel.giraph.io.VertexData4LPWritable;
import com.intel.giraph.io.IdWithVectorMessage;
import com.intel.ia.giraph.lp.LabelPropagationConfig;
import com.intel.ia.giraph.lp.LabelPropagationConfiguration;
import org.apache.giraph.Algorithm;
import org.apache.giraph.aggregators.AggregatorWriter;
import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.counters.GiraphStats;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.mahout.math.Vector;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

/**
 * Label Propagation on Gaussian Random Fields
 * The algorithm presented in:
 * X. Zhu and Z. Ghahramani. Learning from labeled and unlabeled data with
 * label propagation. Technical Report CMU-CALD-02-107, CMU, 2002.
 */
@Algorithm(
    name = "Label Propagation on Gaussian Random Fields"
)
public class LabelPropagationComputation extends BasicComputation<LongWritable, VertexData4LPWritable,
    DoubleWritable, IdWithVectorMessage> {

    /** Custom argument for number of super steps */
    public static final String MAX_SUPERSTEPS = "lp.maxSupersteps";
    
    /**
     * Custom argument for tradeoff parameter: lambda
     * f = (1-lambda)*Pf + lambda*h
     */
    public static final String LAMBDA = "lp.lambda";
    
    /** Custom argument for the convergence threshold */
    public static final String CONVERGENCE_THRESHOLD = "lp.convergenceThreshold";
    
    /** Aggregator name for sum of cost at each super step */
    private static String SUM_COST = "sum_cost";
    
    /** Cost of previous super step for convergence monitoring */
    private static String PREV_COST = "prev_cost";

    /** Number of super steps */
    private int maxSupersteps;
    
    /** The trade-off parameter between prior and posterior */
    private float lambda;

    @Override
    public void preSuperstep() {
        LabelPropagationConfig config = new LabelPropagationConfiguration(getConf()).getConfig();

        maxSupersteps = getConf().getInt(MAX_SUPERSTEPS, config.maxIterations());
        lambda = getConf().getFloat(LAMBDA, config.lambda());
    }

    /**
     * initialize vertex and edges
     *
     * @param vertex a graph vertex
     */
    private void initializeVertexEdges(Vertex<LongWritable, VertexData4LPWritable, DoubleWritable> vertex) {
        
        // normalize prior and initialize posterior
        VertexData4LPWritable vertexValue = vertex.getValue();
        Vector priorValues = vertexValue.getPriorVector().normalize(1d);
        
        vertexValue.setPriorVector(priorValues);
        vertexValue.setPosteriorVector(priorValues.clone());
        vertexValue.setDegree(initializeEdge(vertex));
        
        // send out messages
        IdWithVectorMessage newMessage = new IdWithVectorMessage(vertex.getId().get(), priorValues);
        sendMessageToAllEdges(vertex, newMessage);
    }

    @Override
    public void compute(Vertex<LongWritable, VertexData4LPWritable, DoubleWritable> vertex,
        Iterable<IdWithVectorMessage> messages) throws IOException {
        long superStep = getSuperstep();
        
        if (superStep == 0) {
            initializeVertexEdges(vertex);
            vertex.voteToHalt();
        }
        else if (superStep <= maxSupersteps) {
            VertexData4LPWritable vertexValue = vertex.getValue();
            Vector prior = vertexValue.getPriorVector();
            Vector posterior = vertexValue.getPosteriorVector();
            double degree = vertexValue.getDegree();

            // collect messages sent to this vertex
            HashMap<Long, Vector> map = new HashMap();
            for (IdWithVectorMessage message : messages) {
                map.put(message.getData(), message.getVector());
            }

            // Update belief and calculate cost
            double hi = prior.getQuick(0);
            double fi = posterior.getQuick(0);
            double crossSum = 0d;
            Vector newBelief = posterior.clone().assign(0d);
            
            for (Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
                double weight = edge.getValue().get();
                long targetVertex = edge.getTargetVertexId().get();
                if (map.containsKey(targetVertex)) {
                    Vector tempVector = map.get(targetVertex);
                    newBelief = newBelief.plus(tempVector.times(weight));
                    double fj = tempVector.getQuick(0);
                    crossSum += weight * fi * fj;
                }
            }
            
            double cost = degree * ((1 - lambda) * (Math.pow(fi,2) - crossSum) + 
                                    0.5 * lambda * Math.pow ((fi - hi),2)
                                   );
            aggregate(SUM_COST, new DoubleWritable(cost));

            // Update posterior if the vertex was not processed
            if (vertexValue.wasLabeled() == false) {
                newBelief = newBelief.times(1 - lambda).plus(prior.times(lambda));
                vertexValue.setPosteriorVector(newBelief);
                vertexValue.markLabeled();
            }

            // Send out messages if not the last step
            if (superStep != maxSupersteps) {
                IdWithVectorMessage newMessage = new IdWithVectorMessage(vertex.getId().get(),
                    vertexValue.getPosteriorVector());
                sendMessageToAllEdges(vertex, newMessage);
            }
        }

        vertex.voteToHalt();
    }

    private double calculateVertexDegree (Vertex<LongWritable, VertexData4LPWritable, DoubleWritable> vertex) {
        double degree = 0d;
        for (Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
            double weight = edge.getValue().get();
            if (weight <= 0d) {
                throw new IllegalArgumentException(
                                "Vertex ID: " + 
                                vertex.getId() +
                                " has an edge with negative or zero weight value.");
            }
            degree += weight;
        }
        return degree;
    }

    private double initializeEdge(Vertex<LongWritable, VertexData4LPWritable, DoubleWritable> vertex) {
        double degree = calculateVertexDegree(vertex);
        degree = (degree == 0) ? 1 : degree;
        
        for (Edge<LongWritable, DoubleWritable> edge : vertex.getMutableEdges()) {
            edge.getValue().set(edge.getValue().get() / degree);
        }

        return degree;
    }

    /**
     * Master compute associated with {@link LabelPropagationComputation}. It registers required aggregators.
     */
    public static class LabelPropagationMasterCompute extends DefaultMasterCompute {

        private LabelPropagationConfig config = null;
        private float convergenceThreshold = 1f;

        @Override
        public void initialize() throws InstantiationException, IllegalAccessException {
            config = new LabelPropagationConfiguration(getConf()).getConfig();
            convergenceThreshold = getConf().getFloat(CONVERGENCE_THRESHOLD, config.convergenceThreshold());

            registerAggregator(SUM_COST, DoubleSumAggregator.class);
        }

        @Override
        public void compute() {
            if (getSuperstep() > 1) {

                float prevCost = getConf().getFloat(PREV_COST, 0f);
                DoubleWritable sumCost = getAggregatedValue(SUM_COST);
                double cost = sumCost.get() / getTotalNumVertices();
                sumCost.set(cost);

                // evaluate convergence condition           
                if (Math.abs(prevCost - cost) < convergenceThreshold) {
                    haltComputation();
                }
                
                getConf().setFloat(PREV_COST, (float) cost);
            }
        }
    }

    /**
     * This is an aggregator writer for label propagation, which after each super step will persist the
     * aggregator values to disk, by use of the Writable interface.
     */
    public static class LabelPropagationAggregatorWriter implements AggregatorWriter {
        /** Name of the file we wrote to */
        private static String filename;
        
        /** Saved output stream to write to */
        private FSDataOutputStream output;
        
        /** Last superstep number */
        private long currentStep = -1L;
        
        /** giraph configuration */
        private ImmutableClassesGiraphConfiguration conf;


        public static String getFilename() {
            return filename;
        }

        @SuppressWarnings("rawtypes")
        @Override
        public void initialize(Context context, long applicationAttempt) throws IOException {
            
            setFilename(applicationAttempt);
            String outputDir = context.getConfiguration().get("mapred.output.dir");
            Path outputPath = new Path(outputDir + File.separator + filename);
            FileSystem fileSystem = FileSystem.get(context.getConfiguration());
            
            if (fileSystem.exists(outputPath)) {
                fileSystem.delete(outputPath, true);
            }
            output = fileSystem.create(outputPath, true);
        }

        /**
         * Set filename written to
         *
         * @param applicationAttempt of type long
         */
        private static void setFilename(long applicationAttempt) {
            filename = "lp-learning-report_" + applicationAttempt;
        }

        @Override
        public void writeAggregator(Iterable<Entry<String, Writable>> aggregatorMap, long superstep)
            throws IOException {

            if (currentStep == 0) {

                output.writeBytes("======Graph Statistics======\n");
                output.writeBytes(String.format("Number of vertices: %d%n",
                    GiraphStats.getInstance().getVertices().getValue()));
                output.writeBytes(String.format("Number of edges: %d%n",
                    GiraphStats.getInstance().getEdges().getValue()));
                output.writeBytes("\n");

                float lambda = getConf().getFloat(LAMBDA, 0f);
                float convergenceThreshold = getConf().getFloat(CONVERGENCE_THRESHOLD, 0.001f);
                int maxSupersteps = getConf().getInt(MAX_SUPERSTEPS, 10);

                output.writeBytes("======LP Configuration======\n");
                output.writeBytes(String.format("lambda: %f%n", lambda));
                output.writeBytes(String.format("convergenceThreshold: %f%n", convergenceThreshold));
                output.writeBytes(String.format("maxSupersteps: %d%n", maxSupersteps));
                output.writeBytes("\n");
                output.writeBytes("======Learning Progress======\n");
            } else if (currentStep > 0) {
                
                // collect aggregator data
                HashMap<String, String> map = new HashMap();
                for (Entry<String, Writable> entry : aggregatorMap) {
                    map.put(entry.getKey(), entry.getValue().toString());
                }
                
                // output learning progress
                output.writeBytes(String.format("superstep = %d%c", currentStep, '\t'));
                double cost = Double.parseDouble(map.get(SUM_COST));
                output.writeBytes(String.format("cost = %f%n", cost));
            }
            output.flush();
            currentStep =  superstep;
        }

        @Override
        public void close() throws IOException {
            output.close();
        }

        /**
         * Set the configuration to be used by this object.
         *
         * @param configuration Set configuration
         */
        @Override
        public void setConf(ImmutableClassesGiraphConfiguration configuration) {
            this.conf = configuration;
        }

        /**
         * Return the configuration used by this object.
         *
         * @return Set configuration
         */
        @Override
        public ImmutableClassesGiraphConfiguration getConf() {
            return conf;
        }
    }
}
