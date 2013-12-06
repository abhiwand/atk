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
package com.intel.hadoop.graphbuilder.pipeline.input;

import com.intel.hadoop.graphbuilder.graphelements.SerializedPropertyGraphElement;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction.KeyFunction;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.GraphTokenizer;
import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.util.GraphBuilderExit;
import com.intel.hadoop.graphbuilder.util.StatusCode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;

/**
 * The basic mapper tasks of context.writes of keyed property graph elements are done here.
 *
 * @see com.intel.hadoop.graphbuilder.pipeline.input.hbase.HBaseReaderMapper
 * @see com.intel.hadoop.graphbuilder.pipeline.input.text.TextParsingMapper
 */
public class BaseMapper {

    /**
     * vertex/edge counters. should never be called directly use the getter methods.
     *
     * need to add regular non error counters
     */
    private enum Counters {
        VERTEX_WRITE_ERROR,
        EDGE_WRITE_ERROR,
    }

    private Logger               log;
    private IntWritable          mapKey;
    private SerializedPropertyGraphElement mapVal;
    private Class                valClass;
    private GraphTokenizer       tokenizer;
    private KeyFunction          keyFunction;
    private Mapper.Context       context;
    private Configuration        conf;

    /**
     * An Exception construction will log fatal and cause a system.exit. Their is no point in going forward if we
     * can't initialize the tokenizer class, key function, map val or map key
     *
     * @param context the Mapper.Context for the running mapper
     * @param conf    the current conf for the mapper
     * @param log     the log instance so all logs are attributed to the calling class
     */
    public BaseMapper(Mapper.Context context, Configuration conf, Logger log) {
        this.context = context;
        this.log     = log;
        this.conf    = conf;
        setUp(conf);
    }

    /**
     * Mapper bootstrapping. initialize the tokenizer for parsing the edges and vertices, the key function for getting
     * the context.write key, and initialize mapKey mapValue. InstantiationException, IllegalAccessException, and
     * ClassNotFoundException will all be caught logged and a system exit will be called. no reason continue if we
     * couldn't boot strap.
     *
     * @param conf the mappers current configuration usually context.getConfiguration()
     */
    public void setUp(Configuration conf) {

        initializeTokenizer(conf);
        initializeKeyFunction(conf);
        setValClass(context.getMapOutputValueClass());

        try {
            setMapVal((SerializedPropertyGraphElement) valClass.newInstance());
        } catch (InstantiationException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                    "Cannot instantiate map value class (" + SerializedPropertyGraphElement.class.getName() + " )", log, e);
        } catch (IllegalAccessException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                    "Illegal access exception when instantiating map value class ("
                            + SerializedPropertyGraphElement.class.getName() + " )", log, e);
        }

        setMapKey(new IntWritable());
    }

    /**
     * wrapper method to initialize key function. makes it easier to mock in unit test.
     *
     * @param conf the mappers conf usually context.getConfiguration()
     */

    protected void initializeKeyFunction(Configuration conf) {
        try {
            this.keyFunction = (KeyFunction) Class.forName(conf.get("KeyFunction")).newInstance();
        } catch (ClassNotFoundException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                    "Could not find class named for key function.", log, e);
        } catch (IllegalAccessException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                    "Illegal access exception when instantiating key function.", log, e);
        } catch (InstantiationException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                    "Instantiation exception when instantiating key function.", log, e);
        }
    }

    /**
     * wrapper method to initialize tokenizer. makes it easier to mock in unit test and is general good practice to
     * encapsulate.
     *
     * @param conf the mappers conf usually context.getConfiguration()
     */
    protected void initializeTokenizer(Configuration conf) {
        try {
            this.tokenizer = (GraphTokenizer) Class.forName(conf.get("GraphTokenizer")).newInstance();
        } catch (ClassNotFoundException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                    "Could not find class named for tokenizer.", log, e);
        } catch (IllegalAccessException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                    "Illegal access exception when instantiating tokenizer.", log, e);
        } catch (InstantiationException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                    "Instantiation exception when instantiating tokenizer.", log, e);
        }
        this.tokenizer.configure(conf);
    }

    /**
     * increment the correct error Counter either the Vertex or Edge error counter.
     *
     * @param context the current context for the mapper
     * @param val     the SerializedPropertyGraphElement that through the error
     */
    protected void incrementErrorCounter(Mapper.Context context, SerializedPropertyGraphElement val) {
        if (val.graphElement().isEdge()) {
            context.getCounter(getEdgeWriteErrorCounter()).increment(1);
        } else if (val.graphElement().isVertex()) {
            context.getCounter(getVertexWriteErrorCounter()).increment(1);
        }
    }

    /**
     * Attempt to write the key and value pair. IOException, InterruptedException will be logged and the appropriate
     * edge or vertex counter will be incremented
     *
     * @param context the current mapper context
     * @param key     the vertex/edge key  to write
     * @param val     the property graph element to write either vertex/edge
     */
    protected void contextWrite(Mapper.Context context, IntWritable key, SerializedPropertyGraphElement val) {
        try {
            context.write(key, val);
        } catch (IOException e) {
            incrementErrorCounter(context, val);
            log.error(e.getMessage(), e);
        } catch (InterruptedException e) {
            incrementErrorCounter(context, val);
            log.error(e.getMessage(), e);
        }
    }

    /**
     * iterate through the edge list create Edge graph element get it's key and write. NullPointerException are being
     * captures for the times when edge/vertex has any null values
     *
     * @param context the mappers current context
     */
    public void writeEdges(Mapper.Context context) {
        try {
             Iterator<Edge> edgeIterator = tokenizer.getEdges();

            while (edgeIterator.hasNext()) {

                Edge edge = edgeIterator.next();

                mapVal.init(edge);
                mapKey.set(keyFunction.getEdgeKey(edge));

                contextWrite(context, mapKey, mapVal);
            }
        } catch (NullPointerException e) {
            context.getCounter(getEdgeWriteErrorCounter()).increment(1);
            log.error(e.getMessage(), e);
        }
    }

    /**
     * iterate through the vertex list create a vertex graph element get it's key and write. NullPointerExceptions are
     * being captures for the times when edge/vertex has any null values
     *
     * @param context the mappers current context
     */
    public void writeVertices(Mapper.Context context) {
        try {
            Iterator<Vertex> vertexIterator = tokenizer.getVertices();
            while (vertexIterator.hasNext()) {

                Vertex vertex = vertexIterator.next();

                mapVal.init(vertex);
                mapKey.set(keyFunction.getVertexKey(vertex));

                contextWrite(context, mapKey, mapVal);
            }
        } catch (NullPointerException e) {
            context.getCounter(getVertexWriteErrorCounter()).increment(1);
            log.error(e.getMessage(), e);
        }
    }

    public void setMapKey(IntWritable mapKey) {
        this.mapKey = mapKey;
    }

    public void setMapVal(SerializedPropertyGraphElement mapVal) {
        this.mapVal = mapVal;
    }

    public void setValClass(Class valClass) {
        this.valClass = valClass;
    }

    public GraphTokenizer getTokenizer() {
        return tokenizer;
    }

    /**
     * a getter for the edge write error counter. Will make it easier to change the enum in the future if we need to
     * with  out affecting other code
     *
     * @return Counter
     */
    public static Counters getEdgeWriteErrorCounter() {
        return Counters.EDGE_WRITE_ERROR;
    }

    /**
     * a getter for the vertex write error counter. Will make it easier to change the enum in the future if we need to
     * with out affecting other code
     *
     * @return Counter
     */
    public static Counters getVertexWriteErrorCounter() {
        return Counters.VERTEX_WRITE_ERROR;
    }
}
