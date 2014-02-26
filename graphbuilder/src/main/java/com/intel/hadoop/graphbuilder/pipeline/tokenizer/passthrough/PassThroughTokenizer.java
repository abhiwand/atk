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

package com.intel.hadoop.graphbuilder.pipeline.tokenizer.passthrough;

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.pipeline.input.BaseMapper;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.GraphTokenizer;
import com.intel.hadoop.graphbuilder.types.StringType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * The passthrough tokenizer that takes serialized graph elements and put them in a vertex list and an edge list.
 */
public class PassThroughTokenizer implements GraphTokenizer<SerializedGraphElement, StringType> {

    private static final Logger LOG = Logger.getLogger(PassThroughTokenizer.class);

    private ArrayList<Vertex<StringType>> vertexList;
    private ArrayList<Edge<StringType>> edgeList;

    /**
     * Allocates and initializes the graph elements stores.
     */
    public PassThroughTokenizer() {
        vertexList = new ArrayList<Vertex<StringType>>();
        edgeList = new ArrayList<Edge<StringType>>();
    }

    /**
     * Configures the tokenizer from the Map Reduce  configuration.
     *
     * @param configuration  The Map Reduce configuration.
     */
    @Override
    public void configure(Configuration configuration) {
    }

    /**
     * The parser simply repacks the graphelements into serialized graphelements... even though
     * they are serialized coming in. Probably goes back to problems with the BaseMapper.
     *
     * TODO: get rid of this entire framework and have a simple mapper that propagates the graph elements with the keys.
     */
    public void parse(SerializedGraphElement value, Mapper.Context context, BaseMapper baseMapper) {
        vertexList.clear();
        edgeList.clear();

        if (value.graphElement().isVertex()) {
            writeVertexToContext((Vertex) value.graphElement(), context, baseMapper);
        } else if (value.graphElement().isEdge()) {
            writeEdgeToContext((Edge) value.graphElement(), context, baseMapper);
        } else {
            LOG.error("Null property graph element encountered in input stream.");
        }
    }

    /**
     * This method is used to emit edges from the HBaseReaderMapper
     * @param edge
     * @param baseMapper
     */
    public void writeEdgeToContext(Edge<StringType> edge, Mapper.Context context, BaseMapper baseMapper) {

        try {
            IntWritable mapKey = baseMapper.getMapKey();
            mapKey.set(baseMapper.getKeyFunction().getEdgeKey(edge));
            SerializedGraphElement mapVal = baseMapper.getMapVal();
            mapVal.init(edge);

            baseMapper.contextWrite(context, mapKey, mapVal);
        } catch (Exception e) {
            context.getCounter(baseMapper.getEdgeWriteErrorCounter()).increment(1);
            LOG.error(e.getMessage(), e);
        }
    }

    /**
     * This method is used to emit vertices from the HBaseReaderMapper
     * @param vertex
     * @param baseMapper
     */
    public void writeVertexToContext(Vertex<StringType> vertex, Mapper.Context context, BaseMapper baseMapper) {

        try {
            IntWritable mapKey = baseMapper.getMapKey();
            mapKey.set(baseMapper.getKeyFunction().getVertexKey(vertex));
            SerializedGraphElement mapVal = baseMapper.getMapVal();
            mapVal.init(vertex);

            baseMapper.contextWrite(context, mapKey, mapVal);
        } catch (NullPointerException e) {
            context.getCounter(baseMapper.getVertexWriteErrorCounter()).increment(1);
            LOG.error(e.getMessage(), e);
        }

    }
}
