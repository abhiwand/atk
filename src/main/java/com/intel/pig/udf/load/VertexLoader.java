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
package com.intel.pig.udf.load;

import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElementStringTypeVids;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.graphelements.serializers.VertexListFormat;
import com.intel.pig.udf.util.BooleanUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.log4j.Logger;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;

/**
 * UDF for loading Vertices written in format of class VertexList
 */
public class VertexLoader extends LoadFunc {

    private static final Logger LOG = Logger.getLogger(VertexLoader.class);

    private RecordReader reader;

    private VertexListFormat vertexListFormat;
    private TupleFactory tupleFactory = TupleFactory.getInstance();

    private boolean withProperties;

    /**
     * UDF for loading Vertices written in format of class VertexList
     * @param withProperties include properties while loading
     *                       Boolean value (String because that is what Pig expects)
     */
    public VertexLoader(String withProperties) {
        this.withProperties = BooleanUtils.toBoolean(withProperties);
        vertexListFormat = new VertexListFormat();
    }

    /**
     * UDF for loading Vertices written in format of class VertexList
     * @param withProperties include properties while loading
     *                       Boolean value (String because that is what Pig expects)
     * @param delimiter specify a different delimiter than the default
     */
    public VertexLoader(String withProperties, String delimiter) {
        this.withProperties = BooleanUtils.toBoolean(withProperties);
        vertexListFormat = new VertexListFormat(delimiter);
    }

    @Override
    public Tuple getNext() throws IOException {

        try {
            if (!reader.nextKeyValue() ) {
                return null;
            }

            Text text = (Text) reader.getCurrentValue();

            Vertex vertex = vertexListFormat.toVertex(text.toString(), withProperties);
            SerializedGraphElementStringTypeVids serializedGraphElement = new SerializedGraphElementStringTypeVids();
            serializedGraphElement.init(vertex);

            return tupleFactory.newTuple(serializedGraphElement);
        }
        catch (InterruptedException e) {
            LOG.error("getNext() interrupted: ", e);
            Thread.currentThread().interrupt();
            throw new IOException("interrupted", e);
        }
    }

    @Override
    public InputFormat getInputFormat() throws IOException {
        return new TextInputFormat();
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {
        this.reader = reader;
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        FileInputFormat.setInputPaths(job, location);
    }
}
