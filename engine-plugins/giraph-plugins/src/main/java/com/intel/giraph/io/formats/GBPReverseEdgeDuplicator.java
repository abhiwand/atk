/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package com.intel.giraph.io.formats;

import java.io.IOException;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.EdgeReader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import com.intel.giraph.io.EdgeData4GBPWritable;

/**
 * A JsonGBPEdgeReader that creates the opposite direction edge for each edge read.
 * Used to create an undirected graph from a directed input.
 * This class is a decorator around JsonGBPEdgeReader.
 */
public class GBPReverseEdgeDuplicator extends EdgeReader<LongWritable, EdgeData4GBPWritable> {
    /** The underlying EdgeReader to wrap */
    private final EdgeReader<LongWritable, EdgeData4GBPWritable> baseReader;

    /** Whether the reverse edge stored currently is valid */
    private boolean haveReverseEdge = true;
    /** Reverse of the edge last read */
    private Edge<LongWritable, EdgeData4GBPWritable> reverseEdge;
    /** Reverse source of last edge, in other words last edge's target */
    private LongWritable reverseSourceId;

    /**
     * Constructor
     * @param baseReader EdgeReader to wrap
     */
    public GBPReverseEdgeDuplicator(EdgeReader<LongWritable, EdgeData4GBPWritable> baseReader) {
        this.baseReader = baseReader;
    }

    /**
     * Get wrapped EdgeReader
     * @return EdgeReader
     */
    public EdgeReader<LongWritable, EdgeData4GBPWritable> getBaseReader() {
        return baseReader;
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context)
        throws IOException, InterruptedException {
        baseReader.initialize(inputSplit, context);
        haveReverseEdge = true;
    }

    @Override
    public boolean nextEdge() throws IOException, InterruptedException {
        boolean result = true;
        if (haveReverseEdge) {
            result = baseReader.nextEdge();
            haveReverseEdge = false;
        } else {
            Edge<LongWritable, EdgeData4GBPWritable> currentEdge = baseReader.getCurrentEdge();
            reverseSourceId = currentEdge.getTargetVertexId();
            reverseEdge = EdgeFactory.create(baseReader.getCurrentSourceId(),
                currentEdge.getValue().getReverseEdgeData());
            haveReverseEdge = true;
        }
        return result;
    }

    @Override
    public LongWritable getCurrentSourceId() throws IOException, InterruptedException {
        if (haveReverseEdge) {
            return reverseSourceId;
        } else {
            return baseReader.getCurrentSourceId();
        }
    }

    @Override
    public Edge<LongWritable, EdgeData4GBPWritable> getCurrentEdge() throws IOException, InterruptedException {
        if (haveReverseEdge) {
            return reverseEdge;
        } else {
            return baseReader.getCurrentEdge();
        }
    }

    @Override
    public void close() throws IOException {
        baseReader.close();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return baseReader.getProgress();
    }

}
