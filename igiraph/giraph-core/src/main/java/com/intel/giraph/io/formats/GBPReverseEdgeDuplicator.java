//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
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
