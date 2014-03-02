//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2013 Intel Corporation All Rights Reserved.
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

import com.intel.giraph.io.DistanceMapWritable;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Output format for average path length that supports {@link AveragePathLengthComputation}
 * First column: source vertex id
 * Second column: the number of destination vertices
 * Third column: sum of hop counts to all destinations
 */

public class AveragePathLengthComputationOutputFormat extends
        TextVertexOutputFormat<LongWritable, DistanceMapWritable, NullWritable> {
    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context) throws
            IOException, InterruptedException {
        return new AveragePathLengthComputationWriter();
    }

    /**
     * Simple VertexWriter that supports {@link AveragePathLengthComputation}
     */
    public class AveragePathLengthComputationWriter extends TextVertexWriter {
        @Override
        public void writeVertex(Vertex<LongWritable, DistanceMapWritable, NullWritable> vertex) throws
                IOException, InterruptedException {

            String destinationVidStr = vertex.getId().toString();
            HashMap<Long, Integer> distanceMap = vertex.getValue().getDistanceMap();

            long numSources = 0;
            long sumHopCounts = 0;
            for (Map.Entry<Long, Integer> entry : distanceMap.entrySet()) {
                numSources++;
                sumHopCounts += entry.getValue();
            }
            getRecordWriter().write(new Text(destinationVidStr), new Text(numSources + "\t" + sumHopCounts));
        }
    }
}
