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
 * Output format for average path length that supports AveragePathLengthComputation
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
     * Simple VertexWriter that supports AveragePathLengthComputation
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
