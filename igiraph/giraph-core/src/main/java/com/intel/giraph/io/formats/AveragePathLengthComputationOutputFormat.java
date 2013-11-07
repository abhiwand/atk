package com.intel.giraph.io.formats;

import org.apache.giraph.io.formats.TextVertexOutputFormat;
import com.intel.giraph.io.DistanceMapWritable;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
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