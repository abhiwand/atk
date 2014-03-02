package com.intel.giraph.algorithms.apl;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.intel.giraph.io.formats.AveragePathLengthComputationOutputFormat;
import com.intel.giraph.io.formats.LongNullTextEdgeInputFormat;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.utils.InternalVertexRunner;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Unit test for average path length computation.
 */
public class AveragePathLengthComputationTest {
    /**
     * Average path length test with toy data.
     */
    @Test
    public void testToyData() throws Exception {
        /** edge list for test */
        String[] graph = new String[]{
                "0 1",
                "0 3",
                "1 2",
                "1 3",
                "2 0",
                "2 1",
                "2 4",
                "3 4",
                "4 2",
                "4 3"
        };

        GiraphConfiguration conf = new GiraphConfiguration();

        // Configuration.
        conf.setComputationClass(AveragePathLengthComputation.class);
        conf.setMasterComputeClass(AveragePathLengthComputation.AveragePathLengthMasterCompute.class);
        conf.setAggregatorWriterClass(AveragePathLengthComputation.AveragePathLengthAggregatorWriter.class);
        conf.setEdgeInputFormatClass(LongNullTextEdgeInputFormat.class);
        conf.setVertexOutputFormatClass(AveragePathLengthComputationOutputFormat.class);

        // run internally
        Iterable<String> results = InternalVertexRunner.run(conf, null, graph);

        // Pase the results
        Map<Long, Integer[]> hopCountMap = parseResults(results);

        // Setup expected results
        Integer[][] expectedResults = {{4, 8}, {4, 7}, {4, 6}, {4, 5}, {4, 6}};

        // check the results with the expected results
        for (int key = 0; key < hopCountMap.size(); key++) {
            assertTrue(Arrays.equals(hopCountMap.get(Long.valueOf(key)), expectedResults[key]));
        }
    }

    /**
     * @param results String container of output lines.
     * @return Parsed KV pairs stored in Map.
     * @brief Parse the output.
     */
    private Map<Long, Integer[]> parseResults(Iterable<String> results) {
        Map<Long, Integer[]> hopCountResults = Maps.newHashMapWithExpectedSize(Iterables.size(results));
        for (String line : results) {
            Long key;
            Integer[] values = new Integer[2];

            // split
            String[] key_values = line.split("\\s+");

            // make sure line has three values
            assertEquals(key_values.length, 3);

            // get the key and values
            key = Long.parseLong(key_values[0]);
            values[0] = Integer.parseInt(key_values[1]);
            values[1] = Integer.parseInt(key_values[2]);

            // make sure key is unique
            assertFalse(hopCountResults.containsKey(key));

            // add KV to the map
            hopCountResults.put(key, values);
        }
        return hopCountResults;
    }
}
