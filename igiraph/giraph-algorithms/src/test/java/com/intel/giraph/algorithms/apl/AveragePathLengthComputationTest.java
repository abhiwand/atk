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
