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

import com.intel.giraph.utils.LongPair;
import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Simple text-based {@link org.apache.giraph.io.EdgeInputFormat} for
 * unweighted graphs with int ids.
 * <p/>
 * Each line consists of: source_vertex, target_vertex
 */
public class LongNullTextEdgeInputFormat extends
    TextEdgeInputFormat<LongWritable, NullWritable> {
    /**
     * Splitter for endpoints
     */
    private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

    @Override
    public EdgeReader<LongWritable, NullWritable> createEdgeReader(
        InputSplit split, TaskAttemptContext context) throws IOException {
        return new LongNullTextEdgeReader();
    }

    /**
     * {@link org.apache.giraph.io.EdgeReader} associated with
     * {@link LongNullTextEdgeInputFormat}.
     */
    public class LongNullTextEdgeReader extends
        TextEdgeReaderFromEachLineProcessed<LongPair> {
        @Override
        protected LongPair preprocessLine(Text line) throws IOException {
            String[] tokens = SEPARATOR.split(line.toString());
            return new LongPair(Long.valueOf(tokens[0]),
                Long.valueOf(tokens[1]));
        }

        @Override
        protected LongWritable getSourceVertexId(LongPair endpoints)
            throws IOException {
            return new LongWritable(endpoints.getFirst());
        }

        @Override
        protected LongWritable getTargetVertexId(LongPair endpoints)
            throws IOException {
            return new LongWritable(endpoints.getSecond());
        }

        @Override
        protected NullWritable getValue(LongPair endpoints) throws IOException {
            return NullWritable.get();
        }
    }
}
