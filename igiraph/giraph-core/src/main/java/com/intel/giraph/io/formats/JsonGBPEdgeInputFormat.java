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

import org.apache.giraph.io.EdgeReader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.Text;

import com.intel.giraph.io.EdgeData4GBPWritable;

import org.json.JSONArray;
import org.json.JSONException;

import java.io.IOException;

/**
 * A text-based undirected edge input format for GBP.
 * Each line consists of: [source_vertex, target_vertex, weight, <weight2>]
 * Besides creating a directed edge from source to target with edge
 * value "weight, weight2", it also creates a reverse edge with edge value
 * "weight2, weight1". If weight2 is NOT provided, weight2 will be equal to
 * weight.
 */
public class JsonGBPEdgeInputFormat extends JsonGBPDirectedEdgeInputFormat {

    @Override
    public EdgeReader<LongWritable, EdgeData4GBPWritable> createEdgeReader(
        InputSplit split, TaskAttemptContext context) throws IOException {
        EdgeReader<LongWritable, EdgeData4GBPWritable> edgeReader = super.createEdgeReader(split, context);
        edgeReader.setConf(getConf());
        return new GBPReverseEdgeDuplicator(edgeReader);
    }

}

/**
 * A text-based directed edge input format for GBP.
 * Each line consists of: [source_vertex, target_vertex, weight, <weight2>]
 * It creates a directed edge from source to target with edge value "weight, weight2".
 * If weight2 is NOT provided, weight2 will be equal to weight.
 */
class JsonGBPDirectedEdgeInputFormat extends TextEdgeInputFormat<LongWritable, EdgeData4GBPWritable> {

    @Override
    public EdgeReader<LongWritable, EdgeData4GBPWritable> createEdgeReader(
        InputSplit split, TaskAttemptContext context) throws IOException {
        return new JsonGBPEdgeReader();
    }

    /**
     * An edgeReader to parse json-formatted edge records for GBP.
     */
    public class JsonGBPEdgeReader extends TextEdgeReaderFromEachLineProcessed<JSONArray> {

        @Override
        protected JSONArray preprocessLine(Text line) throws IOException {
            JSONArray jsonEdge = null;
            try {
                jsonEdge = new JSONArray(line.toString());
                if (jsonEdge.length() < 3) {
                    throw new IllegalArgumentException(String.format("Edge record: %s contains less than 3 fields!",
                        line.toString()));
                }
            } catch (JSONException e) {
                throw new IllegalArgumentException("Couldn't parse edge record from line " + line, e);
            }
            return jsonEdge;
        }

        @Override
        protected LongWritable getSourceVertexId(JSONArray jsonEdge) throws IOException {
            LongWritable sid = null;
            try {
                sid = new LongWritable(jsonEdge.getLong(0));
            } catch (JSONException e) {
                throw new IllegalArgumentException("Couldn't parse source id from line " + jsonEdge.toString(), e);
            }
            return sid;
        }

        @Override
        protected LongWritable getTargetVertexId(JSONArray jsonEdge) throws IOException {
            LongWritable tid = null;
            try {
                tid = new LongWritable(jsonEdge.getLong(1));
            } catch (JSONException e) {
                throw new IllegalArgumentException("Couldn't parse target id from line " + jsonEdge.toString(), e);
            }
            return tid;
        }

        @Override
        protected EdgeData4GBPWritable getValue(JSONArray jsonEdge) throws IOException {
            EdgeData4GBPWritable edgeValue = null;
            try {
                double weight = jsonEdge.getDouble(2);
                double reverseWeight = weight;
                if (jsonEdge.length() >= 4) {
                    reverseWeight = jsonEdge.getDouble(3);
                }
                edgeValue = new EdgeData4GBPWritable(weight, reverseWeight);
            } catch (JSONException e) {
                throw new IllegalArgumentException("Couldn't parse edge value from line " + jsonEdge.toString(), e);
            }
            return edgeValue;
        }

    }

}
