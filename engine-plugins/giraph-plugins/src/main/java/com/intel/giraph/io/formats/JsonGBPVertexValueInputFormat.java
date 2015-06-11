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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.giraph.io.formats.TextVertexValueInputFormat;

import com.intel.giraph.io.GaussianDistWritable;
import com.intel.giraph.io.VertexData4GBPWritable;

import org.json.JSONArray;
import org.json.JSONException;

import java.io.IOException;

/**
 * A text-based vertex value input format for GBP.
 * Each line consists of a json record: [id, [value1, value2]]
 *
 * @param <E> Edge value
 */
public class JsonGBPVertexValueInputFormat<E extends Writable> extends
    TextVertexValueInputFormat<LongWritable, VertexData4GBPWritable, E> {

    @Override
    public TextVertexValueReader createVertexValueReader(
        InputSplit split, TaskAttemptContext context) {
        return new JsonGBPVertexValueReader();
    }

    /**
    * A VertexValueReader to parse json-formatted vertex values for GBP.
    */
    public class JsonGBPVertexValueReader extends
        TextVertexValueReaderFromEachLineProcessed<JSONArray> {

        @Override
        protected JSONArray preprocessLine(Text line) throws IOException {
            JSONArray jsonVertex = null;
            try {
                jsonVertex = new JSONArray(line.toString());
            } catch (JSONException e) {
                throw new IllegalArgumentException("Couldn't parse vertex record from line: " + line, e);
            }
            return jsonVertex;
        }

        @Override
        protected LongWritable getId(JSONArray jsonVertex) throws IOException {
            LongWritable id = null;
            try {
                id = new LongWritable(jsonVertex.getLong(0));
            } catch (JSONException e) {
                throw new IllegalArgumentException("Couldn't parse vertex id from line " + jsonVertex.toString(), e);
            }
            return id;
        }

        @Override
        protected VertexData4GBPWritable getValue(JSONArray jsonVertex) throws IOException {
            VertexData4GBPWritable vertexData = null;
            try {
                JSONArray vector = jsonVertex.getJSONArray(1);
                if (vector.length() != 2) {
                    throw new IllegalArgumentException("Error in vertex data: mean and precision are needed!");
                }
                GaussianDistWritable prior = new GaussianDistWritable();
                GaussianDistWritable posterior = new GaussianDistWritable();
                prior.setMean(vector.getDouble(0));
                prior.setPrecision(vector.getDouble(1));
                double prevMean = 0d;
                vertexData = new VertexData4GBPWritable(prior, posterior, prior, prevMean);
            } catch (JSONException e) {
                throw new IllegalArgumentException("Couldn't parse vertex value from line " + jsonVertex.toString(), e);
            }
            return vertexData;
        }

    }

}
