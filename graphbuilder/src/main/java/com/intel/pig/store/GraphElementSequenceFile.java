/**
 * Copyright (C) 2013 Intel Corporation.
 *     All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * For more about this software visit:
 *     http://www.01.org/GraphBuilder
 */
package com.intel.pig.store;

import com.google.common.base.Preconditions;
import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElementStringTypeVids;
import com.intel.pig.udf.GBUdfException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;

/**
 * Take those property graph element tuples and dump 'em to a sequence file.
 */

public class GraphElementSequenceFile extends StoreFunc {

    private RecordWriter writer = null;

    @Override
    public OutputFormat<NullWritable, SerializedGraphElementStringTypeVids> getOutputFormat() {
        return new SequenceFileOutputFormat<NullWritable, SerializedGraphElementStringTypeVids>();
    }

    @Override
    public void checkSchema(ResourceSchema schema) throws IOException {
        Preconditions.checkNotNull(schema, "Schema is null");
        ResourceFieldSchema[] fields = schema.getFields();
        Preconditions.checkNotNull(fields, "Schema fields are undefined");
        Preconditions.checkArgument(1 == fields.length,
                "Expecting 1 schema field but found %s", fields.length);
    }

    /*
     * Methods called on the backend
     */
    @Override
    public void prepareToWrite(@SuppressWarnings("rawtypes") RecordWriter writer) throws IOException {
        this.writer = writer;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(SerializedGraphElementStringTypeVids.class);
        FileOutputFormat.setOutputPath(job, new Path(location));
    }

    @Override
    public void putNext(Tuple t) throws IOException {

        // validate input tuple

        if (t == null) {
            String msg = "GraphElementSequenceFile: cannot write null tuple";
            throw new IOException(new GBUdfException(msg));
        }

        Object rawValue = t.get(0);

        if (!(rawValue instanceof SerializedGraphElement)) {
            String msg = "GraphElementSequenceFile: given a tuple containing a non- SerializedGraphElement";
            throw new IOException(new IllegalArgumentException(msg));
        }

        SerializedGraphElement value = (SerializedGraphElement) rawValue;

        try {
            writer.write(NullWritable.get(), value);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}