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
package com.intel.pig.udf.load;

import org.apache.hadoop.mapreduce.*;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This is a dummy load that doesn't do anything.  It was created because loading Titan doesn't fit
 * in nicely with normal Pig use cases.  This dummy load allows us to trigger data flows without
 * creating dummy files.
 * <p>
 * Example Usage:
 *
 * LOAD '/tmp/non_existent_file' USING NoOpLoad();
 *
 * </p>
 */
public class NoOpLoad extends LoadFunc {

    @Override
    public void setLocation(String location, Job job) throws IOException {
    }

    @Override
    public InputFormat getInputFormat() throws IOException {

        return new InputFormat() {

            @Override
            public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
                return new ArrayList<InputSplit>();
            }

            @Override
            public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
                return null;
            }
        };
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {
    }

    @Override
    public Tuple getNext() throws IOException {
        return null;
    }
}
