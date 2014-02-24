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
package com.intel.pig.udf.store;

import org.apache.hadoop.mapreduce.*;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;

/**
 * This is a dummy store that doesn't do anything.  It was created because loading Titan doesn't fit
 * in nicely with normal Pig use cases.  This dummy store allows us to trigger data flows without
 * creating dummy files.
 * <p>
 * Example Usage:
 *
 *  STORE graph INTO '/tmp/dummy_file_wont_be_created' USING NoOpStore();
 *
 * </p>
 */
public class NoOpStore extends StoreFunc {

    @Override
    public OutputFormat getOutputFormat() throws IOException {

        return new OutputFormat() {
            @Override
            public RecordWriter getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

                return new RecordWriter() {

                    @Override
                    public void write(Object o, Object o2) throws IOException, InterruptedException {
                    }

                    @Override
                    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
                    }
                };
            }

            @Override
            public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
            }

            @Override
            public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

                return new OutputCommitter() {

                    @Override
                    public void setupJob(JobContext jobContext) throws IOException {
                    }

                    @Override
                    public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException {
                    }

                    @Override
                    public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) throws IOException {
                        return true;
                    }

                    @Override
                    public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException {
                    }

                    @Override
                    public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {
                    }
                };
            }
        };
    }

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
    }

    @Override
    public void prepareToWrite(RecordWriter writer) throws IOException {
    }

    @Override
    public void putNext(Tuple t) throws IOException {
    }
}
