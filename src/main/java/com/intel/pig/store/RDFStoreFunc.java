/* Copyright (C) 2013 Intel Corporation.
*     All rights reserved.
*
 *  Licensed under the Apache License, Version 2.0 (the "License");
*  you may not use this file except in compliance with the License.
*  You may obtain a copy of the License at
*
*       http://www.apache.org/licenses/LICENSE-2.0
*
*   Unless required by applicable law or agreed to in writing, software
*   distributed under the License is distributed on an "AS IS" BASIS,
*   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*   See the License for the specific language governing permissions and
*   limitations under the License.
*
* For more about this software visit:
*      http://www.01.org/GraphBuilder
 */
package com.intel.pig.store;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.Tuple;

public class RDFStoreFunc extends StoreFunc {

	private String someArgument;

	public RDFStoreFunc(String targetGraphDB) {
		this.someArgument = targetGraphDB;
		System.out.println("someArgument " + someArgument);
	}

	@Override
	public OutputFormat getOutputFormat() throws IOException {
		return new MyOutputFormat();
	}

	@Override
	public void setStoreLocation(String location, Job job) throws IOException {
		System.out.println("setStoreLocation " + location);

	}

	@Override
	public void setStoreFuncUDFContextSignature(String signature) {
		System.out.println("setStoreFuncUDFContextSignature " + signature);
	}

	@Override
	public void checkSchema(ResourceSchema s) throws IOException {
		System.out.println("checkSchema " + s);
	}

	@Override
	public void prepareToWrite(RecordWriter writer) throws IOException {
		System.out.println("prepareToWrite " + writer);

	}

	@SuppressWarnings("unchecked")
	public void putNext(Tuple t) throws IOException {
		System.out.println("Writing " + t + " to " + this.someArgument);
	}

	class MyOutputFormat extends OutputFormat<NullWritable, NullWritable> {

		@Override
		public void checkOutputSpecs(JobContext context) throws IOException,
				InterruptedException {
		}

		@Override
		public OutputCommitter getOutputCommitter(TaskAttemptContext arg0)
				throws IOException, InterruptedException {
			// need a non-null OutputCommitter
			return new OutputCommitter() {

				@Override
				public void abortTask(TaskAttemptContext arg0)
						throws IOException {
				}

				@Override
				public void commitTask(TaskAttemptContext arg0)
						throws IOException {
				}

				@Override
				public boolean needsTaskCommit(TaskAttemptContext arg0)
						throws IOException {
					return false;
				}

				@Override
				public void setupJob(JobContext arg0) throws IOException {
				}

				@Override
				public void setupTask(TaskAttemptContext arg0)
						throws IOException {
				}
			};
		}

		@Override
		public RecordWriter<NullWritable, NullWritable> getRecordWriter(
				TaskAttemptContext arg0) throws IOException,
				InterruptedException {
			// don't need a record writer to write to graph DB
			// but at least we need a non-null one
			return new RecordWriter<NullWritable, NullWritable>() {
				@Override
				public void close(TaskAttemptContext context) {
				}

				@Override
				public void write(NullWritable k, NullWritable v) {
				}
			};
		}

	}

}