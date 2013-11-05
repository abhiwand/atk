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
package com.intel.pig.gb;

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

/**
 * A dummy store function. In practice this should write to some graph DB
 * specified while constructing objects of this class
 */
public class GraphDBStore extends StoreFunc {

	private String targetGraphDB;

	public GraphDBStore(String targetGraphDB) {
		this.targetGraphDB = targetGraphDB;
		System.out.println("Target graph DB " + targetGraphDB);
	}

	@Override
	public OutputFormat getOutputFormat() throws IOException {
		return new MyOutputFormat();
	}

	@Override
	public void setStoreLocation(String location, Job job) throws IOException {
		System.out.println("[GraphDBStore] setStoreLocation " + location);

	}

	@Override
	public void setStoreFuncUDFContextSignature(String signature) {
		System.out.println("[GraphDBStore] setStoreFuncUDFContextSignature "
				+ signature);
	}

	@Override
	public void checkSchema(ResourceSchema s) throws IOException {
		System.out.println("[GraphDBStore] checkSchema " + s);
	}

	@Override
	public void prepareToWrite(RecordWriter writer) throws IOException {
		System.out.println("[GraphDBStore] prepareToWrite " + writer);

	}

	@SuppressWarnings("unchecked")
	public void putNext(Tuple t) throws IOException {
		System.out.println("[GraphDBStore] Writing " + t + " to "
				+ this.targetGraphDB);
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