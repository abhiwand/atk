package com.intel.graph;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;

import java.io.IOException;
import java.io.OutputStream;

/**
 * This interface encapsulates the method to get an OutputStream from
 * job context and path.
 */
public interface IFileOutputStreamGenerator {
    OutputStream getOutputStream(JobContext context, Path path) throws IOException;
}
