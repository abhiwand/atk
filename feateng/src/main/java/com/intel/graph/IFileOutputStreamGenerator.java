package com.intel.graph;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;

import java.io.IOException;
import java.io.OutputStream;

public interface IFileOutputStreamGenerator {
    OutputStream getOutputStream(JobContext context, Path path) throws IOException;
}
