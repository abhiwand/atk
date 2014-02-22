package com.intel.graph;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Implementation to get OutputStream from filesystem
 */
public class FileOutputStreamGenerator implements IFileOutputStreamGenerator {
    @Override
    public OutputStream getOutputStream(JobContext context, Path path) throws IOException {
        FileSystem fs = FileSystem.get(context.getConfiguration());
        return fs.create(path, true);
    }
}
