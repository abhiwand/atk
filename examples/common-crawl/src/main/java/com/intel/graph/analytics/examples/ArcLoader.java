package com.intel.graph.analytics.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class ArcLoader extends LoadFunc {

    private ARCRecordReader reader;
    private TupleFactory tupleFactory = TupleFactory.getInstance();
    private long offset = 0L;

    @Override
    public InputFormat getInputFormat() throws IOException {
        return new WholeFileInputFormat();
    }

    @Override
    public void setLocation(String s, Job job) throws IOException {
        FileInputFormat.setInputPaths(job, s);
    }

    @Override
    public void prepareToRead(RecordReader recordReader, PigSplit pigSplit) throws IOException {
        this.reader = (ARCRecordReader) recordReader;
    }

    @Override
    public Tuple getNext() throws IOException {
        Tuple result;

        try {

            if (!reader.nextKeyValue()) {
                return null;
            }

            result = reader.getCurrentValue();
        } catch (InterruptedException e) {
            throw new IOException(e);
        }

        return result;
    }

    public class WholeFileInputFormat extends FileInputFormat<String, Tuple> {

        @Override
        protected boolean isSplitable(JobContext context, Path filename) {
            return false;
        }

        @Override
        public RecordReader<String, Tuple> createRecordReader(
                InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {

            ARCRecordReader reader = new ARCRecordReader();
            reader.initialize(inputSplit, context);
            return reader;
        }
    }

    public class ARCRecordReader extends RecordReader<String, Tuple> {

        private FileSplit split;
        private Configuration conf;
        private CompressionCodecFactory compressionCodecs = null;

        private String url, timestamp;
        private String contents;
        private boolean fileProcessed = false;
        private String key;
        private long fileOffset = 0L;

        private InputStream in;
        private BufferedReader br;

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context)
                throws IOException, InterruptedException {
            this.split = (FileSplit)split;
            this.conf = context.getConfiguration();

            Configuration job = context.getConfiguration();
            final Path file = ((FileSplit)split).getPath();
            FileSystem fs = file.getFileSystem(conf);
            in = fs.open(file);
            key = file.toString();

            compressionCodecs = new CompressionCodecFactory(conf);
            final CompressionCodec codec = compressionCodecs.getCodec(file);

            if (codec != null) {
                in = codec.createInputStream(in);
            }

            br = new BufferedReader(new InputStreamReader(in, "UTF-8"));
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (fileProcessed) {
                return false;
            }

            int content_length;
            String header;

            // scan header
            do {
                header = br.readLine();
                if (header != null)
                    fileOffset += header.length() + 2; // 2 extra bytes for the CRLF at the end
                else
                {
                    fileProcessed = true;
                    return false;
                }
            } while (!header.matches("^https?://\\S+ \\S+ \\S+ \\S+ \\d+") && br.ready());

            String[] items = header.split("\\s+");
            if (items.length == 5) {
                url = items[0];
                timestamp = items[2];
                content_length = Integer.parseInt(items[4]) - 1;
            }
            else {
                // invalid header, reached the end, quit
                this.fileProcessed = true;
                return false;
            }

            // read content
            contents = "";
            char[] buf = new char[content_length];
            int num_read;
            num_read = br.read(buf, 0, content_length);
            fileOffset += num_read;
            contents = new String(buf);

            if (num_read < content_length)
                this.fileProcessed = true;

            return true;
        }

        @Override
        public String getCurrentKey() throws IOException, InterruptedException {
            return key + ":" + fileOffset;
        }

        @Override
        public Tuple getCurrentValue() throws IOException, InterruptedException {
            Tuple result = tupleFactory.newTuple(3);
            result.set(0, url);
            result.set(1, timestamp);
            result.set(2, contents);
            return result;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return fileProcessed ? 1.0f : 0.0f;
        }

        @Override
        public void close() throws IOException {
            IOUtils.closeStream(in);
        }

    }
}
