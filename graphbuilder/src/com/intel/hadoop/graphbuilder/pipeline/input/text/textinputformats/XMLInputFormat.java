/**
 * Copyright (C) 2012 Intel Corporation.
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
package com.intel.hadoop.graphbuilder.pipeline.input.text.textinputformats;

import com.intel.hadoop.graphbuilder.util.GraphBuilderExit;
import com.intel.hadoop.graphbuilder.util.StatusCode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.log4j.Logger;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * XMLInputFormat is a builtin InputFormat for XML, borrowed from Cloud9:
 * { @link https://github.com/lintoolCloud9/blob/master/src/dist/edu/umd/cloud9/collection/XMLInputFormat.java }.
 *
 * The class recognizes begin-of-document and end-of-document tags only:
 * everything between those delimiting tags is returned in an uninterpreted Text
 * object.
 */

public class XMLInputFormat extends TextInputFormat {

    private static final Logger LOG = Logger.getLogger(XMLInputFormat.class);

    /**
     * Define start tag of a complete input entry.
     */

    public static final String START_TAG_KEY = "xmlinput.start";

    /**
     * Define end tag of a complete input entry.
     */

    public static final String END_TAG_KEY = "xmlinput.end";

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext context) {

        XMLRecordReader xmlRecordReader = null;

        try {
            xmlRecordReader = new XMLRecordReader(context);
            xmlRecordReader.initialize(inputSplit, context);
        }
        catch (IOException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.UNHANDLED_IO_EXCEPTION,
                    "Unable to create xml record reader.", LOG, e);
        }

        return xmlRecordReader;
    }

    /**
     * RecordReader for XML documents Recognizes begin-of-document and
     * end-of-document tags only: Returning text object of everything in between
     * delimiters
     */

    public static class XMLRecordReader extends RecordReader<LongWritable, Text> {

        private static final Logger LOG = Logger.getLogger(XMLRecordReader.class);

        private Configuration conf;

        private byte[] startTag;
        private byte[] endTag;
        private long   start;
        private long   end;
        private long   pos;
        private long   recordStartPos;

        private Text         value = new Text();
        private LongWritable key   = new LongWritable();

        private DataInputStream fsIn   = null;
        private DataOutputBuffer buffer = new DataOutputBuffer();
        private FileSplit fileSplit;

        private static final String TEXT_ENCODING = "utf-8";

        public XMLRecordReader(TaskAttemptContext context) throws IOException {

            this.conf = context.getConfiguration();

            if (this.conf.get(START_TAG_KEY) == null || this.conf.get(END_TAG_KEY) == null) {
                throw new RuntimeException("Error! XML start and end tags unspecified!");
            }

            startTag = this.conf.get(START_TAG_KEY).getBytes(TEXT_ENCODING);
            endTag   = this.conf.get(END_TAG_KEY).getBytes(TEXT_ENCODING);
        }

        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {

            this.conf = context.getConfiguration();

            this.fileSplit = (FileSplit) split;
            Path file = fileSplit.getPath();
            start     = fileSplit.getStart();

            CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(this.conf);
            CompressionCodec codec = compressionCodecs.getCodec(file);

            FileSystem fs = file.getFileSystem(this.conf);

            if (codec != null) {
                LOG.info("Reading compressed file...");

                fsIn = new DataInputStream(codec.createInputStream(fs.open(file)));

                end = Long.MAX_VALUE;
            } else {
                LOG.info("Reading uncompressed file...");

                FSDataInputStream fileIn = fs.open(file);

                fileIn.seek(start);
                fsIn = fileIn;

                end = start + fileSplit.getLength();
            }

            recordStartPos = start;

            // Because input streams of gzipped files are not seekable (specifically,
            // do not support getPos), we need to keep track of bytes consumed ourselves.

            pos = start;
        }

        @Override
        public boolean nextKeyValue() throws IOException {

            if (pos < end) {

                key.set(pos);

                if (readUntilMatch(startTag, false)) {

                    recordStartPos = pos - startTag.length;

                    try {
                        buffer.write(startTag);
                        if (readUntilMatch(endTag, true)) {
                            key.set(recordStartPos);
                            value.set(buffer.getData(), 0, buffer.getLength());
                            return true;
                        }
                    } finally {

                        // Because input streams of gzipped files are not seekable
                        // (specifically, do not support
                        // getPos), we need to keep track of bytes consumed ourselves.

                        // This is a sanity check to make sure our internal computation of
                        // bytes consumed is
                        // accurate. This should be removed later for efficiency once we
                        // confirm that this code
                        // works correctly.

                        if (fsIn instanceof Seekable) {

                            if (pos != ((Seekable) fsIn).getPos()) {
                                // throw new RuntimeException("bytes consumed error!");
                                LOG.info("bytes conusmed error: " + String.valueOf(pos)
                                        + " != " + String.valueOf(((Seekable) fsIn).getPos()));
                            }
                        }

                        buffer.reset();
                    }
                }
            }
            return false;
        }

        @Override
        public Text getCurrentValue() {
            return this.value;
        }

        @Override
        public LongWritable getCurrentKey() {
            return this.key;
        }

        @Override
        public void close() throws IOException {
            fsIn.close();
        }

        @Override
        public float getProgress() throws IOException {
            return ((float) (pos - start)) / ((float) (end - start));
        }

        public long getStart() {
            return start;
        }

        public long getEnd() {
            return end;
        }

        private boolean readUntilMatch(byte[] match, boolean withinBlock)
                throws IOException {

            int position = 0;

            while (true) {
                int inputInteger = fsIn.read();
                // Increment position (bytes consumed).
                pos++;

                // End of file:
                if (inputInteger == -1) {
                    return false;
                }

                // Save to buffer:
                if (withinBlock) {
                    buffer.write(inputInteger);
                }

                // Check if we're matching:
                if (inputInteger == match[position]) {

                    position++;

                    if (position >= match.length) {
                        return true;
                    }
                } else {
                    position = 0;
                }

                // See if we've passed the stop point:
                if (!withinBlock && position == 0 && pos >= end) {
                    return false;
                }
            }
        }
    }
}
