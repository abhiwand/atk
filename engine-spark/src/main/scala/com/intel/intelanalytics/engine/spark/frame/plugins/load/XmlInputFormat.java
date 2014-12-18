package com.intel.intelanalytics.engine.spark.frame.plugins.load;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import com.google.common.base.Charsets;
import com.google.common.io.Closeables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads records that are delimited by a specific begin/end tag.
 */
public class XmlInputFormat extends TextInputFormat {

    private static final Logger log = LoggerFactory.getLogger(XmlInputFormat.class);

    public static final String START_TAG_KEY = "xmlinput.start";
    public static final String END_TAG_KEY = "xmlinput.end";
    public static final String IS_XML_KEY = "xmlinput.isxml";

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        try {
            return new XmlRecordReader((FileSplit) split, context.getConfiguration());
        } catch (IOException ioe) {
            log.warn("Error while creating XmlRecordReader", ioe);
            return null;
        }
    }

    /**
     * XMLRecordReader class to read through a given xml document to output xml blocks as records as specified
     * by the start tag and end tag
     *
     */
    public static class XmlRecordReader extends RecordReader<LongWritable, Text> {

        private final List<byte[]> startTags;
        private final List<byte[]> endTags;
        private final long start;
        private final long end;
        private final FSDataInputStream fsin;
        private final DataOutputBuffer buffer = new DataOutputBuffer();
        private LongWritable currentKey;
        private Text currentValue;
        private final String[] startTagStrings;
        private String latestStartTag = "";
        private boolean isXML;

        public XmlRecordReader(FileSplit split, Configuration conf) throws IOException {
            startTagStrings = conf.getStrings(START_TAG_KEY);
            String[] endTagStrings = conf.getStrings(END_TAG_KEY);
            startTags = getTagArray(startTagStrings);
            endTags = getTagArray(endTagStrings);
            isXML = conf.getBoolean(IS_XML_KEY, false);

            for(String endTag : endTagStrings){
                for(String startTag : startTagStrings){
                    if(endTag.contains(startTag)){
                        throw new IOException("An end tag cannot contain any start tag as a substring");
                    }
                }
            }

            // open the file and seek to the start of the split
            start = split.getStart();
            end = start + split.getLength();
            Path file = split.getPath();
            FileSystem fs = file.getFileSystem(conf);
            fsin = fs.open(split.getPath());
            fsin.seek(start);
        }

        /**
         * Convert a String array into a list of byte arrays
         * @param values array of strings
         */
        private List<byte []> getTagArray(String[] values){
            List<byte []> tags = new ArrayList<byte []>(values.length);
            for(String value: values){
                tags.add(value.getBytes(Charsets.UTF_8));
            }
            return tags;
        }

        /**
         * Search for the next key/value pair in the hadoop file
         * @param key
         * @param value
         * @return
         * @throws IOException
         */
        private boolean next(LongWritable key, Text value) throws IOException {
            if (fsin.getPos() < end && readUntilMatch(startTags, false)) {
                try {
                    if (readUntilMatchNested(endTags, true, startTags, isXML)) {
                        key.set(fsin.getPos());
                        value.set(buffer.getData(), 0, buffer.getLength());
                        return true;
                    }
                } finally {
                    buffer.reset();
                }
            }
            return false;
        }

        @Override
        public void close() throws IOException {
            Closeables.close(fsin, true);
        }

        @Override
        public float getProgress() throws IOException {
            return (fsin.getPos() - start) / (float) (end - start);
        }

        /**
         * Create an array of integers and set all values to 0
         * @param size length of the array
         */
        private int[] initIntArray(int size){
            int[] arr = new int[size];
            for(int i = 0; i < arr.length; i++){
                arr[i] = 0;
            }
            return arr;
        }

        /**
         * Parse the file byte by byte until we find a match or the file ends this does not track nested values
         * @param match a list of possible matches
         * @param withinBlock true if we are inside of a tag and are looking for an end tag
         * @return true if we find a match
         * @throws IOException
         */
        private boolean readUntilMatch(List<byte[]> match, boolean withinBlock) throws IOException {
            return readUntilMatchNested(match, withinBlock, null, false);
        }

        /**
         * Check if all of the values of an array are equal to 0
         * @param arr int array to verify
         * @return true if all values in the array are equal to 0
         */
        private boolean allValuesAreZero(int[] arr) {
            for (int i = 0; i < arr.length; i++) {
                if (arr[i] != 0)
                    return false;
            }
            return true;
        }

        private enum ParseState {
            NORMAL,
            IN_QUOTE,
            ESCAPE_CHAR
        }

        /**
         * update the byte array index flags
         * @param b current byte
         * @param indices an array of indices related to the corresponding tags
         * @param tags Tag List we are checking for
         * @return -1 if no match, if a match it is the index we are looking for
         */
        private int checkMatches(int b, int[] indices, List<byte []> tags) {
            for(int i = 0; i < indices.length; i++) {
                if (b == tags.get(i)[indices[i]]) {
                    indices[i] = indices[i] + 1;
                    if (indices[i] >= tags.get(i).length) {
                        indices[i] = 0;
                        return i;
                    }
                } else {
                    indices[i] = 0;
                }
            }
            return -1;
        }

        /**
         * Parse the file byte by byte until we find a match or the file ends
         * @param match a list of possible matches
         * @param withinBlock true if we are inside of a tag and are looking for an end tag
         * @param startTag list of possible startTags. only needed if we are within a Block
         * @param checkXML if this is an xml file (rather than a json) check for an alternate end
         * @return true if we find a match
         * @throws IOException
         */
        private boolean readUntilMatchNested(List<byte[]> match, boolean withinBlock, List<byte[]> startTag, boolean checkXML) throws IOException {
            int[] matchIndices = initIntArray(match.size());
            int[] startTagIndices = startTag != null ? initIntArray(startTag.size()) : null;
            List<byte[]> quoteChars = getTagArray(new String[] { "\"", "'"});
            List<byte[]> escapeChars = getTagArray(new String[]{"\\"});
            int[] escapeCharIndices = initIntArray(escapeChars.size());
            int[] quoteIndices = initIntArray(quoteChars.size());
            List<byte[]> xmlNodeEnd = getTagArray(new String[]{">"});
            int[] xmlNodeEndIndices = initIntArray(xmlNodeEnd.size());
            List<byte[]> xmlAltEnd = getTagArray(new String[]{"/>"});
            int[] xmlAltEndIndices = initIntArray(xmlAltEnd.size());
            ParseState parseState = ParseState.NORMAL;
            //if we are examining an xml file we know we need to check for an empty-element tag if the found start tag ends with a space
            boolean inStartTag = checkXML && latestStartTag.endsWith(" ");


            int depth = 0;
            int currentQuote = 0;
            int currentQuoteIndex = -1;
            while (true) {
                int b = fsin.read();
                // end of file:
                if (b == -1) {
                    return false;
                }
                // save to buffer:
                if (withinBlock) {
                    buffer.write(b);
                }

                // This section checks for quoted attributes. If a tag appears in an attribute than it should not count as a match
                if (parseState == ParseState.ESCAPE_CHAR) {
                    //if we are in an escape char we want to check for a quote. If we find a quote we want to stay in the quote mode.
                    if (currentQuoteIndex >= quoteChars.get(currentQuote).length) {
                        parseState = ParseState.IN_QUOTE;
                    }
                    currentQuoteIndex++;
                } else {
                    if (parseState == ParseState.IN_QUOTE) {
                        // if we are inside of a quote we need to verify that the next quote we find is not escaped
                        if (checkMatches(b, escapeCharIndices, escapeChars) != -1)
                            parseState = ParseState.ESCAPE_CHAR;
                    }
                    if (parseState != ParseState.ESCAPE_CHAR) {
                        int quoteIndex;
                        if ((quoteIndex = checkMatches(b, quoteIndices, quoteChars)) != -1) {
                            if (parseState == ParseState.NORMAL) {
                                parseState = ParseState.IN_QUOTE;
                                currentQuote = quoteIndex;
                                currentQuoteIndex = 0;
                            } else {
                                if (quoteIndex == currentQuote) {
                                    parseState = ParseState.NORMAL;
                                }
                            }
                        }
                    }
                }

                int matchIndex;
                if(parseState == ParseState.NORMAL) {
                    if(checkXML && inStartTag){
                        //if this is xml we need to make sure that when the tag ends it doesn't end the node.
                        if(checkMatches(b, xmlAltEndIndices, xmlAltEnd) != -1){
                            //This ends the node because it is an Empty-Element tag
                            if (depth == 0){
                                return true;
                            } else {
                                depth--;
                                inStartTag = false;
                            }
                        }else if(checkMatches(b, xmlNodeEndIndices, xmlNodeEnd) != -1){
                            inStartTag = false;
                        }
                    }


                    if (startTag != null) {
                        // Check if we match the start tag
                        if ((matchIndex = checkMatches(b, startTagIndices, startTag)) != -1) {
                            depth++;
                            if(checkXML) {
                                byte[] foundBytes = startTag.get(matchIndex);
                                latestStartTag = new String(foundBytes, Charsets.UTF_8);
                                inStartTag = latestStartTag.endsWith(" ");
                            }
                            startTagIndices = initIntArray(startTagIndices.length);
                        }
                    }

                    // check if we're matching:
                    if ((matchIndex = checkMatches(b, matchIndices, match)) != -1) {
                        if (depth == 0) {
                            if (!withinBlock) {
                                byte[] foundBytes = match.get(matchIndex);
                                latestStartTag = new String(foundBytes, Charsets.UTF_8);
                                buffer.write(foundBytes);
                            }
                            return true;
                        } else {
                            depth--;
                            matchIndices = initIntArray(matchIndices.length);
                        }
                    }
                }

                // see if we've passed the stop point:
                if (!withinBlock && allValuesAreZero(matchIndices) && fsin.getPos() >= end) {
                    return false;
                }
            }
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return currentKey;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return currentValue;
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            currentKey = new LongWritable();
            currentValue = new Text();
            return next(currentKey, currentValue);
        }
    }
}
