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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;

public class WikiPageInputFormat extends TextInputFormat {

    private static final Logger LOG = Logger.getLogger(WikiPageInputFormat.class);

    public static final String START_TAG = "<page>";
    public static final String END_TAG   = "</page>";

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {

        Configuration conf = context.getConfiguration();
        conf.set(XMLInputFormat.START_TAG_KEY, START_TAG);
        conf.set(XMLInputFormat.END_TAG_KEY,   END_TAG);

        XMLInputFormat.XMLRecordReader xmlRecordReader = null;

        try {
            xmlRecordReader = new XMLInputFormat.XMLRecordReader(context);
            xmlRecordReader.initialize(split, context);
        } catch (IOException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.UNHANDLED_IO_EXCEPTION,
                    "Unable to initialize xml record reader.", LOG, e);
        }

        return xmlRecordReader;
    }
}