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
package com.intel.hadoop.graphbuilder.pipeline.input.text;

import java.io.IOException;

import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement;
import com.intel.hadoop.graphbuilder.pipeline.input.BaseMapper;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction.KeyFunction;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.GraphTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

/**
 * The Mapper that processes Text values (with long keys), applies a string-based parser to the text,
 * and emits property graph elements.
 *
 * @see GraphTokenizer
 * @see KeyFunction
 */

public class TextParsingMapper extends Mapper<LongWritable, Text, IntWritable, SerializedGraphElement> {

    private static final Logger LOG = Logger.getLogger(TextParsingMapper.class);

    private BaseMapper baseMapper;

    public void setBaseMapper(BaseMapper baseMapper) {
        this.baseMapper = baseMapper;
    }

    @Override
    public void setup(Context context) {

        Configuration conf = context.getConfiguration();

        setBaseMapper(new BaseMapper(context, conf, LOG));

    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException {
        baseMapper.getTokenizer().parse(value.toString(), context);

        baseMapper.writeEdges(context);

        baseMapper.writeVertices(context);
    }
}
