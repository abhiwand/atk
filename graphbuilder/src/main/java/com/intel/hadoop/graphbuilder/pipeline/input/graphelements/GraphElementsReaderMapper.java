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

package com.intel.hadoop.graphbuilder.pipeline.input.graphelements;

import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElementStringTypeVids;
import com.intel.hadoop.graphbuilder.pipeline.input.BaseMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

/**
 * Read graph elements from a sequence file and passes them to the next stage in the pipeline.
 * <p/>
 * Importantly, the elements get a new key.
 *
 * @see com.intel.hadoop.graphbuilder.pipeline.input.BaseMapper
 */
public class GraphElementsReaderMapper extends Mapper<NullWritable, SerializedGraphElementStringTypeVids, IntWritable, SerializedGraphElement> {
    private static final Logger LOG = Logger.getLogger(GraphElementsReaderMapper.class);

    private BaseMapper baseMapper;

    /**
     * Sets  the configuration and the basemapper.
     *
     * @param context
     */
    @Override
    protected void setup(Context context) {

        Configuration conf = context.getConfiguration();

        //Initializes the tokenizer key function and map key and map values.

        setBaseMapper(new BaseMapper(context, conf, LOG));
    }

    /**
     * The mapper used by Hadoop to read from a {@code SequenceFile<NullWritable, SerializedGraphElement>} and
     * emit re-keyed serialized graph elements.
     */

    @Override
    public void map(NullWritable key, SerializedGraphElementStringTypeVids value, Context context) {

        // context.getCounter(GBHTableConfiguration.Counters.HTABLE_ROWS_READ).increment(1);

        // RecordTypeHBaseRow record = getRecordTypeHBaseRow(row, columns);

        // the "tokenizer" in this case won't do anything except pass the element to the right pile
        baseMapper.getTokenizer().parse(value, context, baseMapper);

    }

    /**
     * Sets the basemapper.
     *
     * @param baseMapper The incoming basemapper.
     */
    public void setBaseMapper(BaseMapper baseMapper) {
        this.baseMapper = baseMapper;
    }
}
