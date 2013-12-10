/* Copyright (C) 2013 Intel Corporation.
*     All rights reserved.
*
 *  Licensed under the Apache License, Version 2.0 (the "License");
*  you may not use this file except in compliance with the License.
*  You may obtain a copy of the License at
*
*       http://www.apache.org/licenses/LICENSE-2.0
*
*   Unless required by applicable law or agreed to in writing, software
*   distributed under the License is distributed on an "AS IS" BASIS,
*   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*   See the License for the specific language governing permissions and
*   limitations under the License.
*
* For more about this software visit:
*      http://www.01.org/GraphBuilder
 */

package com.intel.hadoop.graphbuilder.pipeline.tokenizer;

import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Tokenizes the input provided as a string to a list of
 * {@code Edge} and {@code Vertex}objects. This should be the
 * first step to implement along with the design of the InputConfiguration of
 * the raw input.
 *
 * @param <RecordType>
 * @param <VidType>
 * @see com.intel.hadoop.graphbuilder.pipeline.input.InputConfiguration
 */
public interface GraphTokenizer<RecordType, VidType extends WritableComparable<VidType>> {
    /**
     * Configure the tokenizer from JobConf.
     *
     * @param configuration
     */
    void configure(Configuration configuration);

    /**
     * Parses the input record and adds edges and vertices to lists returned by
     * @code getEdges
     * @code getVertices
     * @param record
     * @param context
     */
    void parse(RecordType record, Mapper.Context context);

    /**
     * @return A list of {@code Vertex} extracted from the input.
     */
    Iterator<Vertex<VidType>> getVertices();

    /**
     * @return A list of {@code Edge} extracted from the input.
     */
    Iterator<Edge<VidType>> getEdges();

}
