/* Copyright (C) 2012 Intel Corporation.
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
package com.intel.hadoop.graphbuilder.job;

import java.io.IOException;
import java.util.HashMap;

import javassist.CannotCompileException;
import javassist.NotFoundException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputFormat;

import com.intel.hadoop.graphbuilder.util.Functional;
import com.intel.hadoop.graphbuilder.graphConstruction.GraphTokenizer;
import com.intel.hadoop.graphbuilder.graphConstruction.mapreduce.CreateGraphMR;
import com.intel.hadoop.graphbuilder.graphConstruction.mapreduce.CreateGraphJobValueFactory;
import com.intel.hadoop.graphbuilder.util.FsUtil;

/**
 * An abstract wrapper class for running the CreateGraphMR Job, which creates a set of
 * graphElements from the raw input data. See an example in {@code CreateGraphJobTest}.
 * <p>
 * User first needs a {@code GraphTokenizer}, and a {@code InputFormat} specific
 * to the input data. The {@code InputFormat} is used for generate a single
 * input from the raw data. And the {@code GraphTokenizer} is used for extract a
 * list of {@Vertex}s and {@code Edge}s from each input given by the
 * {@code InputFormat}. For example, to create a link graph from a Wikipedia xml
 * dump, {@code WikiPageInputFormat} splits the file by the begin and close of
 * "page" tag, and output the string in between as a "page" to the
 * {@code LinkGraphTokenizer}, which then extract the title of the page as the
 * vertex and link as the edges.
 * </p>
 * <p>
 * Next, user will need to override 3 functions: {@code vertexReducer()}, and
 * {@code edgeReducer()}, which are applied to duplicate vertices and edges.
 * They both can return {@code null} in which case only the first instance of
 * duplicate objects will remain. The third function to override is
 * {@code cleanBidirectionalEdge()}, which is the option to keep or discard the
 * bi-directional edges in the graphElements.
 * </p>
 * <p>
 * Additional options can be added into the jobConf by calling
 * {@code addUserOpt}. {@code Functional}s can get option using
 * {@code configure(JobConf)}.
 * </p>
 * <p>
 * Input directories contain any raw input data. Output directories:
 * <ul>
 * <li>$outputdir/edata list of edges</li>
 * <li>$outputdir/vdata list of vertices</li>
 * </ul>
 *
 * @see CreateGraphMR
 * @see Functional
 *
 * @param <VidType>

 */
public abstract class AbstractCreateGraphJob<VidType extends WritableComparable<VidType>> {

  public AbstractCreateGraphJob() {
    this.userOpts = new HashMap<String, String>();
  }

  public abstract boolean cleanBidirectionalEdge();

  public void addUserOpt(String key, String value) {
    userOpts.put(key, value);
  }

  public boolean run(GraphTokenizer<VidType> tokenizer,
      InputFormat inputformat, String[] inputs, String output)
      throws CannotCompileException, NotFoundException, IOException {
    // Required parameters;
    CreateGraphMR mr = new CreateGraphMR(tokenizer, inputformat);

    Class valueClass = CreateGraphJobValueFactory.getValueClassByClassName(
            tokenizer.vidClass().getName());
    // Set value class based on the types of tokenizer.
    mr.setValueClass(valueClass);
    // Distributed the new class file to cluster.
    FsUtil.distributedTempClassToClassPath(mr.getConf());

    // Optional parameters;

    mr.cleanBidirectionalEdge(cleanBidirectionalEdge());

    // User defined parameters;
    if (userOpts != null) {
      mr.setUserOptions(userOpts);
    }

    try {
      mr.run(inputs, output);
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }

  protected HashMap<String, String> userOpts;
}
