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
package com.intel.hadoop.graphbuilder.test.job;

import java.io.IOException;

import com.intel.hadoop.graphbuilder.job.AbstractCreateGraphJob;
import javassist.CannotCompileException;
import javassist.NotFoundException;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.mapred.InputFormat;

import com.intel.hadoop.graphbuilder.demoapps.wikipedia.inputformat.WikiPageInputFormat;
import com.intel.hadoop.graphbuilder.demoapps.wikipedia.linkgraph.LinkGraphTokenizer;
import com.intel.hadoop.graphbuilder.util.Functional;
import com.intel.hadoop.graphbuilder.graphConstruction.GraphTokenizer;

/**
 * Test runnable for creating a link graphElements from wikipedia xml file.
 * 
 */
public class CreateGraphJobTest {
  public class Job extends AbstractCreateGraphJob {

    @Override
    public boolean cleanBidirectionalEdge() {
      return false;
    }
  }

  /**
   * @param args
   *          [inputPath, outputPath]
   * @throws CannotCompileException
   * @throws NotFoundException
   * @throws ParserConfigurationException
   * @throws IOException 
   */
  public static void main(String[] args) throws CannotCompileException,
      NotFoundException, ParserConfigurationException, IOException {
    String wikiinput = args[0];
    String graphoutput = args[1];

    GraphTokenizer tokenizer = new LinkGraphTokenizer();
    InputFormat format = new WikiPageInputFormat();

    CreateGraphJobTest test = new CreateGraphJobTest();
    Job job = test.new Job();
    job.run(tokenizer, format, new String[] { wikiinput }, graphoutput);
  }

}
