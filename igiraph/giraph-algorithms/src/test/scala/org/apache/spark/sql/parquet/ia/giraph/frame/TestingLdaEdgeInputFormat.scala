//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

package org.apache.spark.sql.parquet.ia.giraph.frame

import com.intel.giraph.io.{ LdaEdgeData, LdaVertexId }
import org.apache.commons.lang3.StringUtils
import org.apache.giraph.io.formats.TextEdgeInputFormat
import org.apache.giraph.io.{ EdgeReader, ReverseEdgeDuplicator }
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.{ InputSplit, TaskAttemptContext }

/**
 * InputFormat for testing LDA
 */
class TestingLdaEdgeInputFormat extends TextEdgeInputFormat[LdaVertexId, LdaEdgeData] {

  override def createEdgeReader(split: InputSplit, context: TaskAttemptContext): EdgeReader[LdaVertexId, LdaEdgeData] = {
    val reader = new TextEdgeReaderFromEachLine {

      override def getSourceVertexId(line: Text): LdaVertexId = {
        new LdaVertexId(parseLine(line)._1, true)
      }

      override def getTargetVertexId(line: Text): LdaVertexId = {
        new LdaVertexId(parseLine(line)._2, false)
      }

      override def getValue(line: Text): LdaEdgeData = {
        new LdaEdgeData(parseLine(line)._3)
      }

      private def parseLine(line: Text): (String, String, Long) = {
        require(StringUtils.isNotBlank(line.toString), "input cannot be blank, please use 'doc,word,word_count'")
        val parts = line.toString.split(',')
        require(parts.length == 3, "please use 'doc,word,word_count' e.g. 'nytimes,crossword,1'")
        (parts(0), parts(1), parts(2).toLong)
      }
    }
    new ReverseEdgeDuplicator(reader)
  }

}

