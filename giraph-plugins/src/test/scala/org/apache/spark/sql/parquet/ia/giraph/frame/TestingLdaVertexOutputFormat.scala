//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
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

import com.intel.giraph.io.{ LdaEdgeData, LdaVertexData, LdaVertexId }
import org.apache.giraph.graph.Vertex
import org.apache.giraph.io.{ VertexOutputFormat, VertexWriter }
import org.apache.hadoop.mapreduce.{ JobContext, OutputCommitter, TaskAttemptContext }
import org.apache.mahout.math.Vector

import scala.collection.mutable

import scala.collection.JavaConversions._

/**
 * Object holds global values for tests
 */
object TestingLdaOutputResults {

  val docResults = mutable.Map[String, Vector]()
  val wordResults = mutable.Map[String, Vector]()

}

/**
 * OutputFormat for LDA testing
 */
class TestingLdaVertexOutputFormat extends VertexOutputFormat[LdaVertexId, LdaVertexData, LdaEdgeData] {

  override def createVertexWriter(context: TaskAttemptContext): VertexWriter[LdaVertexId, LdaVertexData, LdaEdgeData] = {
    new VertexWriter[LdaVertexId, LdaVertexData, LdaEdgeData] {

      override def initialize(context: TaskAttemptContext): Unit = {}

      override def writeVertex(vertex: Vertex[LdaVertexId, LdaVertexData, LdaEdgeData]): Unit = {
        if (vertex.getId.isDocument) {
          TestingLdaOutputResults.docResults += vertex.getId.getValue -> vertex.getValue.getLdaResult
        }
        else {
          TestingLdaOutputResults.wordResults += vertex.getId.getValue -> vertex.getValue.getLdaResult
        }
      }

      override def close(context: TaskAttemptContext): Unit = {}
    }
  }

  override def checkOutputSpecs(context: JobContext): Unit = {}

  override def getOutputCommitter(context: TaskAttemptContext): OutputCommitter = new DummyOutputCommitter

}
