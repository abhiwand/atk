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

package org.apache.spark.sql.parquet.ia.giraph.frame.cf

import java.util

import com.intel.giraph.io.{ CFVertexId, EdgeData4CFWritable }
import com.intel.ia.giraph.cf.CollaborativeFilteringConfiguration
import com.intel.intelanalytics.engine.spark.frame.RowWrapper
import org.apache.giraph.edge.{ DefaultEdge, Edge }
import org.apache.giraph.io._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.hadoop.mapreduce.{ InputSplit, JobContext, TaskAttemptContext }
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.parquet.RowReadSupport
import parquet.hadoop.{ ParquetInputFormat, ParquetRecordReader }

import scala.collection.JavaConverters._

/**
 * InputFormat for LDA reads edges from Parquet Frame
 */
class CollaborativeFilteringEdgeInputFormat extends EdgeInputFormat[CFVertexId, EdgeData4CFWritable] {

  private val parquetInputFormat = new ParquetInputFormat[Row](classOf[RowReadSupport])

  override def checkInputSpecs(conf: Configuration): Unit = {
    new CollaborativeFilteringConfiguration(conf).validate()
  }

  override def createEdgeReader(split: InputSplit, context: TaskAttemptContext): EdgeReader[CFVertexId, EdgeData4CFWritable] = {
    val edgeReader = new CollaborativeFilteringEdgeReader(new CollaborativeFilteringConfiguration(context.getConfiguration))

    // algorithm expects edges that go both ways (seems to be how undirected is modeled in Giraph)
    new ReverseEdgeDuplicator(edgeReader)
  }

  override def getSplits(context: JobContext, minSplitCountHint: Int): util.List[InputSplit] = {

    val config = new CollaborativeFilteringConfiguration(context.getConfiguration).getConfig
    val path = config.inputFormatConfig.parquetFileLocation
    val fs = FileSystem.get(context.getConfiguration)

    val statuses = if (fs.isDirectory(new Path(path))) {
      fs.globStatus(new Path(path + "/*.parquet"))
    }
    else {
      fs.globStatus(new Path(path))
    }

    val footers = parquetInputFormat.getFooters(context.getConfiguration, statuses.toList.asJava)
    parquetInputFormat.getSplits(context.getConfiguration, footers).asInstanceOf[java.util.List[InputSplit]]
  }
}

class CollaborativeFilteringEdgeReader(conf: CollaborativeFilteringConfiguration) extends EdgeReader[CFVertexId, EdgeData4CFWritable] {

  private val config = conf.getConfig
  private val reader = new ParquetRecordReader[Row](new RowReadSupport)
  private val row = new RowWrapper(config.inputFormatConfig.frameSchema)

  private var currentSourceId: CFVertexId = null
  private var currentEdge: DefaultEdge[CFVertexId, EdgeData4CFWritable] = null

  override def initialize(inputSplit: InputSplit, context: TaskAttemptContext): Unit = {
    reader.initialize(inputSplit, context)
  }

  override def getProgress: Float = {
    reader.getProgress
  }

  override def getCurrentEdge: Edge[CFVertexId, EdgeData4CFWritable] = {
    currentEdge
  }

  override def getCurrentSourceId: CFVertexId = {
    currentSourceId
  }

  override def nextEdge(): Boolean = {
    val hasNext: Boolean = reader.nextKeyValue
    if (hasNext) {
      row.apply(reader.getCurrentValue)

      val userId = new CFVertexId(row.stringValue(config.userColName), true)
      val itemId = new CFVertexId(row.stringValue(config.itemColName), false)
      val edgeData = new EdgeData4CFWritable()
      edgeData.setWeight(row.longValue(config.ratingColName).toDouble)

      currentSourceId = userId
      currentEdge = new DefaultEdge[CFVertexId, EdgeData4CFWritable]()
      currentEdge.setTargetVertexId(itemId)
      currentEdge.setValue(edgeData)
    }

    hasNext
  }

  override def close(): Unit = {
    reader.close()
  }

}
