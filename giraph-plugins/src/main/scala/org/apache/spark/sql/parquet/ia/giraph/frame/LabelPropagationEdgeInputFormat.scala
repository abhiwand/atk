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

import com.intel.ia.giraph.lp.LabelPropagationConfiguration
import com.intel.intelanalytics.engine.spark.frame.RowWrapper
import org.apache.giraph.edge.{ DefaultEdge, Edge }
import org.apache.giraph.io._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.hadoop.io.{ LongWritable, DoubleWritable }
import org.apache.hadoop.mapreduce.{ InputSplit, JobContext, TaskAttemptContext }
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.parquet.RowReadSupport
import parquet.hadoop.{ ParquetInputFormat, ParquetRecordReader }

import scala.collection.JavaConverters._

/**
 * InputFormat for Label propagation reads edges from Parquet Frame
 */
class LabelPropagationEdgeInputFormat extends EdgeInputFormat[LongWritable, DoubleWritable] {

  private val parquetInputFormat = new ParquetInputFormat[Row](classOf[RowReadSupport])

  override def checkInputSpecs(conf: Configuration): Unit = {
  }

  override def createEdgeReader(split: InputSplit, context: TaskAttemptContext): EdgeReader[LongWritable, DoubleWritable] = {
    val edgeReader = new LabelPropagationEdgeReader(new LabelPropagationConfiguration(context.getConfiguration))
    // algorithm expects edges that go both ways (seems to be how undirected is modeled in Giraph)
    new ReverseEdgeDuplicator(edgeReader)
  }

  override def getSplits(context: JobContext, minSplitCountHint: Int): java.util.List[InputSplit] = {
    val conf = context.getConfiguration
    val path: String = new LabelPropagationConfiguration(conf).getConfig.inputFormatConfig.parquetFileLocation
    val fs: FileSystem = FileSystem.get(conf)

    val statuses = if (fs.isDirectory(new Path(path))) {
      fs.globStatus(new Path(path + "/*.parquet"))
    }
    else {
      fs.globStatus(new Path(path))
    }
    val footers = parquetInputFormat.getFooters(context.getConfiguration, statuses.toList.asJava)

    parquetInputFormat.getSplits(conf, footers).asInstanceOf[java.util.List[InputSplit]]
  }
}

class LabelPropagationEdgeReader(config: LabelPropagationConfiguration)
    extends EdgeReader[LongWritable, DoubleWritable] {

  private val conf = config.getConfig
  private val reader = new ParquetRecordReader[Row](new RowReadSupport)
  private val row = new RowWrapper(conf.inputFormatConfig.frameSchema)

  private var currentSourceId: LongWritable = null
  private var currentEdge: DefaultEdge[LongWritable, DoubleWritable] = null

  override def initialize(inputSplit: InputSplit, context: TaskAttemptContext): Unit = {
    reader.initialize(inputSplit, context)
  }

  override def getProgress: Float = {
    reader.getProgress
  }

  override def getCurrentEdge: Edge[LongWritable, DoubleWritable] = {
    currentEdge
  }

  override def getCurrentSourceId: LongWritable = {
    currentSourceId
  }

  override def nextEdge(): Boolean = {
    val hasNext: Boolean = reader.nextKeyValue
    if (hasNext) {
      row.apply(reader.getCurrentValue)

      val sourceId = new LongWritable(row.longValue(conf.srcColName))
      val destId = new LongWritable(row.longValue(conf.destColName))
      val edgeWeight = new DoubleWritable(row.stringValue(conf.weightColName).toDouble)

      currentSourceId = sourceId
      currentEdge = new DefaultEdge[LongWritable, DoubleWritable]()
      currentEdge.setTargetVertexId(destId)
      currentEdge.setValue(edgeWeight)
    }
    hasNext
  }

  override def close(): Unit = {
    reader.close()
  }
}
