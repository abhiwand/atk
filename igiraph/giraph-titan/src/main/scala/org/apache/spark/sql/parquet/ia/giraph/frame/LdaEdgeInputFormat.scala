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

import java.util

import com.intel.giraph.io.{ LdaVertexId, LdaEdgeData }
import com.intel.ia.giraph.lda.v2.LdaConfiguration
import com.intel.intelanalytics.engine.spark.frame.RowWrapper
import org.apache.giraph.edge.{ DefaultEdge, Edge }
import org.apache.giraph.io._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.{ InputSplit, JobContext, TaskAttemptContext }
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.parquet.RowReadSupport
import parquet.hadoop.{ ParquetInputFormat, ParquetRecordReader }

import scala.collection.JavaConverters._

/**
 * InputFormat for LDA reads edges from Parquet Frame
 */
class LdaParquetFrameEdgeInputFormat extends EdgeInputFormat[LdaVertexId, LdaEdgeData] {

  //val clazz: java.lang.Class[ReadSupport[Row]] = classOf[RowReadSupport].asInstanceOf[Class[ReadSupport[Row]]]
  private val parquetInputFormat = new ParquetInputFormat[Row](classOf[RowReadSupport])

  override def checkInputSpecs(conf: Configuration): Unit = {
    new LdaConfiguration(conf).validate()
  }

  override def createEdgeReader(split: InputSplit, context: TaskAttemptContext): EdgeReader[LdaVertexId, LdaEdgeData] = {
    val ldaEdgeReader = new LdaParquetFrameEdgeReader(new LdaConfiguration(context.getConfiguration))
    new ReverseEdgeDuplicator(ldaEdgeReader)
  }

  override def getSplits(context: JobContext, minSplitCountHint: Int): util.List[InputSplit] = {
    val path: String = new LdaConfiguration(context.getConfiguration).ldaConfig.inputFormatConfig.parquetFileLocation
    val fs: FileSystem = FileSystem.get(context.getConfiguration)

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

class LdaParquetFrameEdgeReader(config: LdaConfiguration) extends EdgeReader[LdaVertexId, LdaEdgeData] {

  private val ldaConfig = config.ldaConfig
  private val reader = new ParquetRecordReader[Row](new RowReadSupport)
  private val row = new RowWrapper(config.ldaConfig.inputFormatConfig.frameSchema)

  private var currentSourceId: LdaVertexId = null
  private var currentEdge: DefaultEdge[LdaVertexId, LdaEdgeData] = null

  override def initialize(inputSplit: InputSplit, context: TaskAttemptContext): Unit = {
    reader.initialize(inputSplit, context)
  }

  override def getProgress: Float = {
    reader.getProgress
  }

  override def getCurrentEdge: Edge[LdaVertexId, LdaEdgeData] = {
    currentEdge
  }

  override def getCurrentSourceId: LdaVertexId = {
    currentSourceId
  }

  override def nextEdge(): Boolean = {
    val hasNext: Boolean = reader.nextKeyValue
    if (hasNext) {
      row.apply(reader.getCurrentValue)

      val docId = new LdaVertexId(row.stringValue(ldaConfig.documentColumnName), true)
      val wordId = new LdaVertexId(row.stringValue(ldaConfig.wordColumnName), false)
      val wordCount = row.longValue(ldaConfig.wordCountColumnName)

      currentSourceId = docId

      currentEdge = new DefaultEdge[LdaVertexId, LdaEdgeData]()
      currentEdge.setTargetVertexId(wordId)
      currentEdge.setValue(new LdaEdgeData(wordCount.toDouble))
    }
    hasNext
  }

  override def close(): Unit = {
    reader.close()
  }

}

