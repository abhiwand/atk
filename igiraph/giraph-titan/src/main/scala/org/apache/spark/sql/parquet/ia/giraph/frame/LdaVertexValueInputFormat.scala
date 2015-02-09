////////////////////////////////////////////////////////////////////////////////
//// INTEL CONFIDENTIAL
////
//// Copyright 2014 Intel Corporation All Rights Reserved.
////
//// The source code contained or described herein and all documents related to
//// the source code (Material) are owned by Intel Corporation or its suppliers
//// or licensors. Title to the Material remains with Intel Corporation or its
//// suppliers and licensors. The Material may contain trade secrets and
//// proprietary and confidential information of Intel Corporation and its
//// suppliers and licensors, and is protected by worldwide copyright and trade
//// secret laws and treaty provisions. No part of the Material may be used,
//// copied, reproduced, modified, published, uploaded, posted, transmitted,
//// distributed, or disclosed in any way without Intel's prior express written
//// permission.
////
//// No license under any patent, copyright, trade secret or other intellectual
//// property right is granted to or conferred upon you by disclosure or
//// delivery of the Materials, either expressly, by implication, inducement,
//// estoppel or otherwise. Any license under such intellectual property rights
//// must be express and approved by Intel in writing.
////////////////////////////////////////////////////////////////////////////////
//
//package org.apache.spark.sql.parquet.ia.giraph.frame
//
//import java.util
//
//import com.intel.giraph.io.{ LdaVertexData, LdaVertexId }
//import com.intel.ia.giraph.lda.v2.LdaConfiguration
//import com.intel.intelanalytics.engine.spark.frame.RowWrapper
//import org.apache.giraph.io.{ VertexValueInputFormat, VertexValueReader }
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.fs.{ FileSystem, Path }
//import org.apache.hadoop.mapreduce.{ InputSplit, JobContext, TaskAttemptContext }
//import org.apache.spark.sql.catalyst.expressions.Row
//import org.apache.spark.sql.parquet.RowReadSupport
//import parquet.hadoop.{ ParquetInputFormat, ParquetRecordReader }
//
//import scala.collection.JavaConverters._
//
//class LdaParquetFrameVertexValueInputFormat extends VertexValueInputFormat[LdaVertexId, LdaVertexData] {
//
//  private val parquetInputFormat = new ParquetInputFormat[Row](classOf[RowReadSupport])
//
//  override def checkInputSpecs(conf: Configuration): Unit = {
//    new LdaConfiguration(conf).validate()
//  }
//
//  override def createVertexValueReader(split: InputSplit, context: TaskAttemptContext): VertexValueReader[LdaVertexId, LdaVertexData] = {
//    new LdaParquetFrameVertexValueReader(new LdaConfiguration(context.getConfiguration))
//  }
//
//  override def getSplits(context: JobContext, minSplitCountHint: Int): util.List[InputSplit] = {
//    val path: String = new LdaConfiguration(context.getConfiguration).ldaConfig.inputFormatConfig.parquetFileLocation
//    val fs: FileSystem = FileSystem.get(context.getConfiguration)
//
//    val statuses = if (fs.isDirectory(new Path(path))) {
//      fs.globStatus(new Path(path + "/*.parquet"))
//    }
//    else {
//      fs.globStatus(new Path(path))
//    }
//    val footers = parquetInputFormat.getFooters(context.getConfiguration, statuses.toList.asJava)
//
//    //context.getConfiguration.set(ParquetInputFormat.READ_SUPPORT_CLASS, RowReadSupport.getClass.getName)
//
//    parquetInputFormat.getSplits(context.getConfiguration, footers).asInstanceOf[java.util.List[InputSplit]]
//  }
//}
//
//class LdaParquetFrameVertexValueReader(config: LdaConfiguration) extends VertexValueReader[LdaVertexId, LdaVertexData] {
//
//  private val ldaConfig = config.ldaConfig
//  private val reader = new ParquetRecordReader[Row](new RowReadSupport)
//  private val row = new RowWrapper(config.ldaConfig.inputFormatConfig.frameSchema)
//
//  /** two vertices are read out of every parquet row */
//  private var onSecondVertex = true
//
//  /** two vertices are read out of every parquet row */
//  private var firstVertexId: LdaVertexId = null
//  private var secondVertexId: LdaVertexId = null
//
//  private var firstVertexValue: LdaVertexData = null
//  private var secondVertexValue: LdaVertexData = null
//
//  override def initialize(inputSplit: InputSplit, context: TaskAttemptContext): Unit = {
//    reader.initialize(inputSplit, context)
//  }
//
//  override def getCurrentVertexId: LdaVertexId = {
//    if (onSecondVertex) {
//      secondVertexId
//    }
//    else {
//      firstVertexId
//    }
//  }
//
//  override def getCurrentVertexValue: LdaVertexData = {
//    if (onSecondVertex) {
//      secondVertexValue
//    }
//    else {
//      firstVertexValue
//    }
//  }
//
//  override def getProgress: Float = {
//    reader.getProgress
//  }
//
//  override def nextVertex(): Boolean = {
//    if (onSecondVertex) {
//      onSecondVertex = false
//      val hasNext: Boolean = reader.nextKeyValue
//      if (hasNext) {
//        row.apply(reader.getCurrentValue)
//
//        val docId = new LdaVertexId(row.stringValue(ldaConfig.documentColumnName), true)
//        val wordId = new LdaVertexId(row.stringValue(ldaConfig.wordColumnName), false)
//
//        firstVertexId = docId
//        firstVertexValue = new LdaVertexData(LdaVertexData.VertexType.DOCUMENT)
//
//        secondVertexId = wordId
//        secondVertexValue = new LdaVertexData(LdaVertexData.VertexType.WORD)
//
//        println("nextVertex() doc " + docId + " word " + wordId)
//      }
//      hasNext
//    }
//    else {
//      onSecondVertex = true
//      true
//    }
//  }
//
//  override def close(): Unit = {
//    reader.close()
//  }
//
//}
//
