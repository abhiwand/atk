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

import com.intel.giraph.io.{ LdaVertexId, LdaVertexData }
import com.intel.ia.giraph.lda.v2.LdaConfiguration
import org.apache.giraph.graph.Vertex
import org.apache.giraph.io.{ VertexOutputFormat, VertexWriter }
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce._
import org.apache.spark.sql.catalyst.expressions.{ GenericRow, Row }
import org.apache.spark.sql.parquet.RowWriteSupport
import parquet.hadoop.ParquetOutputFormat

/**
 * OutputFormat for LDA writes Vertices to two Parquet Frames
 */
class LdaParquetFrameVertexOutputFormat extends VertexOutputFormat[LdaVertexId, LdaVertexData, Nothing] {

  private val docResultsOutputFormat = new ParquetOutputFormat[Row](new RowWriteSupport)
  private val wordResultsOutputFormat = new ParquetOutputFormat[Row](new RowWriteSupport)

  override def createVertexWriter(context: TaskAttemptContext): LdaParquetFrameVertexWriter = {
    new LdaParquetFrameVertexWriter(new LdaConfiguration(context.getConfiguration), docResultsOutputFormat, wordResultsOutputFormat)
  }

  override def checkOutputSpecs(context: JobContext): Unit = {
    new LdaConfiguration(context.getConfiguration).validate()
  }

  override def getOutputCommitter(context: TaskAttemptContext): OutputCommitter = {
    val outputFormatConfig = new LdaConfiguration(context.getConfiguration).ldaConfig.outputFormatConfig

    // configure outputdir for committer
    context.getConfiguration.set("mapreduce.output.fileoutputformat.outputdir", outputFormatConfig.documentResultsFileLocation)
    val docCommitter = docResultsOutputFormat.getOutputCommitter(context)

    // re-configure outputdir for committer
    context.getConfiguration.set("mapreduce.output.fileoutputformat.outputdir", outputFormatConfig.wordResultsFileLocation)
    val wordCommitter = wordResultsOutputFormat.getOutputCommitter(context)

    new MultiOutputCommitter(List(docCommitter, wordCommitter))
  }
}

object LdaOutputFormat {
  val OutputRowSchema = "StructType(row(StructField(id,StringType,false),StructField(result,ArrayType(DoubleType,true),true)))"
}

class LdaParquetFrameVertexWriter(conf: LdaConfiguration, docResultsOutputFormat: ParquetOutputFormat[Row], wordResultsOutputFormat: ParquetOutputFormat[Row]) extends VertexWriter[LdaVertexId, LdaVertexData, Nothing] {

  private val outputFormatConfig = conf.ldaConfig.outputFormatConfig

  private var documentResultsWriter: RecordWriter[Void, Row] = null
  private var wordResultsWriter: RecordWriter[Void, Row] = null

  override def initialize(context: TaskAttemptContext): Unit = {
    // TODO: this looks like it will be needed in future version
    //context.getConfiguration.setBoolean(ParquetOutputFormat.ENABLE_JOB_SUMMARY, true)
    context.getConfiguration.set(RowWriteSupport.SPARK_ROW_SCHEMA, LdaOutputFormat.OutputRowSchema)

    val fileName = s"/part-${context.getTaskAttemptID.getTaskID.getId}.parquet"
    documentResultsWriter = docResultsOutputFormat.getRecordWriter(context, new Path(outputFormatConfig.documentResultsFileLocation + fileName))
    wordResultsWriter = wordResultsOutputFormat.getRecordWriter(context, new Path(outputFormatConfig.wordResultsFileLocation + fileName))
  }

  override def close(context: TaskAttemptContext): Unit = {
    documentResultsWriter.close(context)
    wordResultsWriter.close(context)
  }

  override def writeVertex(vertex: Vertex[LdaVertexId, LdaVertexData, Nothing]): Unit = {

    if (vertex.getId.isDocument) {
      documentResultsWriter.write(null, giraphVertexToRow(vertex))
    }
    else {
      wordResultsWriter.write(null, giraphVertexToRow(vertex))
    }
  }

  private def giraphVertexToRow(vertex: Vertex[LdaVertexId, LdaVertexData, Nothing]): Row = {
    val content = new Array[Any](2)
    content(0) = vertex.getId.getValue
    content(1) = vertex.getValue.getLdaResultAsDoubleArray.toSeq
    new GenericRow(content)
  }
}
