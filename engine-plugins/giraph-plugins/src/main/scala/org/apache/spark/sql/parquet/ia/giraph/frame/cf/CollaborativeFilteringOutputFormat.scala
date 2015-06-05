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

import com.intel.giraph.io.{ VertexData4CFWritable, CFVertexId }
import com.intel.ia.giraph.cf.CollaborativeFilteringConfiguration
import org.apache.giraph.graph.Vertex
import org.apache.giraph.io.{ VertexOutputFormat, VertexWriter }
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce._
import org.apache.spark.mllib.ia.plugins.VectorUtils
import org.apache.spark.sql.catalyst.expressions.{ GenericRow, Row }
import org.apache.spark.sql.parquet.RowWriteSupport
import org.apache.spark.sql.parquet.ia.giraph.frame.MultiOutputCommitter
import parquet.hadoop.ParquetOutputFormat

/**
 * OutputFormat for LDA writes Vertices to two Parquet Frames
 */
class CollaborativeFilteringVertexOutputFormat[T <: VertexData4CFWritable] extends VertexOutputFormat[CFVertexId, T, Nothing] {

  private val userOutputFormat = new ParquetOutputFormat[Row](new RowWriteSupport)
  private val itemOutputFormat = new ParquetOutputFormat[Row](new RowWriteSupport)

  override def createVertexWriter(context: TaskAttemptContext): CollaborativeFilteringVertexWriter[T] = {
    new CollaborativeFilteringVertexWriter[T](new CollaborativeFilteringConfiguration(context.getConfiguration),
      userOutputFormat,
      itemOutputFormat)
  }

  override def checkOutputSpecs(context: JobContext): Unit = {
    new CollaborativeFilteringConfiguration(context.getConfiguration).validate()
  }

  override def getOutputCommitter(context: TaskAttemptContext): OutputCommitter = {
    val outputFormatConfig = new CollaborativeFilteringConfiguration(context.getConfiguration).getConfig.outputFormatConfig

    // configure outputdir for committer
    context.getConfiguration.set("mapreduce.output.fileoutputformat.outputdir", outputFormatConfig.userFileLocation)
    val userCommitter = userOutputFormat.getOutputCommitter(context)

    // re-configure outputdir for committer
    context.getConfiguration.set("mapreduce.output.fileoutputformat.outputdir", outputFormatConfig.itemFileLocation)
    val itemCommitter = itemOutputFormat.getOutputCommitter(context)

    new MultiOutputCommitter(List(userCommitter, itemCommitter))
  }
}

object CollaborativeFilteringOutputFormat {
  val OutputRowSchema = "StructType(row(StructField(id,StringType,false),StructField(result,ArrayType(DoubleType,true),true)))"
}

class CollaborativeFilteringVertexWriter[T <: VertexData4CFWritable](conf: CollaborativeFilteringConfiguration,
                                                                     userResultsOutputFormat: ParquetOutputFormat[Row],
                                                                     itemResultsOutputFormat: ParquetOutputFormat[Row])
    extends VertexWriter[CFVertexId, T, Nothing] {

  private val outputFormatConfig = conf.getConfig.outputFormatConfig

  private var userResultsWriter: RecordWriter[Void, Row] = null
  private var itemResultsWriter: RecordWriter[Void, Row] = null

  override def initialize(context: TaskAttemptContext): Unit = {
    context.getConfiguration.set(RowWriteSupport.SPARK_ROW_SCHEMA, CollaborativeFilteringOutputFormat.OutputRowSchema)

    val fileName = s"/part-${context.getTaskAttemptID.getTaskID.getId}.parquet"
    userResultsWriter = userResultsOutputFormat.getRecordWriter(context, new Path(outputFormatConfig.userFileLocation + fileName))
    itemResultsWriter = itemResultsOutputFormat.getRecordWriter(context, new Path(outputFormatConfig.itemFileLocation + fileName))
  }

  override def close(context: TaskAttemptContext): Unit = {
    userResultsWriter.close(context)
    itemResultsWriter.close(context)
  }

  override def writeVertex(vertex: Vertex[CFVertexId, T, Nothing]): Unit = {

    if (vertex.getId.isUser) {
      userResultsWriter.write(null, giraphVertexToRow(vertex))
    }
    else {
      itemResultsWriter.write(null, giraphVertexToRow(vertex))
    }
  }

  private def giraphVertexToRow(vertex: Vertex[CFVertexId, T, Nothing]): Row = {
    val content = new Array[Any](2)
    content(0) = vertex.getId.getValue
    content(1) = VectorUtils.toDoubleArray(vertex.getValue.getVector).toSeq

    new GenericRow(content)
  }
}
