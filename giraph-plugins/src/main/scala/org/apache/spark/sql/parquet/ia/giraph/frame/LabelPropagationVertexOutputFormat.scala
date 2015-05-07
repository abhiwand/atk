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

import com.intel.giraph.io.{ VertexData4LPWritable }
import com.intel.ia.giraph.lp.LabelPropagationConfiguration
import org.apache.giraph.graph.Vertex
import org.apache.giraph.io.{ VertexWriter, VertexOutputFormat }
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce._
import org.apache.spark.mllib.ia.plugins.VectorUtils
import org.apache.spark.sql.catalyst.expressions.{ GenericRow, Row }
import org.apache.spark.sql.parquet.RowWriteSupport
import parquet.hadoop.ParquetOutputFormat

object LabelPropagationOutputFormat {
  val OutputRowSchema = "StructType(row(StructField(id,LongType,false),StructField(result,ArrayType(DoubleType,true),true)))"
}
/**
 * OutputFormat for parquet frame
 */
class LabelPropagationVertexOutputFormat extends VertexOutputFormat[LongWritable, VertexData4LPWritable, Nothing] {

  private val resultsOutputFormat = new ParquetOutputFormat[Row](new RowWriteSupport)

  override def createVertexWriter(context: TaskAttemptContext): LabelPropagationVertexWriter = {
    new LabelPropagationVertexWriter(new LabelPropagationConfiguration(context.getConfiguration), resultsOutputFormat)
  }

  override def checkOutputSpecs(context: JobContext): Unit = {
    new LabelPropagationConfiguration(context.getConfiguration).validate()
  }

  override def getOutputCommitter(context: TaskAttemptContext): OutputCommitter = {
    val outputFormatConfig = new LabelPropagationConfiguration(context.getConfiguration).getConfig.outputFormatConfig

    context.getConfiguration.set("mapreduce.output.fileoutputformat.outputdir", outputFormatConfig.parquetFileLocation)
    resultsOutputFormat.getOutputCommitter(context)
  }
}

class LabelPropagationVertexWriter(conf: LabelPropagationConfiguration,
                                   resultsOutputFormat: ParquetOutputFormat[Row])
    extends VertexWriter[LongWritable, VertexData4LPWritable, Nothing] {

  private val outputFormatConfig = conf.getConfig.outputFormatConfig
  private var resultsWriter: RecordWriter[Void, Row] = null

  override def initialize(context: TaskAttemptContext): Unit = {
    // TODO: this looks like it will be needed in future version
    //context.getConfiguration.setBoolean(ParquetOutputFormat.ENABLE_JOB_SUMMARY, true)
    context.getConfiguration.set(RowWriteSupport.SPARK_ROW_SCHEMA, LabelPropagationOutputFormat.OutputRowSchema)

    val fileName = s"/part-${context.getTaskAttemptID.getTaskID.getId}.parquet"
    resultsWriter = resultsOutputFormat.getRecordWriter(context, new Path(outputFormatConfig.parquetFileLocation + fileName))
  }

  override def close(context: TaskAttemptContext): Unit = {
    resultsWriter.close(context)
  }

  override def writeVertex(vertex: Vertex[LongWritable, VertexData4LPWritable, Nothing]): Unit = {

    resultsWriter.write(null, giraphVertexToRow(vertex))
  }

  private def giraphVertexToRow(vertex: Vertex[LongWritable, VertexData4LPWritable, Nothing]): Row = {
    val content = new Array[Any](2)
    content(0) = vertex.getId
    content(1) = VectorUtils.toScalaVector(vertex.getValue.getPosteriorVector())
    new GenericRow(content)
  }
}

