/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package org.apache.spark.sql.parquet.ia.giraph.frame.cf

import com.intel.taproot.giraph.io.{ VertexData4CFWritable, CFVertexId }
import com.intel.taproot.giraph.cf.CollaborativeFilteringConfiguration
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
