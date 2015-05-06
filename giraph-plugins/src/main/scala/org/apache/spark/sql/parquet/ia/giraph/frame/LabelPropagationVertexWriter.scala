package org.apache.spark.sql.parquet.ia.giraph.frame

import com.intel.giraph.io.{VertexData4LPWritable, LabelPropagationVertexId}
import com.intel.ia.giraph.lp.LabelPropagationConfiguration
import org.apache.giraph.graph.Vertex
import org.apache.giraph.io.VertexWriter
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{TaskAttemptContext, RecordWriter}
import org.apache.spark.sql.catalyst.expressions.{GenericRow, Row}
import org.apache.spark.sql.parquet.RowWriteSupport
import parquet.hadoop.ParquetOutputFormat

class LabelPropagationVertexWriter(conf: LabelPropagationConfiguration,
                                   resultsOutputFormat: ParquetOutputFormat[Row])
  extends VertexWriter[LabelPropagationVertexId, VertexData4LPWritable, Nothing] {

  private val outputFormatConfig = conf.labelPropagationConfig.outputFormatConfig
  private var resultsWriter: RecordWriter[Void, Row] = null

  override def initialize(context: TaskAttemptContext): Unit = {
    // TODO: this looks like it will be needed in future version
    //context.getConfiguration.setBoolean(ParquetOutputFormat.ENABLE_JOB_SUMMARY, true)
    context.getConfiguration.set(RowWriteSupport.SPARK_ROW_SCHEMA, LdaOutputFormat.OutputRowSchema)

    val fileName = s"/part-${context.getTaskAttemptID.getTaskID.getId}.parquet"
    resultsWriter = resultsOutputFormat.getRecordWriter(context, new Path(outputFormatConfig.parquetFileLocation + fileName))
  }

  override def close(context: TaskAttemptContext): Unit = {
    resultsWriter.close(context)
  }

  override def writeVertex(vertex: Vertex[LabelPropagationVertexId, VertexData4LPWritable, Nothing]): Unit = {

      resultsWriter.write(null, giraphVertexToRow(vertex))
  }

  private def giraphVertexToRow(vertex: Vertex[LabelPropagationVertexId, VertexData4LPWritable, Nothing]): Row = {
    val content = new Array[Any](2)
    content(0) = vertex.getId.getValue
    content(1) = vertex.getValue.getPosteriorVector()
    new GenericRow(content)
  }
}
