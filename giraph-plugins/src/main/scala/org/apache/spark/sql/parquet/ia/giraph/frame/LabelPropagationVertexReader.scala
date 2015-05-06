package org.apache.spark.sql.parquet.ia.giraph.frame

import com.intel.giraph.io.{ LabelPropagationVertexId, VertexData4LPWritable }
import com.intel.ia.giraph.lp.LabelPropagationConfiguration
import com.intel.intelanalytics.engine.spark.frame.RowWrapper
import org.apache.giraph.graph.Vertex
import org.apache.giraph.io.{ VertexReader }
import org.apache.hadoop.mapreduce.{ InputSplit, TaskAttemptContext }
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.parquet.{ RowReadSupport }
import parquet.hadoop.{ ParquetRecordReader, ParquetInputFormat }

class LabelPropagationVertexReader(conf: LabelPropagationConfiguration, vertexInputFormat: ParquetInputFormat[Row])
    extends VertexReader[LabelPropagationVertexId, VertexData4LPWritable, Nothing] {

  private val config = conf.labelPropagationConfig
  private val reader = new ParquetRecordReader[Row](new RowReadSupport)
  private val row = new RowWrapper(config.inputFormatConfig.frameSchema)

  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
    reader.initialize(split, context)
  }

  override def close(): Unit = {
    reader.close()
  }

  override def nextVertex(): Boolean = {
    reader.nextKeyValue
  }

  override def getProgress: Float = {
    reader.getProgress
  }

  override def getCurrentVertex(): Vertex[LabelPropagationVertexId, VertexData4LPWritable, Nothing] = {
    val hasNext: Boolean = reader.nextKeyValue
    if (hasNext) {
      row.apply(reader.getCurrentValue)

      val sourceId = new LabelPropagationVertexId(row.stringValue(config.sourceIdColumnName))
      val vertex = this.getConf().createVertex()

      //TODO - what is the value to initialize with?
      vertex.initialize(sourceId, null)
      return vertex
    }
    else {
      return null
    }
  }
}
