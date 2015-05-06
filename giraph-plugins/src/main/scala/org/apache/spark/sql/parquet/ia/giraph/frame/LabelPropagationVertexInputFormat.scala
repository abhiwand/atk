package org.apache.spark.sql.parquet.ia.giraph.frame

import com.intel.giraph.io.{ VertexData4LPWritable, LabelPropagationVertexId }
import com.intel.ia.giraph.lp.LabelPropagationConfiguration
import com.intel.intelanalytics.engine.spark.frame.RowWrapper
import org.apache.giraph.graph.Vertex
import org.apache.giraph.io.{ VertexReader, VertexInputFormat }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ Path, FileSystem }
import org.apache.hadoop.mapreduce.{ TaskAttemptContext, InputSplit, JobContext }
import org.apache.mahout.math.{ VectorWritable, Vector, DenseVector }
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.parquet.RowReadSupport
import parquet.hadoop.{ ParquetRecordReader, ParquetInputFormat }

import scala.collection.JavaConverters._

class LabelPropagationVertexInputFormat extends VertexInputFormat[LabelPropagationVertexId, VertexData4LPWritable, Nothing] {

  private val parquetInputFormat = new ParquetInputFormat[Row](classOf[RowReadSupport])

  override def checkInputSpecs(conf: Configuration): Unit = {
  }

  override def createVertexReader(split: InputSplit, context: TaskAttemptContext): VertexReader[LabelPropagationVertexId, VertexData4LPWritable, Nothing] = {
    new LabelPropagationVertexReader(new LabelPropagationConfiguration(context.getConfiguration), parquetInputFormat)
  }

  override def getSplits(context: JobContext, minSplitCountHint: Int): java.util.List[InputSplit] = {
    val path: String = new LabelPropagationConfiguration(context.getConfiguration).getConfig.inputFormatConfig.parquetFileLocation
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

class LabelPropagationVertexReader(conf: LabelPropagationConfiguration, vertexInputFormat: ParquetInputFormat[Row])
    extends VertexReader[LabelPropagationVertexId, VertexData4LPWritable, Nothing] {

  private val config = conf.getConfig
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
      val values = row.vectorValue(config.sourceIdLabelColumnName)
      val denseVector = new DenseVector(values.toArray)

      val vertex = this.getConf().createVertex()
      val priorData = new VertexData4LPWritable()
      priorData.setPriorVector(denseVector)
      vertex.initialize(sourceId, priorData)
      return vertex
    }
    else {
      return null
    }
  }
}