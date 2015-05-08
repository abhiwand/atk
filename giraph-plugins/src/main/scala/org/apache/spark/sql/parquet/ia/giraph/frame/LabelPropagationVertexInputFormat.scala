package org.apache.spark.sql.parquet.ia.giraph.frame

import com.intel.giraph.io.{ VertexData4LPWritable }
import com.intel.ia.giraph.lp.LabelPropagationConfiguration
import com.intel.intelanalytics.engine.spark.frame.RowWrapper
import org.apache.giraph.graph.Vertex
import org.apache.giraph.io.{ VertexInputFormat, VertexValueInputFormat, VertexReader }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ Path, FileSystem }
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.{ TaskAttemptContext, InputSplit, JobContext }
import org.apache.mahout.math.{ DenseVector }
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.parquet.RowReadSupport
import parquet.hadoop.{ ParquetRecordReader, ParquetInputFormat }

import scala.collection.JavaConverters._

/**
 * Vertex input format class.
 */
class LabelPropagationVertexInputFormat extends VertexInputFormat[LongWritable, VertexData4LPWritable, Nothing] {

  private val parquetInputFormat = new ParquetInputFormat[Row](classOf[RowReadSupport])

  /**
   * Validate the input parameters
   * @param conf giraph configuration
   */
  override def checkInputSpecs(conf: Configuration): Unit = {
    new LabelPropagationConfiguration(conf).validate()
  }

  /**
   * Creates a vertex reader for giraph engine
   * @param split data split
   * @param context execution context
   * @return vertex reader
   */
  override def createVertexReader(split: InputSplit, context: TaskAttemptContext): VertexReader[LongWritable, VertexData4LPWritable, Nothing] = {
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

/**
 * Vertex reader class for parquet
 * @param conf reader configuration
 * @param vertexInputFormat format for vertex reader
 */
class LabelPropagationVertexReader(conf: LabelPropagationConfiguration, vertexInputFormat: ParquetInputFormat[Row])
    extends VertexReader[LongWritable, VertexData4LPWritable, Nothing] {

  private val config = conf.getConfig
  private val reader = new ParquetRecordReader[Row](new RowReadSupport)
  private val row = new RowWrapper(config.inputFormatConfig.frameSchema)
  private var currentVertex: Vertex[LongWritable, VertexData4LPWritable, Nothing] = null

  /**
   * initialize the reader
   * @param split data split
   * @param context execution context
   */
  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
    reader.initialize(split, context)
  }

  /**
   * Close the reader
   */
  override def close(): Unit = {
    reader.close()
  }

  /**
   * Get the next vertex from parquet
   * @return true if a new vertex has been read; false otherwise
   */
  override def nextVertex(): Boolean = {
    val hasNext: Boolean = reader.nextKeyValue
    if (hasNext) {
      row.apply(reader.getCurrentValue)

      val sourceId = new LongWritable(row.longValue(config.srcColName))
      val values = row.vectorValue(config.srcLabelColName)
      val denseVector = new DenseVector(values.toArray)

      currentVertex = this.getConf().createVertex()
      val vertexData = new VertexData4LPWritable()
      vertexData.setPriorVector(denseVector)
      vertexData.setPosteriorVector(denseVector)

      currentVertex.initialize(sourceId, vertexData)
    }
    hasNext
  }

  /**
   * See parquet documentation for the progress indicator
   * @return see documentation
   */
  override def getProgress: Float = {
    reader.getProgress
  }

  /**
   * Returns the current vertex (retrieved through nextVertex call)
   * @return a vertex
   */
  override def getCurrentVertex(): Vertex[LongWritable, VertexData4LPWritable, Nothing] = {
    currentVertex
  }
}