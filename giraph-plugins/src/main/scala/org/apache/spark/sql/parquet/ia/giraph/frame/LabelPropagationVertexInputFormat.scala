package org.apache.spark.sql.parquet.ia.giraph.frame

import java.util

import com.intel.giraph.io.{LabelPropagationEdgeData, VertexData4LPWritable, LabelPropagationVertexId}
import com.intel.ia.giraph.lp.LabelPropagationConfiguration
import org.apache.giraph.io.{VertexReader, VertexInputFormat}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.mapreduce.{TaskAttemptContext, InputSplit, JobContext}
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.parquet.RowReadSupport
import parquet.hadoop.ParquetInputFormat

class LabelPropagationVertexInputFormat  extends VertexInputFormat[LabelPropagationVertexId, VertexData4LPWritable, Nothing] {

  private val parquetInputFormat = new ParquetInputFormat[Row](classOf[RowReadSupport])

  override def checkInputSpecs(conf: Configuration): Unit = {
  }

  override def createVertexReader(split: InputSplit, context: TaskAttemptContext): VertexReader[LabelPropagationVertexId, LabelPropagationEdgeData] = {
    new LabelPropagationVertexReader(new LabelPropagationConfiguration(context.getConfiguration),
                                     parquetInputFormat)
  }

  override def getSplits(context: JobContext, minSplitCountHint: Int): util.List[InputSplit] = {
    val path: String = new LabelPropagationConfiguration(context.getConfiguration).labelPropagationConfig.inputFormatConfig.parquetFileLocation
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