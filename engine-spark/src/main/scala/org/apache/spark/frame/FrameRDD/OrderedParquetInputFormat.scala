//package org.apache.spark.frame.FrameRDD
//
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.mapreduce.{ InputSplit, JobContext }
//import org.apache.spark.rdd.NewHadoopPartition
//import parquet.hadoop.{ Footer, ParquetInputSplit, ParquetInputFormat }
//
//import scala.collection.JavaConversions._
//import scala.collection.JavaConverters._
//
//class OrderedParquetInputFormat extends ParquetInputFormat {
//
//  override def getSplits(configuration: Configuration, footers: java.util.List[Footer]): java.util.List[ParquetInputSplit] = {
//    println("Getsplits was called")
//    val splits = super.getSplits(configuration, footers)
//    val sorted = splits.sortBy(split => {
//      val uri = split.getPath.toUri
//      val index = uri.getPath.lastIndexOf("/")
//      val filename = uri.getPath.substring(index)
//      val fileNumber = filename.replaceAll("[a-zA-Z.\\-/]+", "")
//      fileNumber.toLong
//    })
//    sorted.toList.asJava
//  }
//}
