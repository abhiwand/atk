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

//package org.apache.spark.frame
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
