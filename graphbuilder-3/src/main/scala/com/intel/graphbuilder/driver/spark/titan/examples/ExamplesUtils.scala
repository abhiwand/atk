//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
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

package com.intel.graphbuilder.driver.spark.titan.examples

object ExamplesUtils {

  private val SPARK_PORT = "7077"
  private val HOST_NAME = java.net.InetAddress.getLocalHost().getHostName()
  private val SPARK_MASTER = "spark://" + HOST_NAME + ":" + SPARK_PORT
  private val SPARK_HOME = "/opt/cloudera/parcels/CDH/lib/spark/"
  private val GB_JAR_PATH = System.getProperty("user.dir") + "/target//scala-2.10/gb.jar"
  private val HDFS_MASTER = "hdfs://" + HOST_NAME
  private val MOVIE_DATA_SET =  HDFS_MASTER + "/user/hadoop/netflix.csv"

  // Returns Spark Master
  def sparkMaster(): String = {
    return SPARK_MASTER;
  }

  def gbJar(): String = {
    val jar = new java.io.File(GB_JAR_PATH)
    if (!jar.exists()) {
      throw new RuntimeException("gb jar wasn't found at: " + jar.getAbsolutePath + " please run 'sbt assembly'")
    }
    jar.getAbsolutePath
  }

  def sparkHome(): String = {
    val path = new java.io.File(SPARK_HOME)
    if (!path.exists()) {
      throw new RuntimeException("path wasn't found at: " + path.getAbsolutePath + " please ensure the path is correct and rerun the test.")
    }
    path.getAbsolutePath
  }

  def movieDataset(): String = {
    println("----------------------------" + MOVIE_DATA_SET)
    return MOVIE_DATA_SET
  }

}
