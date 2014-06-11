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

import com.tinkerpop.blueprints.Graph
import scala.collection.JavaConversions._
import java.io.File
import java.net.InetAddress

/**
 * Single location for settings used in examples to make them easier to run on different machines.
 */
object ExamplesUtils {

  val hdfsMaster = System.getProperty("HDFS_MASTER", "hdfs://" + hostname)

  /**
   * Storage hostname setting for titan.
   */
  def storageHostname: String = {
    val storageHostname = System.getProperty("STORAGE_HOSTNAME", "localhost")
    println("STORAGE_HOSTNAME: " + storageHostname)
    storageHostname
  }

  /**
   * URL to the Spark Master, from either a system property or best guess
   */
  def sparkMaster: String = {
    val sparkMaster = System.getProperty("SPARK_MASTER", "spark://" + hostname + ":7077")
    println("SPARK_MASTER: " + sparkMaster)
    sparkMaster
  }

  /**
   * Absolute path to the gb.jar file from either a system property or best guess
   */
  def gbJar: String = {
    val gbJar = System.getProperty("GB_JAR", guessGbJar)
    println("gbJar: " + gbJar)
    require(new File(gbJar).exists(), "GB_JAR does not exist")
    gbJar
  }

  /**
   * Check for the gb.jar in expected locations
   */
  private def guessGbJar: String = {
    val possiblePaths = List(
      // SBT build - should be removed after Maven build is working right
      System.getProperty("user.dir") + "/graphbuilder-3/target/scala-2.10/gb.jar",
      System.getProperty("user.dir") + "/target/scala-2.10/gb.jar",
      System.getProperty("user.dir") + "/gb.jar",
      // Maven build not working yet
      System.getProperty("user.dir") + "/graphbuilder-3/target/graphbuilder-3.jar",
      System.getProperty("user.dir") + "/target/graphbuilder-3.jar",
      System.getProperty("user.dir") + "/graphbuilder-3.jar"
    )
    possiblePaths.foreach(path => {
      val jar = new File(path)
      if (jar.exists()) {
        return jar.getAbsolutePath
      }
    })
    throw new RuntimeException("gb jar wasn't found at in any of the expected locations, please run 'sbt assembly' or set GB_JAR")
  }

  /**
   * Spark home directory from either a system property or best guess
   */
  def sparkHome: String = {
    val sparkHome = System.getProperty("SPARK_HOME", guessSparkHome)
    println("SPARK_HOME: " + sparkHome)
    require(new File(sparkHome).exists(), "SPARK_HOME does not exist")
    sparkHome
  }

  /**
   * Check for SPARK_HOME in the expected locations
   */
  private def guessSparkHome: String = {
    val possibleSparkHomes = List("/opt/cloudera/parcels/CDH/lib/spark/", "/usr/lib/spark")
    possibleSparkHomes.foreach(dir => {
      val path = new File(dir)
      if (path.exists()) {
        return path.getAbsolutePath
      }
    })
    throw new RuntimeException("SPARK_HOME wasn't found at any of the expected locations, please set SPARK_HOME")
  }

  /** Hostname for current system */
  private def hostname: String = InetAddress.getLocalHost.getHostName

  /**
   * Path to the movie data set.
   */
  def movieDataset: String = {
    val moviePath = System.getProperty("MOVIE_DATA", "/user/hadoop/netflix.csv")
    println("Movie Data Set in HDFS: " + moviePath)
    hdfsMaster + moviePath
  }

  /**
   * Dump the entire graph into a String (not scalable obviously but nice for quick testing)
   */
  def dumpGraph(graph: Graph): String = {
    var vertexCount = 0
    var edgeCount = 0

    val output = new StringBuilder("---- Graph Dump ----\n")

    graph.getVertices.toList.foreach(v => {
      output.append(v).append("\n")
      vertexCount += 1
    })

    graph.getEdges.toList.foreach(e => {
      output.append(e).append("\n")
      edgeCount += 1
    })

    output.append(vertexCount + " Vertices, " + edgeCount + " Edges")

    output.toString()
  }
}
