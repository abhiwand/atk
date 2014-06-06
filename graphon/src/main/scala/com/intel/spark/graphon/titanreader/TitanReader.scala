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

package com.intel.spark.graphon.titanreader

import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.graphbuilder.driver.spark.titan.reader.{TitanReader => GBTitanReader}
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRDDImplicits._
import org.apache.spark.{ SparkConf, SparkContext }
import java.util.Date
import org.apache.spark.rdd.RDD
import com.intel.graphbuilder.elements.{GraphElement, Edge, Vertex}
import java.io.File
import com.intel.graphbuilder.driver.spark.titan.examples.ExamplesUtils

/**
 * Reads a graph from Titan/HBase into a pair of vertex and edge RDDs.
 */
class TitanReader {


  /**
   *
   * @param graphTableName
   * @param titanStorageHostName
   * @return
   */
  def loadGraph(graphTableName: String, titanStorageHostName: String, sparkMaster: String) : RDD[GraphElement] = {


    val conf = new SparkConf()
      .setMaster(sparkMaster)
      .setAppName(this.getClass.getSimpleName + " " + new Date())
      .setSparkHome(ExamplesUtils.sparkHome)
      .setJars(List(ExamplesUtils.gbJar))


    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "com.intel.graphbuilder.driver.spark.titan.GraphBuilderKryoRegistrator")
    conf.set("spark.kryoserializer.buffer.mb", "32")

    val sc = new SparkContext(conf)

    // Create graph connection

    val titanConfig = new SerializableBaseConfiguration()
    titanConfig.setProperty("storage.backend", "hbase")  // TODO : we should probably have this come from... somewhere
    titanConfig.setProperty("storage.hostname", titanStorageHostName)
    titanConfig.setProperty("storage.tablename", graphTableName)

    val titanConnector = new TitanGraphConnector(titanConfig)

    // Read graph
    val titanReader = new GBTitanReader(sc, titanConnector)
    val titanReaderRDD = titanReader.read()

    // Remember to import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRDDImplicits._ to access filter methods
    val vertexRDD = titanReaderRDD.filterVertices()
    val edgeRDD = titanReaderRDD.filterEdges()

    // If you encounter the following error, "com.esotericsoftware.kryo.KryoException: Buffer overflow", because
    // your results are too large, try:
    // a) Increasing the size of the kryoserializer buffer, e.g., conf.set("spark.kryoserializer.buffer.mb", "32")
    // b) Saving results to file instead of collect(), e.g.titanReaderRDD.saveToTextFile()


    sc.stop()

    titanReaderRDD
  }
}
