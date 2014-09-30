
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

package com.intel.spark.graphon.communitydetection.kclique

import java.util.Date
import com.intel.intelanalytics.engine.spark.plugin.{ SparkInvocation, SparkCommandPlugin }
import com.intel.intelanalytics.security.UserPrincipal
import scala.concurrent.{ Await, ExecutionContext }
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.intelanalytics.domain.graph.GraphReference
import com.intel.intelanalytics.domain.DomainJsonProtocol
import spray.json._
import scala.concurrent._
import com.intel.intelanalytics.engine.spark.graph.GraphName
import com.intel.intelanalytics.component.Boot
import com.typesafe.config.Config
import com.intel.intelanalytics.engine.spark.SparkEngineConfig

/**
 * Represents the arguments for KClique Percolation algorithm
 *
 * @param graph reference to the graph for which communities has to be determined
 * @param cliqueSize Parameter determining clique-size and used to find communities. Must be at least 2.
 *                   Large values of cliqueSize result in fewer, smaller communities that are more connected
 * @param communityPropertyLabel name of the community property of vertex that will be
 *                               updated/created in the input graph
 */
case class KClique(graph: GraphReference,
                   cliqueSize: Int,
                   communityPropertyLabel: String) {
  require(cliqueSize > 1, "Invalid clique size; must be at least 2")
}

/**
 * The result object
 *
 * Note: For now it is returning the execution time
 *
 * @param time execution time
 */
case class KCliqueResult(time: Double)

/** Json conversion for arguments and return value case classes */
object KCliquePercolationJsonFormat {
  import DomainJsonProtocol._
  implicit val kcliqueFormat = jsonFormat3(KClique)
  implicit val kcliqueResultFormat = jsonFormat1(KCliqueResult)
}

import KCliquePercolationJsonFormat._
/**
 * KClique Percolation launcher class. Takes the command from python layer
 */
class KCliquePercolation extends SparkCommandPlugin[KClique, KCliqueResult] {

  /**
   * The name of the command, e.g. graphs/ml/kclique_percolation
   */
  override def name: String = "graphs/ml/kclique_percolation"

  override def execute(sparkInvocation: SparkInvocation, arguments: KClique)(implicit user: UserPrincipal, executionContext: ExecutionContext): KCliqueResult = {

    val start = System.currentTimeMillis()

    // Get the SparkContext as one the input parameters for Driver
    val sc = sparkInvocation.sparkContext
    sc.addJar(Boot.getJar("graphon").getPath)

    // Titan Settings for input
    val config = configuration
    val titanConfig = SparkEngineConfig.titanLoadConfiguration

    // Get the graph
    import scala.concurrent.duration._
    val graph = Await.result(sparkInvocation.engine.getGraph(arguments.graph.id), config.getInt("default-timeout") seconds)

    // Set the graph in Titan
    val iatGraphName = GraphName.convertGraphUserNameToBackendName(graph.name)
    titanConfig.setProperty("storage.tablename", iatGraphName)

    // Start KClique Percolation
    Driver.run(titanConfig, sc, arguments.cliqueSize, arguments.communityPropertyLabel)

    // Get the execution time and print it
    val time = (System.currentTimeMillis() - start).toDouble / 1000.0

    KCliqueResult(time)
  }

}
