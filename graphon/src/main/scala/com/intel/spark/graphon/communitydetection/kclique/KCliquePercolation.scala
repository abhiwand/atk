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

package com.intel.spark.graphon.communitydetection.kclique

import java.util.Date
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRddImplicits._
import com.intel.intelanalytics.component.Boot
import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.graph.GraphReference
import com.intel.intelanalytics.engine.spark.context.SparkContextFactory
import com.intel.intelanalytics.engine.spark.graph.{ SparkGraphHBaseBackend, GraphBuilderConfigFactory }
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal
import com.thinkaurelius.titan.hadoop.formats.titan_050.hbase.CachedTitanHBaseRecordReader

import scala.concurrent._

/**
 * Represents the arguments for KClique Percolation algorithm
 *
 * @param graph Reference to the graph for which communities has to be determined.
 * @param cliqueSize Parameter determining clique-size and used to find communities. Must be at least 2.
 *                   Larger values of cliqueSize result in fewer, smaller, more cohesive communities.
 * @param communityPropertyLabel Name of the community property of vertex that will be
 *                               updated/created in the input graph.
 */
case class KClique(graph: GraphReference,
                   cliqueSize: Int,
                   communityPropertyLabel: String) {
  require(cliqueSize > 1, "Invalid clique size; must be at least 2")
}

/**
 * The result object.
 *
 * Note: For now it is returning the execution time
 *
 * @param time execution time
 */

case class KCliqueResult(time: Double)

/**
 * Json conversion for arguments and return value case classes
 */

object KCliquePercolationJsonFormat {
  import com.intel.intelanalytics.domain.DomainJsonProtocol._
  implicit val kcliqueFormat = jsonFormat3(KClique)
  implicit val kcliqueResultFormat = jsonFormat1(KCliqueResult)
}

import KCliquePercolationJsonFormat._
/**
 * KClique Percolation plugin class.
 */
class KCliquePercolation extends SparkCommandPlugin[KClique, KCliqueResult] {

  /**
   * The name of the command, e.g. graphs/ml/kclique_percolation
   */
  override def name: String = "graph:titan/ml/kclique_percolation"

  /**
   * The number of jobs varies with the number of supersteps required to find the connected components
   * of the derived clique-shadow graph.... we cannot properly anticipate this without doing a full analysis of
   * the graph.
   *
   * @param arguments command arguments: used if a command can produce variable number of jobs
   * @return number of jobs in this command
   */
  override def numberOfJobs(arguments: KClique)(implicit invocation: Invocation): Int = {
    8 + 2 * arguments.cliqueSize
  }

  override def kryoRegistrator: Option[String] = None

  override def execute(arguments: KClique)(implicit invocation: Invocation): KCliqueResult = {

    val start = System.currentTimeMillis()

    // Get the SparkContext as one the input parameters for Driver
    if (sc.master != "yarn-cluster")
      sc.addJar(SparkContextFactory.jarPath("graphon"))

    // Titan Settings for input
    val config = configuration

    // Get the graph
    val graph = engine.graphs.expectGraph(arguments.graph)
    val (gbVertices, gbEdges) = engine.graphs.loadGbElements(sc, graph)
    val (outVertices, outEdges) = KCliquePercolationRunner.run(gbVertices, gbEdges, arguments.cliqueSize, arguments.communityPropertyLabel)

    // Update back each vertex in the input Titan graph and the write the community property
    // as the set of communities to which it belongs
    val communityWriterInTitan = new CommunityWriterInTitan()
    val titanConfig = GraphBuilderConfigFactory.getTitanConfiguration(graph)
    communityWriterInTitan.run(outVertices, outEdges, titanConfig)

    // Get the execution time and print it
    val time = (System.currentTimeMillis() - start).toDouble / 1000.0

    KCliqueResult(time)
  }

}
