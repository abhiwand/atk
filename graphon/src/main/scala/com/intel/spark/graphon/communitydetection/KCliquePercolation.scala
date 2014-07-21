
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

package com.intel.spark.graphon.communitydetection

import java.util.Date
import com.intel.intelanalytics.engine.spark.plugin.{ SparkInvocation, SparkCommandPlugin }
import org.apache.hadoop.conf.Configuration
import com.typesafe.config.{ ConfigValue, ConfigObject, Config }
import com.intel.intelanalytics.security.UserPrincipal
import scala.concurrent.{ Await, ExecutionContext }
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.intelanalytics.domain.graph.{GraphTemplate, GraphReference}
import spray.json.JsObject
import com.intel.intelanalytics.domain.DomainJsonProtocol
import spray.json.DefaultJsonProtocol._
import spray.json._
import scala.concurrent._
import scala.collection.JavaConverters._
import com.intel.intelanalytics.engine.spark.graph.GraphName
import com.intel.intelanalytics.component.Boot
import java.util.UUID

case class KClique(graph: GraphReference,
                   cliqueSize: Int,
                   communityPropertyDefaultLabel: String,
                   outputGraphName: String )

case class KCliqueResult(outputGraph: GraphReference)

class KCliquePercolation extends SparkCommandPlugin[KClique, KCliqueResult] {

  import DomainJsonProtocol._
  implicit val kcliqueFormat = jsonFormat4(KClique)
  implicit val kcliqueResultFormat = jsonFormat1(KCliqueResult)

  override def execute(sparkInvocation: SparkInvocation, arguments: KClique)(implicit user: UserPrincipal, executionContext: ExecutionContext): KCliqueResult = {

    val start = System.currentTimeMillis()
    System.out.println("*********In Execute method of KCliquePercolation********")

    // Get the SparkContext as one the input parameters for KCliquePercolationDriver
    val sc = sparkInvocation.sparkContext
    sc.addJar(Boot.getJar("graphon").getPath)

    // Titan Settings for input
    val config = configuration
    val titanConfig = config.getConfig("titan.load")

    val titanConfigInput = new SerializableBaseConfiguration()
    titanConfigInput.setProperty("storage.backend", titanConfig.getString("storage.backend"))
    titanConfigInput.setProperty("storage.hostname", titanConfig.getString("storage.hostname"))
    titanConfigInput.setProperty("storage.port", titanConfig.getString("storage.port"))
    titanConfigInput.setProperty("storage.batch-loading", titanConfig.getString("storage.batch-loading"))
    titanConfigInput.setProperty("storage.buffer-size", titanConfig.getString("storage.buffer-size"))
    titanConfigInput.setProperty("storage.attempt-wait", titanConfig.getString("storage.attempt-wait"))
    titanConfigInput.setProperty("storage.lock-wait-time", titanConfig.getString("storage.lock-wait-time"))
    titanConfigInput.setProperty("storage.lock-retries", titanConfig.getString("storage.lock-retries"))
    titanConfigInput.setProperty("storage.idauthority-retries", titanConfig.getString("storage.idauthority-retries"))
    titanConfigInput.setProperty("storage.read-attempts", titanConfig.getString("storage.read-attempts"))
    titanConfigInput.setProperty("autotype", titanConfig.getString("autotype"))
    titanConfigInput.setProperty("ids.block-size", titanConfig.getString("ids.block-size"))
    titanConfigInput.setProperty("ids.renew-timeout", titanConfig.getString("ids.renew-timeout"))


    import scala.concurrent.duration._
    val graph = Await.result(sparkInvocation.engine.getGraph(arguments.graph.id), config.getInt("default-timeout") seconds)

    val iatGraphName = GraphName.convertGraphUserNameToBackendName(graph.name)
    titanConfigInput.setProperty("storage.tablename", iatGraphName)

    //    Create the output graph in Titan
    val iatOutputGraphName = GraphName.convertGraphUserNameToBackendName(arguments.outputGraphName)
    val outputGraph = Await.result(sparkInvocation.engine.createGraph(GraphTemplate(iatOutputGraphName)), config.getInt("default-timeout") seconds)

    //    Titan settings for output
    val titanConfigOutput = new SerializableBaseConfiguration()
    titanConfigOutput.setProperty("storage.backend", titanConfig.getString("storage.backend"))
    titanConfigOutput.setProperty("storage.hostname", titanConfig.getString("storage.hostname"))
    titanConfigOutput.setProperty("storage.port", titanConfig.getString("storage.port"))
    titanConfigOutput.setProperty("storage.batch-loading", titanConfig.getString("storage.batch-loading"))
    titanConfigOutput.setProperty("storage.buffer-size", titanConfig.getString("storage.buffer-size"))
    titanConfigOutput.setProperty("storage.attempt-wait", titanConfig.getString("storage.attempt-wait"))
    titanConfigOutput.setProperty("storage.lock-wait-time", titanConfig.getString("storage.lock-wait-time"))
    titanConfigOutput.setProperty("storage.lock-retries", titanConfig.getString("storage.lock-retries"))
    titanConfigOutput.setProperty("storage.idauthority-retries", titanConfig.getString("storage.idauthority-retries"))
    titanConfigOutput.setProperty("storage.read-attempts", titanConfig.getString("storage.read-attempts"))
    titanConfigOutput.setProperty("autotype", titanConfig.getString("autotype"))
    titanConfigOutput.setProperty("ids.block-size", titanConfig.getString("ids.block-size"))
    titanConfigOutput.setProperty("ids.renew-timeout", titanConfig.getString("ids.renew-timeout"))
    titanConfigOutput.setProperty("storage.tablename", iatOutputGraphName)

    //    Start KClique Percolation
    System.out.println("*********Starting KClique Percolation********")
    KCliquePercolationDriver.run(titanConfigInput, titanConfigOutput, sc, arguments.cliqueSize, arguments.communityPropertyDefaultLabel)

    val time = (System.currentTimeMillis() - start).toDouble / 1000.0
    System.out.println("*********Finished execution of KCliquePercolation********")
    System.out.println(f"*********Execution Time = $time%.3f seconds********")
    KCliqueResult(time)
  }

  //TODO: Replace with generic code that works on any case class
  def parseArguments(arguments: JsObject) = arguments.convertTo[KClique]

  //TODO: Replace with generic code that works on any case class
  def serializeReturn(returnValue: KCliqueResult): JsObject = returnValue.toJson.asJsObject()

  /**
   * The name of the command, e.g. graphs/ml/kclique_percolation
   */
  override def name: String = "graphs/ml/kclique_percolation"

  //TODO: Replace with generic code that works on any case class
  override def serializeArguments(arguments: KClique): JsObject = arguments.toJson.asJsObject()

}
