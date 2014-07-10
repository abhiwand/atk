
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
import com.intel.intelanalytics.domain.graph.GraphReference
import spray.json.JsObject
import com.intel.intelanalytics.domain.DomainJsonProtocol
import spray.json.DefaultJsonProtocol._
import spray.json._
import scala.concurrent._
import scala.collection.JavaConverters._

case class KClique(graph: GraphReference,
                   cliqueSize: Int,
                   communityPropertyDefaultLabel: String)

case class KCliqueResult(runTimeSeconds: Double) //TODO

class KCliquePercolation extends SparkCommandPlugin[KClique, KCliqueResult] {

  import DomainJsonProtocol._
  implicit val kcliqueFormat = jsonFormat3(KClique)
  implicit val kcliqueResultFormat = jsonFormat1(KCliqueResult)

  /**
   * Set a value in the hadoop configuration, if the argument is not None.
   * @param hadoopConfiguration the configuration to update
   * @param hadoopKey the key name to set
   * @param arg the value to use, if it is defined
   */
  def set(hadoopConfiguration: Configuration, hadoopKey: String, arg: Option[Any]) = arg.foreach { value =>
    hadoopConfiguration.set(hadoopKey, value.toString)
  }

  /**
   * Flatten a nested Config structure down to a simple dictionary that maps complex keys to
   * a string value, similar to java.util.Properties.
   *
   * @param config the config to flatten
   * @return a map of property names to values
   */
  private def flattenConfig(config: Config, prefix: String = ""): Map[String, String] = {
    val result = config.root.asScala.foldLeft(Map.empty[String, String]) {
      (map, kv) =>
        kv._2 match {
          case co: ConfigObject =>
            val nested = flattenConfig(co.toConfig, prefix = prefix + kv._1 + ".")
            map ++ nested
          case value: ConfigValue =>
            map + (prefix + kv._1 -> value.unwrapped().toString)
        }
    }
    result
  }

  override def execute(sparkInvocation: SparkInvocation, arguments: KClique)(implicit user: UserPrincipal, executionContext: ExecutionContext): KCliqueResult = {

    val start = System.currentTimeMillis()
    System.out.println("*********In Execute method of KCliquePercolation********")

    // Get the SparkContext as one the input parameters for KCliquePercolationDriver
    val sparkContext = sparkInvocation.sparkContext

    val config = configuration().get
    val titanConf = config.getConfig("titan")

    val graphFuture = sparkInvocation.engine.getGraph(arguments.graph.id)

    // Change this to read from default-timeout
    import scala.concurrent.duration._
    val graph = Await.result(graphFuture, config.getInt("default-timeout") seconds)

    // Create graph connection
    val titanConfig = new SerializableBaseConfiguration()
    titanConfig.setProperty("storage.backend", titanConf.getString("storage.backend"))
    titanConfig.setProperty("storage.hostname", titanConf.getString("storage.hostname"))
    titanConfig.setProperty("storage.port", titanConf.getString("storage.port"))
    titanConfig.setProperty("storage.tablename", graph.name)
    val titanConnector = new TitanGraphConnector(titanConfig)


    KCliquePercolationDriver.run(titanConnector, sparkContext, arguments.cliqueSize, arguments.communityPropertyDefaultLabel)

    val time = (System.currentTimeMillis() - start).toDouble / 1000.0
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
