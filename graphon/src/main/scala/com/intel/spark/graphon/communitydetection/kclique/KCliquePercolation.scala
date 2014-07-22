package com.intel.spark.graphon.communitydetection.kclique

import java.util.Date
import com.intel.intelanalytics.engine.spark.plugin.{ SparkInvocation, SparkCommandPlugin }
import com.typesafe.config.ConfigValue
import com.intel.intelanalytics.security.UserPrincipal
import scala.concurrent.{ Await, ExecutionContext }
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.intelanalytics.domain.graph.{ GraphTemplate, GraphReference }
import spray.json.JsObject
import com.intel.intelanalytics.domain.DomainJsonProtocol
import spray.json._
import scala.concurrent._
import com.intel.intelanalytics.engine.spark.graph.GraphName
import com.intel.intelanalytics.component.Boot
import java.util.UUID

case class KClique(graph: GraphReference,
                   cliqueSize: Int,
                   communityPropertyDefaultLabel: String)

case class KCliqueResult(time: Double)

class KCliquePercolation extends SparkCommandPlugin[KClique, KCliqueResult] {

  import DomainJsonProtocol._
  implicit val kcliqueFormat = jsonFormat3(KClique)
  implicit val kcliqueResultFormat = jsonFormat1(KCliqueResult)

  override def execute(sparkInvocation: SparkInvocation, arguments: KClique)(implicit user: UserPrincipal, executionContext: ExecutionContext): KCliqueResult = {

    val start = System.currentTimeMillis()
    System.out.println("*********In Execute method of KCliquePercolation********")

    // Get the SparkContext as one the input parameters for Driver
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

    //    Start KClique Percolation
    System.out.println("*********Starting KClique Percolation********")
    Driver.run(titanConfigInput, sc, arguments.cliqueSize, arguments.communityPropertyDefaultLabel)

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
