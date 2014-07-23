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
    val titanConfigInput = config.getConfig("titan.load")

    val titanConfig = new SerializableBaseConfiguration()
    titanConfig.setProperty("storage.backend", titanConfigInput.getString("storage.backend"))
    titanConfig.setProperty("storage.hostname", titanConfigInput.getString("storage.hostname"))
    titanConfig.setProperty("storage.port", titanConfigInput.getString("storage.port"))
    titanConfig.setProperty("storage.batch-loading", titanConfigInput.getString("storage.batch-loading"))
    titanConfig.setProperty("storage.buffer-size", titanConfigInput.getString("storage.buffer-size"))
    titanConfig.setProperty("storage.attempt-wait", titanConfigInput.getString("storage.attempt-wait"))
    titanConfig.setProperty("storage.lock-wait-time", titanConfigInput.getString("storage.lock-wait-time"))
    titanConfig.setProperty("storage.lock-retries", titanConfigInput.getString("storage.lock-retries"))
    titanConfig.setProperty("storage.idauthority-retries", titanConfigInput.getString("storage.idauthority-retries"))
    titanConfig.setProperty("storage.read-attempts", titanConfigInput.getString("storage.read-attempts"))
    titanConfig.setProperty("autotype", titanConfigInput.getString("autotype"))
    titanConfig.setProperty("ids.block-size", titanConfigInput.getString("ids.block-size"))
    titanConfig.setProperty("ids.renew-timeout", titanConfigInput.getString("ids.renew-timeout"))

    // Get the graph
    import scala.concurrent.duration._
    val graph = Await.result(sparkInvocation.engine.getGraph(arguments.graph.id), config.getInt("default-timeout") seconds)

    // Set the graph in Titan
    val iatGraphName = GraphName.convertGraphUserNameToBackendName(graph.name)
    titanConfig.setProperty("storage.tablename", iatGraphName)

    // Start KClique Percolation
    System.out.println("*********Starting KClique Percolation********")
    Driver.run(titanConfig, sc, arguments.cliqueSize, arguments.communityPropertyDefaultLabel)

    // Get the execution time and print it
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
