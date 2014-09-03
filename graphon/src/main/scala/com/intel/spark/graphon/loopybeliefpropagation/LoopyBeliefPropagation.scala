package com.intel.spark.graphon.loopybeliefpropagation

import com.intel.intelanalytics.domain.graph.GraphReference
import com.intel.intelanalytics.engine.spark.plugin.{SparkInvocation, SparkCommandPlugin}
import com.intel.intelanalytics.domain.DomainJsonProtocol
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.spark.graphon.communitydetection.kclique.KCliqueResult
import com.intel.intelanalytics.security.UserPrincipal
import scala.concurrent.{ Await, ExecutionContext }
import com.intel.intelanalytics.component.Boot
import com.intel.intelanalytics.engine.spark.SparkEngineConfig
import com.intel.intelanalytics.engine.spark.graph.GraphName
import spray.json.JsObject
import spray.json._

case class Lbp(graph: GraphReference,
               vertex_value_property_list: Option[String],
               edge_value_property_list: Option[String],
               input_edge_label_list: Option[String],
               output_vertex_property_list: Option[String],
               vertex_type_property_key: Option[String],
               vector_value: Option[String],
               max_supersteps: Option[Int] = None,
               convergence_threshold: Option[Double] = None,
               anchor_threshold: Option[Double] = None,
               smoothing: Option[Double] = None,
               bidirectional_check: Option[Boolean] = None,
               ignore_vertex_type: Option[Boolean] = None,
               max_product: Option[Boolean] = None,
               power: Option[Double] = None)

/**
 * The result object
 *
 * Note: For now it is returning the execution time
 *
 * @param time execution time
 */
case class LbpResult(time: Double)


/**
 * KClique Percolation launcher class. Takes the command from python layer
 */
class LoopyBeliefPropagation extends SparkCommandPlugin[Lbp, LbpResult] {

  import DomainJsonProtocol._
  implicit val kcliqueFormat = jsonFormat15(Lbp)
  implicit val kcliqueResultFormat = jsonFormat1(LbpResult)

  override def execute(sparkInvocation: SparkInvocation, arguments: Lbp)(implicit user: UserPrincipal, executionContext: ExecutionContext): KCliqueResult = {

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

    LbpResult(time)
  }

  def parseArguments(arguments: JsObject) = arguments.convertTo[Lbp]

  def serializeReturn(returnValue: LbpResult): JsObject = returnValue.toJson.asJsObject()

  /**
   * The name of the command, e.g. graphs/ml/kclique_percolation
   */
  override def name: String = "graphs/ml/lbp-graphon"

  override def serializeArguments(arguments: Lbp): JsObject = arguments.toJson.asJsObject()

}