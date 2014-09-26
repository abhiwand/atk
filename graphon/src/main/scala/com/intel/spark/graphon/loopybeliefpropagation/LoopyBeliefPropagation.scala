package com.intel.spark.graphon.loopybeliefpropagation

import com.intel.intelanalytics.domain.graph.GraphReference
import com.intel.intelanalytics.engine.spark.plugin.{ SparkInvocation, SparkCommandPlugin }
import com.intel.intelanalytics.domain.DomainJsonProtocol
import com.intel.intelanalytics.security.UserPrincipal
import scala.concurrent.{ Await, ExecutionContext }
import com.intel.intelanalytics.component.Boot
import com.intel.intelanalytics.engine.spark.SparkEngineConfig
import com.intel.intelanalytics.engine.spark.graph.GraphName
import spray.json._
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.driver.spark.titan.reader.TitanReader
import org.apache.spark.rdd.RDD
import com.intel.graphbuilder.elements.{ Vertex => GBVertex, Edge => GBEdge }
import com.intel.graphbuilder.driver.spark.titan.{ GraphBuilderConfig, GraphBuilder }
import com.intel.graphbuilder.parser.InputSchema
import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRDDImplicits._

case class Lbp(graph: GraphReference,
               vertexPriorPropertyName: String,
               edgeWeightProperty: Option[String],
               posteriorPropertyName: String,
               maxSuperSteps: Option[Int] = None)

/**
 * The result object
 *
 * @param log execution log
 * @param time execution time
 */
case class LbpResult(log: String, time: Double)

/**
 * Launches belief propagation.
 *
 * Pulls graph from underlying store, sends it off to the LBP runner, and then sends results back to the underlying
 * store.
 *
 * Right now it is using only Titan for graph storage. In time we will hopefully make this more flexible.
 *
 */
class LoopyBeliefPropagation extends SparkCommandPlugin[Lbp, LbpResult] {

  import DomainJsonProtocol._

  implicit val LbpFormat = jsonFormat5(Lbp)
  implicit val LbpResultFormat = jsonFormat2(LbpResult)

  override def execute(sparkInvocation: SparkInvocation, arguments: Lbp)(implicit user: UserPrincipal, executionContext: ExecutionContext): LbpResult = {

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

    val iatGraphName = GraphName.convertGraphUserNameToBackendName(graph.name)
    titanConfig.setProperty("storage.tablename", iatGraphName)

    val titanConnector = new TitanGraphConnector(titanConfig)

    // Read the graph from Titan
    val titanReader = new TitanReader(sc, titanConnector)
    val titanReaderRDD = titanReader.read()

    val gbVertices: RDD[GBVertex] = titanReaderRDD.filterVertices()
    val gbEdges: RDD[GBEdge] = titanReaderRDD.filterEdges()

    // do a little GraphX MagiX

    val (outVertices, outEdges, log) = LbpRunner.runLbp(gbVertices, gbEdges, arguments)

    // write out the graph

    // Create the GraphBuilder object
    // Setting true to append for updating existing graph
    val gb = new GraphBuilder(new GraphBuilderConfig(new InputSchema(Seq.empty), List.empty, List.empty, titanConfig, append = true))
    // Build the graph using spark
    gb.buildGraphWithSpark(outVertices, outEdges)

    // Get the execution time and print it
    val time = (System.currentTimeMillis() - start).toDouble / 1000.0

    LbpResult(log, time)
  }

  def parseArguments(arguments: JsObject) = arguments.convertTo[Lbp]

  def serializeReturn(returnValue: LbpResult): JsObject = returnValue.toJson.asJsObject()

  override def name: String = "graphs/ml/lbp_graphon"

  override def serializeArguments(arguments: Lbp): JsObject = arguments.toJson.asJsObject()

}