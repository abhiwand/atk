package com.intel.spark.graphon.beliefpropagation

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

/**
 * Parameters for executing belief propagation.
 * @param graph Reference to the graph object on which to propagate beliefs.
 * @param vertexPriorPropertyName Name of the property that stores the prior beliefs.
 * @param posteriorPropertyName Name of the property to which posterior beliefs will be stored.
 * @param edgeWeightProperty Optional String. Name of the property on edges that stores the edge weight.
 *                           If none is supplied, edge weights default to 1.0
 * @param beliefsAsStrings Boolean, defaults to false.
 * @param maxSuperSteps Optional integer. The maximum number of iterations of message passing that will be invoked.
 *                      Defaults to 20.
 */
case class BeliefPropagationArgs(graph: GraphReference,
                                 vertexPriorPropertyName: String,
                                 posteriorPropertyName: String,
                                 edgeWeightProperty: Option[String] = None,
                                 beliefsAsStrings: Boolean = false,
                                 maxSuperSteps: Int = 20)

/**
 * The result object
 *
 * @param log execution log
 * @param time execution time
 */
case class BeliefPropagationResult(log: String, time: Double)

/**
 * Launches belief propagation.
 *
 * Pulls graph from underlying store, sends it off to the LBP runner, and then sends results back to the underlying
 * store.
 *
 * Right now it is using only Titan for graph storage. In time we will hopefully make this more flexible.
 *
 */
class BeliefPropagation extends SparkCommandPlugin[BeliefPropagationArgs, BeliefPropagationResult] {

  import DomainJsonProtocol._

  implicit val LbpFormat = jsonFormat6(BeliefPropagationArgs)
  implicit val LbpResultFormat = jsonFormat2(BeliefPropagationResult)

  override def execute(sparkInvocation: SparkInvocation, arguments: BeliefPropagationArgs)(implicit user: UserPrincipal, executionContext: ExecutionContext): BeliefPropagationResult = {

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

    val (outVertices, outEdges, log) = BeliefPropagationRunner.runLbp(gbVertices, gbEdges, arguments)

    // write out the graph

    // Create the GraphBuilder object
    // Setting true to append for updating existing graph
    val gb = new GraphBuilder(new GraphBuilderConfig(new InputSchema(Seq.empty), List.empty, List.empty, titanConfig, append = true))
    // Build the graph using spark
    gb.buildGraphWithSpark(outVertices, outEdges)

    // Get the execution time and print it
    val time = (System.currentTimeMillis() - start).toDouble / 1000.0

    BeliefPropagationResult(log, time)
  }

  def parseArguments(arguments: JsObject) = arguments.convertTo[BeliefPropagationArgs]

  def serializeReturn(returnValue: BeliefPropagationResult): JsObject = returnValue.toJson.asJsObject()

  override def name: String = "graphs/ml/lbp_graphon"

  override def serializeArguments(arguments: BeliefPropagationArgs): JsObject = arguments.toJson.asJsObject()

}