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
import com.intel.intelanalytics.domain.command.CommandDoc
import org.apache.spark.{ SparkConf, SparkContext }
import com.intel.graphbuilder.util.SerializableBaseConfiguration

/**
 * Parameters for executing belief propagation.
 * @param graph Reference to the graph object on which to propagate beliefs.
 * @param priorProperty Name of the property that stores the prior beliefs.
 * @param posteriorProperty Name of the property to which posterior beliefs will be stored.
 * @param edgeWeightProperty Optional String. Name of the property on edges that stores the edge weight.
 *                           If none is supplied, edge weights default to 1.0
 * @param stringOutput Optional Boolean, defaults to false.
 * @param maxIterations Optional integer. The maximum number of iterations of message passing that will be invoked.
 *                      Defaults to 20.
 */
case class BeliefPropagationArgs(graph: GraphReference,
                                 priorProperty: String,
                                 posteriorProperty: String,
                                 stateSpaceSize: Int,
                                 edgeWeightProperty: Option[String] = None,
                                 stringOutput: Option[Boolean] = None,
                                 maxIterations: Option[Int] = None)

/**
 * Companion object holds the default values.
 */
object BeliefPropagationDefaults {
  val stringOutputDefault = false
  val maxIterationsDefault = 20
  val edgeWeightDefault = 1.0d
  val powerDefault = 0d
  val smoothingDefault = 2.0d
}

/**
 * The result object
 *
 * @param log execution log
 * @param time execution time
 */
case class BeliefPropagationResult(log: String, time: Double)

/** Json conversion for arguments and return value case classes */
object BeliefPropagationJsonFormat {
  import DomainJsonProtocol._
  implicit val BPFormat = jsonFormat7(BeliefPropagationArgs)
  implicit val BPResultFormat = jsonFormat2(BeliefPropagationResult)
}

import BeliefPropagationJsonFormat._

/**
 * Launches "loopy" belief propagation.
 *
 * Pulls graph from underlying store, sends it off to the LBP runner, and then sends results back to the underlying
 * store.
 *
 * Right now it is using only Titan for graph storage. In time we will hopefully make this more flexible.
 *
 */
class BeliefPropagation extends SparkCommandPlugin[BeliefPropagationArgs, BeliefPropagationResult] {

  override def name: String = "graphs/ml/belief_propagation"

  override def doc = Some(CommandDoc(oneLineSummary = "Belief propagation by the sum-product algorithm." +
    " Also known as loopy belief propagation.",
    extendedSummary = Some("""
    Extended Summary
    ----------------

    This algorithm analyzes a graphical model with prior beliefs using sum product message passing.
    The priors are read from a property in the graph, the posteriors are written to another property in the graph.

    This is the GraphX based implementation of belief propagation in the toolkit.

    Parameters
    ----------
    prior_property : String
        Name of the vertex property which contains the prior belief for the vertex.
        
    posterior_property : String
        Name of the vertex property which will contain the posterior belief for each vertex.

    state_space_size : Int
        The number of states in the MRF. Used for validation: Belief propagation will not run if
        an input vertex provides a prior belief whose length does not match the state space size.

    edge_weight_property :  String (optional)
        The edge property that contains the edge weight for each edge. The default edge weight is 1 if this
        option is not specified.

    string_output :  Boolean (optional, default is False)
        If this is true, the posterior beliefs will be written as a string containing comma-separated doubles.
        Otherwise, the posterior beliefs are written as lists of doubles.


    max_iterations : Integer (optional)
        The maximum number of super steps that the algorithm will execute.
        The valid value range is all positive integer.
        The default value is 20.

    Returns
    -------
    Multiple line string
        Progress report for belief propagation.


    Examples
    --------
    g.ml.belief_propagation(prior_property = "value", posterior_property = "lbp_posterior", edge_weight_property  = "weight",  max_iterations = 10)

    The expected output is like this
     TBD'}
                           """)))

  override def execute(sparkInvocation: SparkInvocation, arguments: BeliefPropagationArgs)(implicit user: UserPrincipal, executionContext: ExecutionContext): BeliefPropagationResult = {

    val start = System.currentTimeMillis()

    // Get the SparkContext as one the input parameters for Driver

    sparkInvocation.sparkContext.stop

    val sparkConf: SparkConf = sparkInvocation.sparkContext.getConf.set("spark.kryo.registrator", "com.intel.spark.graphon.GraphonKryoRegistrator")

    val sc = new SparkContext(sparkConf)

    try {

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

      val (outVertices, outEdges, log) = BeliefPropagationRunner.run(gbVertices, gbEdges, arguments)

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

    finally {
      sc.stop
    }

  }

}