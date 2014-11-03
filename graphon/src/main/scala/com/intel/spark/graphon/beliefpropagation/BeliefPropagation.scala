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
 * @param convergenceThreshold Optional Double. BP will terminate when average change in posterior beliefs between
 *                             supersteps is less than or equal to this threshold. Defaults to 0.
 * @param maxIterations Optional integer. The maximum number of iterations of message passing that will be invoked.
 *                      Defaults to 20.
 */
case class BeliefPropagationArgs(graph: GraphReference,
                                 priorProperty: String,
                                 posteriorProperty: String,
                                 edgeWeightProperty: Option[String] = None,
                                 convergenceThreshold: Option[Double] = None,
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
  val convergenceThreshold = 0d
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
  implicit val BPFormat = jsonFormat6(BeliefPropagationArgs)
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

  override def name: String = "graph:titan/ml/belief_propagation"

  //TODO uncomment when we move the next version of spark
  //override def kryoRegistrator: Option[String] = Some("com.intel.spark.graphon.GraphonKryoRegistrator")

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


    edge_weight_property :  String (optional)
        The edge property that contains the edge weight for each edge. The default edge weight is 1 if this
        option is not specified.

    convergence_threshold :  Double (optional)
        BP will terminate when average change in posterior beliefs between supersteps is less than or equal to
         this threshold. Defaults to 0.

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
    graph.ml.belief_propagation("value", "lbp_output", string_output = True, state_space_size = 5, max_iterations = 6)

    {u'log': u'Vertex Count: 80000\nEdge Count: 318398\nIATPregel engine has completed iteration 1  The average delta is 0.6853413553663811\nIATPregel engine has completed iteration 2  The average delta is 0.38626944467366386\nIATPregel engine has completed iteration 3  The average delta is 0.2365329376479823\nIATPregel engine has completed iteration 4  The average delta is 0.14170840479478952\nIATPregel engine has completed iteration 5  The average delta is 0.08676093923623975\n', u'time': 70.248999999999995}

    graph.query.gremlin("g.V [0..4]")

    {u'results': [{u'vertex_type': u'VA', u'target': 12779523, u'lbp_output': u'0.9485759073302487, 0.001314151524421738, 0.040916996746627056, 0.001397331576080859, 0.0077956128226217315', u'_type': u'vertex', u'value': u'0.125 0.125 0.5 0.125 0.125', u'titanPhysicalId': 4, u'_id': 4}, {u'vertex_type': u'VA', u'titanPhysicalId': 8, u'lbp_output': u'0.7476996339617544, 0.0021769696832380173, 0.24559940461433935, 0.0023272253558738786, 0.002196766384794168', u'_type': u'vertex', u'value': u'0.125 0.125 0.5 0.125 0.125', u'source': 7798852, u'_id': 8}, {u'vertex_type': u'TR', u'target': 13041863, u'lbp_output': u'0.7288360734608738, 0.07162637515155296, 0.15391773902131053, 0.022620779563724287, 0.02299903280253846', u'_type': u'vertex', u'value': u'0.5 0.125 0.125 0.125 0.125', u'titanPhysicalId': 12, u'_id': 12}, {u'vertex_type': u'TR', u'titanPhysicalId': 16, u'lbp_output': u'0.9996400056392905, 9.382190989071985E-5, 8.879762476576982E-5, 8.867586165695348E-5, 8.869896439624652E-5', u'_type': u'vertex', u'value': u'0.5 0.125 0.125 0.125 0.125', u'source': 11731127, u'_id': 16}, {u'vertex_type': u'TE', u'titanPhysicalId': 20, u'lbp_output': u'0.004051247779081896, 0.2257641948616088, 0.01794622866204068, 0.7481547408142287, 0.004083587883039745', u'_type': u'vertex', u'value': u'0.125 0.125 0.5 0.125 0.125', u'source': 3408035, u'_id': 20}], u'run_time_seconds': 1.042}
                             |

                           """)))

  override def execute(sparkInvocation: SparkInvocation, arguments: BeliefPropagationArgs)(implicit user: UserPrincipal, executionContext: ExecutionContext): BeliefPropagationResult = {

    val start = System.currentTimeMillis()

    // Get the SparkContext as one the input parameters for Driver
    //TODO change this code to use the sparkContext that was passed in rather than create a new one when we move to the next version of spark
    sparkInvocation.sparkContext.stop

    val sparkConf: SparkConf = sparkInvocation.sparkContext.getConf.set("spark.kryo.registrator", "com.intel.spark.graphon.GraphonKryoRegistrator")
    //sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") works without setting this, why?

    val sc = /*sparkInvocation.sparkContext*/ new SparkContext(sparkConf)

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

      val bpRunnerArgs = BeliefPropagationRunnerArgs(arguments.posteriorProperty,
        arguments.priorProperty,
        arguments.maxIterations,
        stringOutput = Some(true), // string output is default until the ATK supports Vectors as a datatype in tables
        arguments.convergenceThreshold,
        arguments.edgeWeightProperty)

      val (outVertices, outEdges, log) = BeliefPropagationRunner.run(gbVertices, gbEdges, bpRunnerArgs)

      // edges do not change during this computation so we avoid the very expensive step of appending them into Titan

      val dummyOutEdges: RDD[GBEdge] = sc.parallelize(List.empty[GBEdge])

      // write out the graph

      // Create the GraphBuilder object
      // Setting true to append for updating existing graph
      val gb = new GraphBuilder(new GraphBuilderConfig(new InputSchema(Seq.empty), List.empty, List.empty, titanConfig, append = true))
      // Build the graph using spark
      gb.buildGraphWithSpark(outVertices, dummyOutEdges)

      // Get the execution time and print it
      val time = (System.currentTimeMillis() - start).toDouble / 1000.0
      BeliefPropagationResult(log, time)

    }

    finally {
      sc.stop
    }

  }

}
