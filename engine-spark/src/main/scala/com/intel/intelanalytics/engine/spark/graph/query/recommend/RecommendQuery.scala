package com.intel.intelanalytics.engine.spark.graph.query.recommend

import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRDDImplicits._
import com.intel.graphbuilder.driver.spark.titan.reader.TitanReader
import com.intel.graphbuilder.elements.{ Edge, Vertex, GraphElement }
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.intelanalytics.domain.DomainJsonProtocol
import com.intel.intelanalytics.domain.graph.GraphReference
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal
import org.apache.spark.storage.StorageLevel
import spray.json._

import scala.collection.JavaConverters._
import scala.concurrent._
import spray.json.DefaultJsonProtocol._
import com.intel.intelanalytics.domain.command.CommandDoc

/**
 * Get recommendation to either left-side or right-side vertices
 * The prerequisite is at least one of two algorithms (ALS or CGD) has been run before this query
 *
 * @param graph The graph reference
 * @param vertex_id The vertex id to get recommendation for.
 * @param vertex_type The vertex type to get recommendation for. The valid value is either "L" or "R".
 *                     "L" stands for left-side vertices of a bipartite graph.
 *                     "R" stands for right-side vertices of a bipartite graph.
 *                     For example, if your input data is "user,movie,rating" and you want to
 *                     get recommendation on user, please input "L" because user is your left-side
 *                     vertex. Similarly, please input "R if you want to get recommendation for movie.
 *                     The default value is "L".
 * @param output_vertex_property_list The property name for ALS/CGD results.When bias is enabled,
 *                                    the last property name in the output_vertex_property_list is for bias.
 *                                    The default value is "als_result".
 * @param vertex_type_property_key  The property name for vertex type. The default value is "vertex_type"
 * @param edge_type_property_key The property name for edge type. The default value "splits".
 *                                 We need this name to know data is in train, validation or test splits
 * @param vector_value Whether ALS/CDG results are saved in a vector for each vertex. The default value is "true"
 * @param bias_on Whether bias turned on/off for ALS/CDG calculation.
 *                When bias is enabled, the last property name in the output_vertex_property_list is for bias.
 *                The default value is "false"
 * @param train_str The label for training data. The default value is "TR".
 * @param num_output_results The number of recommendations to output. The default value is 10.
 * @param left_vertex_name The real name for left side vertex. The default value is "user".
 * @param right_vertex_name The real name for right side vertex. The default value is "movie".
 * @param left_vertex_id_property_key The property name for left side vertex id. The default value is "user_id".
 * @param right_vertex_id_property_key The property name for right side vertex id. The default value is "movie_id".
 */
case class RecommendParams(graph: GraphReference,
                           vertex_id: Long,
                           vertex_type: Option[String],
                           output_vertex_property_list: Option[String],
                           vertex_type_property_key: Option[String],
                           edge_type_property_key: Option[String],
                           vector_value: Option[String],
                           bias_on: Option[String],
                           train_str: Option[String],
                           num_output_results: Option[Int],
                           left_vertex_name: Option[String],
                           right_vertex_name: Option[String],
                           left_vertex_id_property_key: Option[String],
                           right_vertex_id_property_key: Option[String]) {
}

/**
 * Algorithm report comprising of recommended Ids and scores.
 *
 * @param recommendation multi-line string. Each line contains recommended vertex Id and score
 */
case class RecommendResult(recommendation: String)

/** Json conversion for arguments and return value case classes */
object RecommendJsonFormat {
  import com.intel.intelanalytics.domain.DomainJsonProtocol._
  implicit val recommendParamsFormat = jsonFormat14(RecommendParams)
  implicit val recommendResultFormat = jsonFormat1(RecommendResult)
}

import RecommendJsonFormat._

class RecommendQuery extends SparkCommandPlugin[RecommendParams, RecommendResult] {

  /**
   * The name of the command, e.g. graph/sampling/vertex_sample
   */
  override def name: String = "graph:titan/query/recommend"

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  override def doc = Some(CommandDoc(oneLineSummary = "Trained model recommendation.",
    extendedSummary = Some("""
                            |   Extended Summary
                            |   ----------------
                            |   Get recommendation to either left-side or right-side vertices.
                            |   The prerequisite is at least one of two algorithms (ALS or CGD) has been run before this query.
                            |   
                            |   Parameters
                            |   ----------
                            |   vertex_id : int32
                            |       The vertex id to get recommendation for
                            |   
                            |   vertex_type : string (optional)
                            |       The vertex type to get recommendation for.
                            |       The valid value is either "L" or "R".
                            |       "L" stands for left-side vertices of a bipartite graph.
                            |       "R" stands for right-side vertices of a bipartite graph.
                            |       For example, if your input data is "user,movie,rating" and
                            |       you want to get recommendations on user, input "L" because
                            |       user is your left-side vertex.
                            |       Similarly, input "R" if you want to get recommendations for movie.
                            |       The default value is "L".
                            |   
                            |   output_vertex_property_list : comma-separated string (optional)
                            |       The property name for ALS/CGD results.
                            |       When bias is enabled,
                            |       the last property name in the output_vertex_property_list is for bias.
                            |       The default value is "als_result".
                            |   
                            |   vertex_type_property_key : string (optional)
                            |       The property name for vertex type.
                            |       The default value is "vertex_type".
                            |   
                            |   edge_type_property_key : string (optional)
                            |       The property name for edge type.
                            |       We need this name to know data is in train, validation or test splits.
                            |       The default value is "splits".
                            |   
                            |   vector_value : string (optional)
                            |       Whether ALS/CDG results are saved in a vector for each vertex.
                            |       The default value is "true".
                            |   
                            |   bias_on : string (optional)
                            |       Whether bias turned on/off for ALS/CDG calculation.
                            |       When bias is enabled,
                            |       the last property name in the output_vertex_property_list is for bias.
                            |       The default value is "false".
                            |   
                            |   train_str : string (optional)
                            |       The label for training data.
                            |       The default value is "TR".
                            |   
                            |   num_output_results : int (optional)
                            |       The number of recommendations to output.
                            |       The default value is 10.
                            |   
                            |   left_vertex_name : string (optional)
                            |       The real name for left side vertex.
                            |       The default value is "user".
                            |   
                            |   right_vertex_name : string (optional)
                            |       The real name for right side vertex.
                            |       The default value is "movie".
                            |   
                            |   left_vertex_id_property_key : string (optional)
                            |       The property name for left side vertex id.
                            |       The default value is "user_id".
                            |   
                            |   right_vertex_id_property_key : string (optional)
                            |       The property name for right side vertex id.
                            |       The default value is "movie_id".
                            |   
                            |   Returns
                            |   -------
                            |   Multiple line string
                            |       Recommendations for the input vertex
                            |   
                            |   Examples
                            |   --------
                            |   For example, if your left-side vertices are users,
                            |   and you want to get movie recommendations for user 1,
                            |   the command to use is::
                            |   
                            |       g.query.recommend(vertex_id = 1)
                            |   
                            |   The expected output is like this::
                            |   
                            |       {u'recommendation': u'Top 10 recommendation for user 1\\nmovie\\t-132\\tscore\\t5.617994036115665\\nmovie\\t-53\\tscore\\t5.311958055352947\\nmovie\\t-389\\tscore\\t5.098006034765436\\nmovie\\t-84\\tscore\\t4.695484062644423\\nmovie\\t-302\\tscore\\t4.693913046573323\\nmovie\\t-462\\tscore\\t4.648850870271126\\nmovie\\t-186\\tscore\\t4.495738316971479\\nmovie\\t-234\\tscore\\t4.432865786903878\\nmovie\\t-105\\tscore\\t4.418878980193627\\nmovie\\t-88\\tscore\\t4.415980762315559\\n'}
                            |   
                            """.stripMargin)))

  override def execute(invocation: SparkInvocation, arguments: RecommendParams)(
    implicit user: UserPrincipal, executionContext: ExecutionContext): RecommendResult = {
    import scala.concurrent.duration._

    System.out.println("*********Start to execute Recommend query********")
    val config = configuration
    val graphFuture = invocation.engine.getGraph(arguments.graph.id)
    val graph = Await.result(graphFuture, config.getInt("default-timeout") seconds)
    val pattern = "[\\s,\\t]+"
    val outputVertexPropertyList = arguments.output_vertex_property_list.getOrElse(
      config.getString("output_vertex_property_list"))
    val resultPropertyList = outputVertexPropertyList.split(pattern)
    val vectorValue = arguments.vector_value.getOrElse(config.getString("vector_value")).toBoolean
    val biasOn = arguments.bias_on.getOrElse(config.getString("bias_on")).toBoolean
    require(resultPropertyList.size >= 1,
      "Please input at least one vertex property name for ALS/CGD results")
    require(!vectorValue || !biasOn ||
      (vectorValue && biasOn && resultPropertyList.size == 2),
      "Please input one property name for bias and one property name for results when both vector_value " +
        "and bias_on are enabled")

    val vertexId = arguments.vertex_id.toString
    val vertexType = arguments.vertex_type.getOrElse(config.getString("vertex_type")).toLowerCase
    val leftVertexIdPropertyKey = arguments.left_vertex_id_property_key.getOrElse(
      config.getString("left_vertex_id_property_key"))
    val rightVertexIdPropertyKey = arguments.right_vertex_id_property_key.getOrElse(
      config.getString("right_vertex_id_property_key"))
    val vertexTypePropertyKey = arguments.vertex_type_property_key.getOrElse(
      config.getString("vertex_type_property_key"))
    val edgeTypePropertyKey = arguments.edge_type_property_key.getOrElse(
      config.getString("edge_type_property_key"))
    val leftVertexName = arguments.left_vertex_name.getOrElse(config.getString("left_vertex_name"))
    val rightVertexName = arguments.right_vertex_name.getOrElse(config.getString("right_vertex_name"))
    val trainStr = arguments.train_str.getOrElse(config.getString("train_str")).toLowerCase
    val numOutputResults = arguments.num_output_results.getOrElse(config.getInt("num_output_results"))

    val (targetVertexType, sourceVertexName, targetVertexName, sourceIdPropertyKey, targetIdPropertyKey) =
      if (vertexType == config.getString("vertex_type").toLowerCase) {
        ("r", leftVertexName, rightVertexName, leftVertexIdPropertyKey, rightVertexIdPropertyKey)
      }
      else {
        ("l", rightVertexName, leftVertexName, rightVertexIdPropertyKey, leftVertexIdPropertyKey)
      }

    // Create graph connection
    val titanConfiguration = new SerializableBaseConfiguration()
    val titanLoadConfig = config.getConfig("titan.load")
    for (entry <- titanLoadConfig.entrySet().asScala) {
      titanConfiguration.addProperty(entry.getKey, titanLoadConfig.getString(entry.getKey))
    }
    titanConfiguration.setProperty("storage.tablename", "iat_graph_" + graph.name)
    val titanConnector = new TitanGraphConnector(titanConfiguration)

    val sc = invocation.sparkContext
    val titanReader = new TitanReader(sc, titanConnector)
    val titanReaderRDD = titanReader.read()
    val vertexRDD = titanReaderRDD.filterVertices().distinct()
    val edgeRDD = titanReaderRDD.filterEdges().distinct()

    //get the source vertex based on its id
    val sourceVertexRDD = vertexRDD.filter(
      vertex => vertex.getPropertyValueAsString(sourceIdPropertyKey) == vertexId &&
        vertex.getPropertyValueAsString(vertexTypePropertyKey).toLowerCase == vertexType
    )
    sourceVertexRDD.persist(StorageLevel.MEMORY_AND_DISK)

    val sourceVertexArray = sourceVertexRDD.toArray()

    if (sourceVertexArray.length != 1) {
      for (i <- 0 until sourceVertexArray.length) {
        System.out.println("found vertex " + sourceVertexArray(i))
      }
      throw new RuntimeException("Found " + sourceVertexArray.length + " matching vertex, " +
        "There should be only one vertex match the required vertex_id and vertex_type!")
    }

    val sourceVertex = sourceVertexArray(0)
    val sourceGbId = sourceVertex.gbId

    // get the target edges
    // when there is "TR" data between source vertex and target vertex,
    // it means source vertex knew target vertex already.
    // The target vertex cannot shown up in recommendation results
    val avoidTargetEdgeRDD = edgeRDD.filter(
      edge => edge.headVertexGbId == sourceGbId &&
        edge.getPropertyValueAsString(edgeTypePropertyKey).toLowerCase == trainStr
    )

    //get list of vertices' gbIds to avoid
    val avoidTargetGbIdsRDD = avoidTargetEdgeRDD.tailVerticesGbIds()

    //get unique list of vertices' gbIds to avoid
    val avoidGbIdsArray = avoidTargetGbIdsRDD.distinct().toArray()

    //filter target vertex RDD
    val targetVertexRDD = vertexRDD.filter {
      case vertex =>
        var keep = false
        if (vertex.getPropertyValueAsString(vertexTypePropertyKey).toLowerCase == targetVertexType) {
          keep = true
          for (i <- 0 until avoidGbIdsArray.length) {
            if (vertex.gbId == avoidGbIdsArray(i)) {
              keep = false
            }
          }
        }
        keep
    }

    // get the result vector of each target vertex
    val targetVectorRDD = targetVertexRDD.map {
      case vertex =>
        val targetVertexId = vertex.getPropertyValueAsString(targetIdPropertyKey)
        val resultVector = RecommendFeatureVector.parseResultArray(vertex,
          resultPropertyList, vectorValue, biasOn)
        TargetTuple(targetVertexId, resultVector)
    }
    targetVectorRDD.persist(StorageLevel.MEMORY_AND_DISK)

    //get the source vector
    val sourceVector = RecommendFeatureVector.parseResultArray(
      sourceVertex, resultPropertyList, vectorValue, biasOn)

    val ratingResultRDD = RecommendFeatureVector
      .predict(sourceVector, targetVectorRDD, biasOn)
      .collect()
      .sortBy(-_.score)
      .take(numOutputResults)

    var results = "================Top " + numOutputResults + " recommendations for " +
      sourceVertexName + " " + vertexId + "==========\n"
    ratingResultRDD.foreach { rating: Rating =>
      results += targetVertexName + "\t" + rating.vertexId + "\tscore\t" + rating.score + "\n"
    }

    targetVectorRDD.unpersist()
    sourceVertexRDD.unpersist()
    RecommendResult(results)
  }

}
