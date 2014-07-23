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

/**
 * Get recommendation to either left-side or right-side vertices
 * The prerequsite is at least one of two algorithms (ALS or CGD) has been run before this query
 *
 * @param graph The graph reference
 * @param vertex_id The vertex id to get recommendation for.   *
 * @param vertex_type The vertex type to get recommendation for. The valid value is either "L" or "R".
 *                     "L" stands for left-side vertices of a bipartite graph.
 *                     "R" stands for right-side vertices of a bipartite graph.
 *                     For example, if your input data is "user,movie,rating" and you want to get recommendation
 *                     on user, please input "L" because user is your left-side vertex. Similarly, please input
 *                     "R if you want to get recommendation for movie.
 *                     The default value is "L"
 * @param output_vertex_property_list The property name for ALS/CGD results. The default value is "als_result"
 *                                    When bias is enabled, the last property name in the output_vertex_property_list
 *                                    is for bias.
 * @param vertex_type_property_key  The property name for vertex type. The default value is "vertex_type"
 * @param edge_type_property_key The property name for edge type. The default value "splits".
 *                                 We need this name to know data is in train, validation or test splits
 * @param vector_value Whether ALS/CDG results are saved in a vector for each vertex. The default value is "true"
 * @param bias_on Whether bias turned on/off for ALS/CDG calculation.
 *                When bias is enabled, the last property name in the output_vertex_property_list is for bias.
 *                The default value is "false"
 * @param train_str The label for training data. The default value is "tr"
 * @param num_output_results The number of recommendations to output. The default value is 10
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

class RecommendQuery extends SparkCommandPlugin[RecommendParams, RecommendResult] {

  import com.intel.intelanalytics.domain.DomainJsonProtocol._

  implicit val recommendParamsFormat = jsonFormat14(RecommendParams)
  implicit val recommendResultFormat = jsonFormat1(RecommendResult)

  override def execute(invocation: SparkInvocation, arguments: RecommendParams)(implicit user: UserPrincipal, executionContext: ExecutionContext): RecommendResult = {
    import scala.concurrent.duration._

    System.out.println("*********Start to execute Recommend query********")
    val config = configuration
    val graphFuture = invocation.engine.getGraph(arguments.graph.id)
    val graph = Await.result(graphFuture, config.getInt("default-timeout") seconds)
    val pattern = ","
    val outputVertexPropertyList = arguments.output_vertex_property_list.getOrElse(
      config.getString("output_vertex_property_list"))
    val resultPropertyList = outputVertexPropertyList.split(pattern)
    val vectorValue = arguments.vector_value.getOrElse(config.getString("vector_value"))
    val biasOn = arguments.bias_on.getOrElse(config.getString("bias_on"))
    require(resultPropertyList.size >= 1,
      "Please input at least one vertex property name for ALS/CGD results")
    require(vectorValue == "false" || biasOn == "false" ||
      (vectorValue == "true" && biasOn == "true" && resultPropertyList.size == 2),
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
      if (vertexType == config.getString("vertex_type")) {
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
      vertex => {
        if (vertex.getStringPropertyValue(sourceIdPropertyKey) == vertexId &&
          vertex.getStringPropertyValue(vertexTypePropertyKey).toLowerCase == vertexType) true
        else false
      })
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
    val sourceGbId = sourceVertex.gbId //.value.toString

    // get the target edges
    // when there is "TR" data between source vertex and target vertex,
    // it means source vertex knew target vertex already.
    // The target vertex cannot shown up in recommendation results
    val avoidTargetEdgeRDD = edgeRDD.filter {
      edge =>
        {
          if (edge.getHeadVertexGbId() /*.value.toString*/ == sourceGbId &&
            edge.getStringPropertyValue(edgeTypePropertyKey).toLowerCase == trainStr) true
          else false
        }
    }

    //get valid target vertices' gbIds
    val avoidTargetGbIdsRDD = avoidTargetEdgeRDD.tailVerticesGbIds()

    //get unique target vertices' gbIds
    val avoidGbIdsArray = avoidTargetGbIdsRDD.distinct().toArray

    //filter target vertex RDD
    val targetVertexRDD = vertexRDD.filter {
      case vertex =>
        var keep = false
        if (vertex.getStringPropertyValue(vertexTypePropertyKey).toLowerCase == targetVertexType) {
          keep = true
          for (i <- 0 until avoidGbIdsArray.length) {
            if (vertex.gbId /*.value.toString*/ == avoidGbIdsArray(i)) {
              keep = false
            }
          }
        }
        keep
    }

    // get the result vector of each target vertex
    val targetVectorRDD = targetVertexRDD.map {
      case vertex =>
        val targetVertexId = vertex.getStringPropertyValue(targetIdPropertyKey)
        val resultVector = RecommendFeatureVector.parseResultArray(vertex, resultPropertyList, vectorValue, biasOn)
        TargetTuple(targetVertexId, resultVector)
    }
    targetVectorRDD.persist(StorageLevel.MEMORY_AND_DISK)

    //get the source vector
    val sourceVector = RecommendFeatureVector.parseResultArray(
      sourceVertex, resultPropertyList, vectorValue, biasOn)

    val ratingResultRDD = RecommendFeatureVector
      .predict(sourceVector, targetVectorRDD)
      .collect()
      .sortBy(-_.score)
      .take(numOutputResults)

    var i = 1
    var results = "Top " + numOutputResults + " recommendation for " + sourceVertexName + " " + vertexId + "\n"
    ratingResultRDD.foreach { rating: Rating =>
      results += targetVertexName + "\t" + rating.vertexId + "\tscore\t" + rating.score + "\n"
      i += 1
    }

    targetVectorRDD.unpersist()
    sourceVertexRDD.unpersist()

    //RecommendResult(ratingResultRDD.toString)
    RecommendResult(results)

  }

  //TODO: Replace with generic code that works on any case class
  def parseArguments(arguments: JsObject) = arguments.convertTo[RecommendParams]

  //TODO: Replace with generic code that works on any case class
  def serializeReturn(returnValue: RecommendResult): JsObject = returnValue.toJson.asJsObject

  /**
   * The name of the command, e.g. graphs/query/recommend
   */
  override def name: String = "graphs/query/recommend"

  //TODO: Replace with generic code that works on any case class
  override def serializeArguments(arguments: RecommendParams): JsObject = arguments.toJson.asJsObject()
}