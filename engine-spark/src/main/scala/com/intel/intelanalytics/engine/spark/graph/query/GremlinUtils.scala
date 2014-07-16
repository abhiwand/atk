package com.intel.intelanalytics.engine.spark.graph.query

import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.thinkaurelius.titan.core.TitanGraph
import com.tinkerpop.blueprints.Element
import com.tinkerpop.blueprints.util.io.graphson.GraphSONMode
import com.tinkerpop.pipes.util.structures.Row
import com.typesafe.config.Config
import spray.json._

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

object GremlinUtils {

  /**
   * Default settings for Gremlin queries.
   *
   * @param config Default configuration
   * @param path Paths are dot-separated expressions such as foo.bar.baz
   * @return Titan configuration with default settings specified in the path expression.
   */
  def getTitanConfiguration(config: Config, path: String): SerializableBaseConfiguration = {
    val titanConfiguration = new SerializableBaseConfiguration
    val titanLoadConfig = config.getConfig(path)
    for (entry <- titanLoadConfig.entrySet().asScala) {
      titanConfiguration.addProperty(entry.getKey, titanLoadConfig.getString(entry.getKey))
    }
    titanConfiguration
  }

  /**
   * Serializes results of Gremlin query to JSON.
   *
   * @param graph Titan graph
   * @param obj Results of Gremlin query
   * @param mode GraphSON mode which can be either normal, compact or extended
   *
   * @return Serialized query results
   */
  def serializeGremlinToJson[T: JsonFormat: ClassTag](graph: TitanGraph,
                                                      obj: T,
                                                      mode: GraphSONMode = GraphSONMode.NORMAL): JsValue = {
    import com.intel.intelanalytics.engine.spark.graph.query.GremlinJsonProtocol._
    implicit val graphSONFormat = new GraphSONFormat(graph)
    val json = obj match {
      case e: Element => e.toJson
      case r: Row[T] => r.toJson
      case x => x.toJson
    }
    json
  }

  /**
   * Deserializes JSON into a Scala object.
   *
   * @param graph Titan graph
   * @param json Json objects
   * @param mode GraphSON mode which can be either normal, compact or extended
   *
   * @return Deserialized query results
   */
  def deserializeJsonToGremlin[T: JsonFormat: ClassTag](graph: TitanGraph,
                                                        json: JsValue,
                                                        mode: GraphSONMode = GraphSONMode.NORMAL): T = {
    import com.intel.intelanalytics.engine.spark.graph.query.GremlinJsonProtocol._
    implicit val graphSONFormat = new GraphSONFormat(graph)
    val obj = json match {
      case x if isGraphElement(x) => graphSONFormat.read(json).asInstanceOf[T]
      case x => x.convertTo[T]
    }
    obj
  }

  /**
   * Get the GraphSON mode type from a string.
   *
   * @param name Name of GraphSON mode
   * @return GraphSON mode type (defaults to GraphSONMode.NORMAL)
   */
  def getGraphSONMode(name: String): GraphSONMode = name match {
    case "compact" => GraphSONMode.COMPACT
    case "extended" => GraphSONMode.EXTENDED
    case _ => GraphSONMode.NORMAL
  }
}
