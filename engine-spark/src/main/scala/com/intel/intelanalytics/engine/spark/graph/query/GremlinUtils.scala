//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

package com.intel.intelanalytics.engine.spark.graph.query

import com.tinkerpop.blueprints.util.io.graphson.GraphSONMode
import com.tinkerpop.blueprints.{ Element, Graph }
import spray.json._

import scala.reflect.ClassTag

object GremlinUtils {

  /**
   * Serializes results of Gremlin query to JSON.
   *
   * @param graph Blueprint's graph
   * @param obj Results of Gremlin query
   * @param mode GraphSON mode which can be either normal, compact or extended
   *
   * @return Serialized query results
   */
  def serializeGremlinToJson[T: JsonFormat: ClassTag](graph: Graph,
                                                      obj: T,
                                                      mode: GraphSONMode = GraphSONMode.NORMAL): JsValue = {
    import com.intel.intelanalytics.engine.spark.graph.query.GremlinJsonProtocol._
    implicit val graphSONFormat = new GraphSONFormat(graph)
    val json = obj match {
      case null => JsNull
      case e: Element => e.toJson // Needed to identify correct implicit for Blueprint's vertices and edges
      case s: java.util.Set[Integer] => s.toString.toJson // WORK AROUND .. long term fix needs to support all Java collections
      case x => x.toJson
    }
    json
  }

  /**
   * Deserializes JSON into a Scala object.
   *
   * @param graph Blueprint's graph
   * @param json Json objects
   * @param mode GraphSON mode which can be either normal, compact or extended
   *
   * @return Deserialized query results
   */
  def deserializeJsonToGremlin[T: JsonFormat: ClassTag](graph: Graph,
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
   * @param name Name of GraphSON mode. Supported names are: "normal", "compact", and "extended".
   * @return GraphSON mode type (defaults to GraphSONMode.NORMAL)
   */
  def getGraphSONMode(name: String): GraphSONMode = name match {
    case "normal" => GraphSONMode.NORMAL
    case "compact" => GraphSONMode.COMPACT
    case "extended" => GraphSONMode.EXTENDED
    case x => throw new IllegalArgumentException(s"Unsupported GraphSON mode: $x. " +
      "Supported values are: normal, compact, and extended.")
  }
}
