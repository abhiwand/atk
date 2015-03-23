//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
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

package com.intel.graphbuilder.driver.spark.titan

import java.text.NumberFormat

import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRddImplicits._
import com.intel.graphbuilder.graph.titan.{ TitanGraphCacheListener, TitanGraphConnector }
import com.intel.graphbuilder.parser.rule._
import com.intel.graphbuilder.elements.{ GBEdge, GBVertex }
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * This is a GraphBuilder that runs on Spark, uses a RuleParser and creates Graphs in Titan.
 * <p>
 * This class wraps the Spark data flow and gives an example of how you can compose a
 * Graph Building utility from the other classes.
 * </p>
 *
 * @param config configuration options
 */
class GraphBuilder(config: GraphBuilderConfig) extends Serializable {

  val titanConnector = new TitanGraphConnector(config.titanConfig)
  val titanSchemaManager = new InferSchemaManager(config)
  val vertexParser = new VertexRuleParser(config.inputSchema.serializableCopy, config.vertexRules)
  val edgeParser = new EdgeRuleParser(config.inputSchema.serializableCopy, config.edgeRules)

  /**
   * Build the Graph, both Edges and Vertices from one source.
   *
   * @param inputRdd the input rows to create the graph from
   */
  def build(inputRdd: RDD[Seq[_]]) {
    build(inputRdd, inputRdd)
  }

  /**
   * Build the Graph, separate sources for Edges and Vertices
   *
   * @param vertexInputRdd the input rows to create the vertices from
   * @param edgeInputRdd the input rows to create the edges from
   */
  def build(vertexInputRdd: RDD[Seq[_]], edgeInputRdd: RDD[Seq[_]]) {
    if (config.inferSchema) {
      titanSchemaManager.writeSchemaFromRules()
    }

    println("Parse and Write Vertices")
    val vertices = vertexInputRdd.parseVertices(vertexParser)
    val edges = edgeInputRdd.parseEdges(edgeParser)

    buildGraphWithSpark(vertices, edges)
  }

  /**
   * Build the Graph using Spark
   *
   * @param vertexRdd RDD of Vertex objects
   * @param edgeRdd RDD of Edge objects
   */
  def buildGraphWithSpark(vertexRdd: RDD[GBVertex], edgeRdd: RDD[GBEdge]) {

    var vertices = vertexRdd
    var edges = edgeRdd

    if (config.retainDanglingEdges) {
      println("retain dangling edges was true so we'll create extra vertices from edges")
      vertices = vertices.union(edges.verticesFromEdges())
    }

    if (config.inferSchema && titanSchemaManager.needsToInferSchemaFromData) {
      println("inferring schema from data")
      titanSchemaManager.writeSchemaFromData(edges, vertices)
    }

    val mergedVertices = vertices.mergeDuplicates()
    val idMap = mergedVertices.write(titanConnector, config.append)
    idMap.persist(StorageLevel.MEMORY_AND_DISK_SER)
    println("done parsing and writing, vertices count: " + NumberFormat.getInstance().format(idMap.count()))

    val broadcastJoinThreshold = titanConnector.config.getLong("auto-partitioner.broadcast-join-threshold", 0)

    if (config.broadcastVertexIds || JoinBroadcastVariable.useBroadcastVariable(idMap, broadcastJoinThreshold)) {
      val vertexMap = idMap.map(gbIdToPhysicalId => gbIdToPhysicalId.toTuple)

      println("broadcasting vertex ids")
      val vertexMapSize = JoinBroadcastVariable
      val gbIdToPhysicalIdMap = JoinBroadcastVariable(vertexMap)

      println("starting write of edges")
      edges.write(titanConnector, gbIdToPhysicalIdMap, config.append)
    }
    else {
      println("join edges with physical ids")
      val edgesWithPhysicalIds = edges.joinWithPhysicalIds(idMap)

      println("starting write of edges")
      edgesWithPhysicalIds.write(titanConnector, config.append)
    }

    // Manually Unpersist RDDs to help with Memory usage
    idMap.unpersist(blocking = false)

    println("done writing edges")
  }

}
