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

package com.intel.spark.graphon.graphstatistics

import com.intel.graphbuilder.elements.{ GBEdge, GBVertex, Property }
import org.apache.spark.SparkContext._
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD

/**
 * Object for calculating weighted degree and its generalizations: Per-vertex statistics about the net weight of
 * edges incident to that vertex.
 *
 * Currently supported statistics:
 * - per vertex weighted in-degree (optionally restricted to a set of edge labels)
 * - per vertex weighted out-degree (optionally restricted to a set of edge labels)
 * - per vertex weighted undirected degree (optionally restricted to a set of edge labels)
 */
object WeightedDegrees {

  /**
   * Calculates the weighted out-degree of each vertex using edges of all possible labels.
   *
   * @param vertexRDD RDD of vertices
   * @param edgeRDD RDD of edges
   * @param weightProperty Option containing name of the edge property containing the edge weight.
   * @param defaultWeight Default edge weight if the property is not present on an edge.
   * @return RDD of (Vertex, weighted out-degree) pairs
   */
  def outWeight(vertexRDD: RDD[GBVertex], edgeRDD: RDD[GBEdge], weightProperty: Option[String], defaultWeight: Double): RDD[(GBVertex, Double)] = {

    weightedDegreeCalculation(vertexRDD, edgeRDD, weightProperty, defaultWeight, calculateOutDegreeFlag = true)
  }

  /**
   * Calculates the weighted in-degree of each vertex using edges of all possible labels.
   *
   * @param vertexRDD RDD of vertices
   * @param edgeRDD RDD of edges
   * @param weightProperty Option containing the ame of the edge property containing the edge weight.
   * @param defaultWeight Default edge weight if the property is not present on an edge.
   * @return RDD of (Vertex, weighted in-degree) pairs
   */
  def inWeight(vertexRDD: RDD[GBVertex], edgeRDD: RDD[GBEdge], weightProperty: Option[String], defaultWeight: Double): RDD[(GBVertex, Double)] = {
    weightedDegreeCalculation(vertexRDD, edgeRDD, weightProperty, defaultWeight, calculateOutDegreeFlag = false)
  }

  /**
   * Calculates the weighted undirected degree of each vertex using edges of all possible labels.
   * Assumes that all provided edge labels are for undirected edges.
   *
   * @param vertexRDD RDD of vertices
   * @param edgeRDD RDD of edges
   * @param weightProperty Option containing the ame of the edge property containing the edge weight.
   * @param defaultWeight Default edge weight if the weight property is not present on an edge.
   * @return RDD of (Vertex, weighted degree) pairs
   */
  def undirectedWeightedDegree(vertexRDD: RDD[GBVertex], edgeRDD: RDD[GBEdge], weightProperty: Option[String], defaultWeight: Double): RDD[(GBVertex, Double)] = {
    /*
     * Because undirected edges are internally represented as bi-directional edge/anti-edge pairs,
     * this is simply the out degree calculation. A change of the representation would change this
     * calculation.
     */
    weightedDegreeCalculation(vertexRDD, edgeRDD, weightProperty, defaultWeight, calculateOutDegreeFlag = true)
  }

  /**
   * Calculates the weighted out-degree of each vertex using edges from a given set of edge labels.
   *
   * @param vertexRDD RDD of vertices
   * @param edgeRDD RDD of edges
   * @param weightProperty Option containing the name of the edge property containing the edge weight.
   * @param defaultWeight Default edge weight if the property is not present on an edge.
   * @param edgeLabels Set of edge labels for which to calculate out-degrees
   * @return RDD of (VertexID, weighted out-degree with respect to the set of considered edge labels) pairs
   */
  def outDegreesByEdgeLabel(vertexRDD: RDD[GBVertex], edgeRDD: RDD[GBEdge], weightProperty: Option[String], defaultWeight: Double, edgeLabels: Option[Set[String]]): RDD[(GBVertex, Double)] = {
    val filteredEdges = filterEdges(edgeRDD, edgeLabels)
    outWeight(vertexRDD, filteredEdges, weightProperty, defaultWeight)
  }

  /**
   * Calculates the weighted in-degree of each vertex using edges from a given set of edge labels.
   *
   * @param vertexRDD RDD containing vertices
   * @param edgeRDD RDD containing edges
   * @param weightProperty Option containing the name of the edge property containing the edge weight.
   * @param defaultWeight Default edge weight if the property is not present on an edge.
   * @param edgeLabels Set of dge label for which to calculate in-degrees
   * @return RDD of (VertexID, weighted in-degree with respect to set of considered edge labels) pairs
   */
  def inWeightByEdgeLabel(vertexRDD: RDD[GBVertex], edgeRDD: RDD[GBEdge], weightProperty: Option[String], defaultWeight: Double, edgeLabels: Option[Set[String]]): RDD[(GBVertex, Double)] = {
    val filteredEdges = filterEdges(edgeRDD, edgeLabels)
    inWeight(vertexRDD, filteredEdges, weightProperty, defaultWeight)
  }

  /**
   * Calculates the weighted undirected degree of each vertex using edges from a given set of edge labels.
   * Assumes that all provided edge labels are for undirected edges.
   *
   * @param vertexRDD RDD of vertices
   * @param edgeRDD RDD of edges
   * @param weightProperty Option containing the name of the edge property containing the edge weight.
   * @param defaultWeight Default edge weight if the property is not present on an edge.
   * @param edgeLabels Set of edge labels for which to calculate degrees
   * @return RDD of (VertexID, weighted degree with respect to the set of considered edge labels) pairs
   */
  def undirectedWeightedDegreeByEdgeLabel(vertexRDD: RDD[GBVertex], edgeRDD: RDD[GBEdge], weightProperty: Option[String], defaultWeight: Double, edgeLabels: Option[Set[String]]): RDD[(GBVertex, Double)] = {
    val filteredEdges = filterEdges(edgeRDD, edgeLabels)
    outWeight(vertexRDD, filteredEdges, weightProperty, defaultWeight)
  }

  private def weightedDegreeCalculation(vertexRDD: RDD[GBVertex],
                                        edgeRDD: RDD[GBEdge],
                                        weightPropertyOption: Option[String],
                                        defaultWeight: Double,
                                        calculateOutDegreeFlag: Boolean): RDD[(GBVertex, Double)] = {

    val weightProperty = weightPropertyOption.getOrElse("not using a weight property, using the default")
    val useDefaultEdgeWeight = weightPropertyOption.isEmpty

    /*
     * To make sure that we handle degree 0 vertices correctly (an especially important case when there are multiple
     * kinds of edges) we must include the vertexRDD in the calculation as well as the edgeRDD.
     *
     * A further complication is that each edge knows only the vertex IDs of its endpoints, not the data stored at
     * those endpoints.
     *
     * The calculation proceeds by the aggregation of (vertex, degree) records keyed by the vertex ID.
     * Each vertex emits (vertexData, degree = 0) keyed by the vertex ID
     * Each edges emits (None, degree = 1) keyed by the vertex ID of the head (if in-degree) or tail (if out-degree)
     *
     * The aggregation sums the net degrees and takes the non-empty vertex data it is provided.
     * In a well-formed graph, vertex IDs are unique, and each vertex ID key will see exactly one record with
     * nonempty vertex data in its aggregation.
     */

    val vertexVDRs: RDD[(Any, VertexDegreeRecord)] =
      vertexRDD.map(gbVertex => (gbVertex.physicalId, VertexDegreeRecord(Some(gbVertex), 0D)))

    val edgeVDRs: RDD[(Any, VertexDegreeRecord)] =
      if (calculateOutDegreeFlag)
        edgeRDD.map(e => (e.tailPhysicalId,
          VertexDegreeRecord(None, getEdgeWeight(e, weightProperty, defaultWeight, useDefaultEdgeWeight))))
      else
        edgeRDD.map(e => (e.headPhysicalId,
          VertexDegreeRecord(None, getEdgeWeight(e, weightProperty, defaultWeight, useDefaultEdgeWeight))))

    val vdrs = vertexVDRs.union(edgeVDRs)

    val combinedVDRs: RDD[VertexDegreeRecord] =
      vdrs.combineByKey(x => x, mergeVertexAndDegrees, mergeVertexAndDegrees).map(_._2)

    // there will be a get on an empty option only if there exists a vertex in the EdgeRDD that is
    // not in the VertexRDD... if this happens something was wrong  with the incoming data

    combinedVDRs.map(vad => (vad.vertexOption.get, vad.weightedDegree))
  }

  private case class VertexDegreeRecord(vertexOption: Option[GBVertex], weightedDegree: Double)

  private def mergeVertexAndDegrees(vad1: VertexDegreeRecord, vad2: VertexDegreeRecord) = {
    require(vad1.vertexOption.isEmpty || vad2.vertexOption.isEmpty)
    val v = if (vad1.vertexOption.isDefined) vad1.vertexOption else vad2.vertexOption
    VertexDegreeRecord(v, vad1.weightedDegree + vad2.weightedDegree)
  }

  private def filterEdges(edgeRDD: RDD[GBEdge], edgeLabels: Option[Set[String]]): RDD[GBEdge] = {
    if (edgeLabels.nonEmpty) {
      edgeRDD.filter(edge => edgeLabels.get.contains(edge.label))
    }
    else {
      edgeRDD
    }
  }

  private def getEdgeWeight(e: GBEdge, propertyName: String, defaultValue: Double, useDefault: Boolean): Double = {
    if (useDefault) {
      defaultValue
    }
    else {
      val propertyOption: Option[Property] = e.getProperty(propertyName)

      if (propertyOption.isEmpty) {
        defaultValue
      }
      else {
        propertyOption.get.value match {
          case d: Double => d
          case f: Float => f.toDouble
          case i: Int => i.toDouble
          case l: Long => l.toDouble
          case bad => throw new SparkException("WeightedDegrees: At edge ID, " + e.id
            + ", between source " + e.tailPhysicalId + " and destination " + e.headPhysicalId
            + ", the property " + propertyName + " contains non-numeric data: " + bad)

        }
      }
    }
  }
}
