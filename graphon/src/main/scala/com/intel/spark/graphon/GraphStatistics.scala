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

package com.intel.spark.graphon

import com.intel.graphbuilder.elements.Edge
import org.apache.spark.rdd.RDD

object GraphStatistics {

  /**
   * A parallel method to calculate the out degree
   * distribution of all vertices in a graph
   *
   * Look at [[examples.StatisticsExample]]
   * to understand an example use case
   *
   * @param edgeRDD RDD containing edges and their properties
   * @return        A list of Tuples of (VertexID, degree)
   */
  def outDegrees(edgeRDD: RDD[Edge]): RDD[(Any, Long)] = {

    val groupedByEdges = edgeRDD.groupBy(edge ⇒ edge.tailPhysicalId)
    groupedByEdges.map(e ⇒ (e._1, e._2.size))
  }

  /**
   * A parallel method to calculate the out degree
   * distribution of all vertices for a given edge
   * in a graph
   *
   * @param edgeRDD   RDD containing edges and their properties
   * @param edgeLabel Edge label to filter edges
   * @return          A list of Tuples of (VertexID, degree)
   */
  def outDegreesByEdgeType(edgeRDD: RDD[Edge], edgeLabel: String): RDD[(Any, Long)] = {

    val filteredEdges = edgeRDD.filter(edge ⇒ edge.label == edgeLabel)
    val groupedByEdges = filteredEdges.groupBy(edge ⇒ edge.tailPhysicalId)
    groupedByEdges.map(e ⇒ (e._1, e._2.size))
  }

  /**
   * A parallel method to calculate the in degree
   * distribution of all vertices in a graph
   *
   * Look at [[examples.StatisticsExample]]
   * to understand an example use case
   *
   * @param edgeRDD RDD containing edges and their properties
   * @return        A list of Tuples of (VertexID, degree)
   */
  def inDegrees(edgeRDD: RDD[Edge]): RDD[(Any, Long)] = {

    val groupedByEdges = edgeRDD.groupBy(edge ⇒ edge.headPhysicalId)
    groupedByEdges.map(e ⇒ (e._1, e._2.size))
  }

  /**
   * A parallel method to calculate the in degree
   * distribution of all vertices for a given edge
   * in a graph
   *
   * @param edgeRDD   RDD containing edges and their properties
   * @param edgeLabel Edge label to filter edges
   * @return          A list of Tuples of (VertexID, degree)
   */
  def inDegreesByEdgeType(edgeRDD: RDD[Edge], edgeLabel: String): RDD[(Any, Long)] = {

    val filteredEdges = edgeRDD.filter(edge ⇒ edge.label == edgeLabel)
    val groupedByEdges = filteredEdges.groupBy(edge ⇒ edge.headPhysicalId)
    groupedByEdges.map(e ⇒ (e._1, e._2.size))
  }
}
