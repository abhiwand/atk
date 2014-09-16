
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

package com.intel.spark.graphon.communitydetection.kclique

import com.intel.graphbuilder.elements.{ Vertex, Property }
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import com.intel.graphbuilder.driver.spark.titan.reader.TitanReader
import com.intel.spark.graphon.communitydetection.ScalaToJavaCollectionConverter

/**
 * Class to set the vertex Ids as required by Graph Builder, by formatting as (physicalId, gbId, propertyList)
 * @param gbVertices graph builder vertices list of the input graph
 * @param vertexCommunitySet pair of vertex Id and set of communities
 */
class GBVertexRDDBuilder(gbVertices: RDD[Vertex], vertexCommunitySet: RDD[(Long, Set[Long])]) extends Serializable {

  /**
   * Set the vertex as required by graph builder with new community property
   * @param communityPropertyLabel the label of the community property (provided by user)
   * @return RDD of graph builder Vertices having new community property
   */
  def setVertex(communityPropertyLabel: String): RDD[Vertex] = {

    val emptySet: Set[Long] = Set()

    // Map the GB Vertex IDs to key-value pairs where the key is the GB Vertex ID set and the value is the emptySet.
    // The empty set will be considered as the empty communities for the vertices that don't belong to any communities.
    val gbVertexIDEmptySetPairs: RDD[(Long, Set[Long])] = gbVertices
      .map((v: Vertex) => v.physicalId.asInstanceOf[Long])
      .map(id => (id, emptySet))

    // Combine the vertices having communities with the vertices having no communities together, to have
    // the complete list of vertices
    val gbVertexIdCombinedWithEmptyCommunity: RDD[(Long, Set[Long])] =
      gbVertexIDEmptySetPairs.union(vertexCommunitySet).combineByKey(
        (x => x),
        ({ case (x, y) => y.union(x) }),
        ({ case (x, y) => y.union(x) }))

    // Convert the pair of vertex and community set into a GB Vertex
    val newGBVertices: RDD[Vertex] = gbVertexIdCombinedWithEmptyCommunity.map({
      case (vertexId, communitySet) => Vertex(
        java.lang.Long.valueOf(vertexId),
        Property(TitanReader.TITAN_READER_DEFAULT_GB_ID, vertexId),
        Set(Property(communityPropertyLabel, ScalaToJavaCollectionConverter.convertSet(communitySet))))
    })
    newGBVertices
  }

}
