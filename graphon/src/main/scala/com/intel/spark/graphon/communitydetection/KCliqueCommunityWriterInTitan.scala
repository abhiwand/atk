
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

package com.intel.spark.graphon.communitydetection

import com.intel.graphbuilder.elements._
import org.apache.spark.rdd.RDD
import com.intel.graphbuilder.driver.spark.titan.reader.TitanReader
import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRDDImplicits._
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import collection.JavaConverters._

/**
 * Write back to each vertex in Titan graph the set of communities to which it
 * belongs in some property specified by the user
 */

class KCliqueCommunityWriterInTitan {

  /**
   *
   * @param vertexAndCommunitySet an RDD of pair of vertex Id and list of communities it belongs to.
   *                               Vertex Id is of type Long and the list of communities is Set[Long]
   * @param communityPropertyDefaultLabel name of the community property of vertex that will be updated/created in the input graph
   * @param titanConnector The titan graph connector
   */
  def run(vertexAndCommunitySet: RDD[(Long, Set[Long])], communityPropertyDefaultLabel: String, titanConnector: TitanGraphConnector) {
    //  To create a GB Vertex from the (Long, Set[Long]) pair in vertexAndCommunitySet.
    //  The ID (the long) is both the physical ID and the GB ID. Since we need a GB ID to
    //  create the vertex, we need to create some default property (TitanReader.TITAN_READER_DEFAULT_GB_ID)
    //  and stick the physical ID in there as well (so the property list will have two entries:
    //  the GB ID and the community list)
    //  The label of the communitySet property is defined as communityPropertyDefaultLabel

    //  Converted communitySet from scala Set to Java.util.Set using .asJava decorator method of JavaConverters.
    //  We need to use Java objects while actually storing to Titan using GraphBuilder.

    val newGBVertex: RDD[Vertex] = vertexAndCommunitySet
      .map({
        case (vertexId, communitySet) => Vertex(vertexId,
          Property(TitanReader.TITAN_READER_DEFAULT_GB_ID, vertexId),
          Seq(Property(communityPropertyDefaultLabel, communitySet.asJava)))
      })

    //  Write back vertices to Titan and append in existing graph
    val vertexRDDFunctions = vertexRDDToVertexRDDFunctions(newGBVertex)
    vertexRDDFunctions.write(titanConnector, append = true)
  }

}
