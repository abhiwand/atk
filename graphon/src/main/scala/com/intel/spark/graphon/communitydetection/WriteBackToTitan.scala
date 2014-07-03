
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
import com.intel.graphbuilder.elements.{ Edge => GBEdge }
import com.intel.spark.graphon.communitydetection.KCliquePercolationDataTypes._
import com.intel.graphbuilder.driver.spark.titan.reader.TitanReader
import com.intel.graphbuilder.driver.spark.rdd.VertexRDDFunctions
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.graphbuilder.graph.titan.TitanGraphConnector

/**
 * Write back to each vertex in Titan graph the set of communities to which it
 * belongs in some property specified by the user
 */

object WriteBackToTitan {

  def run(vertexAndCommunityList: RDD[(Long, Set[Long])], graphTableName: String, titanStorageHostName: String) = {

    /**
     * Convert the pairs of vertex and community list into GB vertex objects
     */
    val newGBVertex: RDD[Vertex] = vertexAndCommunityList
      .map({
        case (vertex, communityList) => Vertex(vertex,
          Property(TitanReader.TITAN_READER_DEFAULT_GB_ID, vertex),
          Seq(Property(COMMUNITY_PROPERTY_DEFAULT_LABEL, communityList)))
      })

    /**
     * Create graph connection
     */
    val titanConfig = new SerializableBaseConfiguration()
    titanConfig.setProperty("storage.backend", "hbase") // TODO : we should probably have this come from... somewhere
    titanConfig.setProperty("storage.hostname", titanStorageHostName)
    titanConfig.setProperty("storage.tablename", graphTableName)

    val titanConnector = new TitanGraphConnector(titanConfig)

    /**
     * Write back vertices to Titan and append in existing graph
     */
    val vertexRDDFunctions = new VertexRDDFunctions(newGBVertex)
    vertexRDDFunctions.write(titanConnector, true)
  }

}
