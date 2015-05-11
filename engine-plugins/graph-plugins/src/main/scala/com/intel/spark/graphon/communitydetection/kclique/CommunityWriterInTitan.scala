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

package com.intel.spark.graphon.communitydetection.kclique

import com.intel.graphbuilder.elements._
import org.apache.spark.rdd.RDD
import com.intel.graphbuilder.driver.spark.titan.{ GraphBuilderConfig, GraphBuilder }
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.graphbuilder.parser.InputSchema

/**
 * Write back to each vertex in Titan graph the set of communities to which it
 * belongs in some property specified by the user
 */

class CommunityWriterInTitan extends Serializable {

  /**
   * Update the graph by updating the vertices properties
   * @param gbVertices RDD of updated Graph Builder vertices list having new community property
   * @param gbEdges RDD of Graph Builder Edge list
   * @param titanConfig The titan configuration
   */
  def run(gbVertices: RDD[GBVertex], gbEdges: RDD[GBEdge], titanConfig: SerializableBaseConfiguration) {

    // Create the GraphBuilder object
    // Setting true to append for updating existing graph
    val gb = new GraphBuilder(new GraphBuilderConfig(new InputSchema(Seq.empty), List.empty, List.empty, titanConfig, append = true))

    // Build the graph using spark
    gb.buildGraphWithSpark(gbVertices, gbEdges)
  }

}