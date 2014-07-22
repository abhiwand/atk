
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

import com.intel.graphbuilder.elements._
import org.apache.spark.rdd.RDD
import com.intel.graphbuilder.driver.spark.titan.{ GraphBuilderConfig, GraphBuilder }
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.graphbuilder.parser.InputSchema
import com.intel.graphbuilder.write.dao.VertexDAO
import com.tinkerpop.blueprints
import com.tinkerpop.blueprints.Graph
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
/**
 * Write back to each vertex in Titan graph the set of communities to which it
 * belongs in some property specified by the user
 */

class CommunityWriterInTitan extends Serializable {

  /**
   *
   * @param gbVertices updated GB vertices list
   * @param gbEdges GB Edge list
   * @param titanConfigInput The titan configuration
   */
  def run(gbVertices: RDD[Vertex], gbEdges: RDD[Edge], titanConfigInput: SerializableBaseConfiguration) {

    val gb = new GraphBuilder(new GraphBuilderConfig(new InputSchema(Seq.empty), List.empty, List.empty, titanConfigInput))
    gb.buildGraphWithSpark(gbVertices, gbEdges)

  }

}
