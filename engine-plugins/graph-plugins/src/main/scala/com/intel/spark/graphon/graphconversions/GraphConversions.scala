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

package com.intel.spark.graphon.graphconversions

import com.intel.graphbuilder.elements.GBEdge
import org.apache.spark.graphx.{ Edge => GraphXEdge }

object GraphConversions {

  /**
   * Converts GraphBuilder edges (ATK internal representation) into GraphX edges.
   *
   * @param gbEdge Incoming ATK edge to be converted into a GraphX edge.
   * @param canonicalOrientation If true, edges are placed in a canonical orientation in which src < dst.
   * @return GraphX representation of the incoming edge.
   */
  def createGraphXEdgeFromGBEdge(gbEdge: GBEdge, canonicalOrientation: Boolean = false): GraphXEdge[Long] = {

    val srcId = gbEdge.tailPhysicalId.asInstanceOf[Long]

    val destId = gbEdge.headPhysicalId.asInstanceOf[Long]

    if (canonicalOrientation && srcId > destId)
      GraphXEdge[Long](destId, srcId)
    else
      GraphXEdge[Long](srcId, destId)
  }
}
