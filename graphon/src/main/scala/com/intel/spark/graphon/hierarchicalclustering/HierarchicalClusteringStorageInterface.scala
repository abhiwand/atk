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

package com.intel.spark.graphon.hierarchicalclustering

import java.io.Serializable

import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.schema.{ EdgeLabelDef, GraphSchema, PropertyDef, PropertyType }
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.graphbuilder.write.titan.TitanSchemaWriter
import com.intel.intelanalytics.domain.schema.GraphSchema
import com.thinkaurelius.titan.core.TitanGraph
import com.tinkerpop.blueprints.{ Edge, Vertex }

trait HierarchicalClusteringStorageInterface extends Serializable {

  def addSchema(): Unit

  def addVertexAndEdges(src: Long, dest: Long, metaNodeCount: Long, metaNodeName: String, iteration: Int): Long

  def commit(): Unit

  def shutdown(): Unit
}
