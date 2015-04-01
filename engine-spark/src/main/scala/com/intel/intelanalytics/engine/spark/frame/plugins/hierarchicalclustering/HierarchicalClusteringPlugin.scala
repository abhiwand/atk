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

package com.intel.intelanalytics.engine.spark.frame.plugins.hierarchicalclustering

import com.intel.intelanalytics.UnitReturn
import com.intel.intelanalytics.domain.frame.FrameReference
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.context.SparkContextFactory
import com.intel.intelanalytics.engine.spark.frame.SparkFrameData
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import com.intel.intelanalytics.domain.DomainJsonProtocol

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Parameters for executing connected components.
 * @param graph Reference to the graph object on which to compute connected components.
 * @param output_property Name of the property to which connected components value will be stored on vertex and edge.
 * @param output_graph_name Name of output graph.
 */
case class HierarchicalClusteringArgs(frame: FrameReference)

/** Json conversion for arguments and return value case classes */
object HierarchicalClusteringFormat {
  import DomainJsonProtocol._
  implicit val hFormat = jsonFormat1(HierarchicalClusteringArgs)
}

import HierarchicalClusteringFormat._

/**
 * ConnectedComponent plugin implements the connected components computation on a graph by invoking graphx api.
 *
 * Pulls graph from underlying store, sends it off to the ConnectedComponentGraphXDefault, and then writes the output graph
 * back to the underlying store.
 *
 * Right now it is using only Titan for graph storage. Other backends including Parquet will be supported later.
 */
class HierarchicalClusteringPlugin extends SparkCommandPlugin[HierarchicalClusteringArgs, UnitReturn] {

  override def name: String = "frame:/hierarchical_clustering"

  //TODO remove when we move to the next version of spark
  override def kryoRegistrator: Option[String] = None

  override def execute(arguments: HierarchicalClusteringArgs)(implicit invocation: Invocation): UnitReturn = {

    sc.addJar(SparkContextFactory.jarPath("graphon"))

    val frame = engine.frames.expectFrame(arguments.frame)
    val data = engine.frames.loadFrameData(sc, frame)
    val d = data.mapRows(row => row.valuesAsArray()).collect()
    HierarchicalClusteringImpl.execute(data)

    new UnitReturn
  }

}
