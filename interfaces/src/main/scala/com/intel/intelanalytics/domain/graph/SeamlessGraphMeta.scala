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

package com.intel.intelanalytics.domain.graph

import com.intel.intelanalytics.domain.frame.DataFrame
import com.intel.intelanalytics.domain.schema.VertexSchema

/**
 * Wrapper for Seamless Graph Meta data stored in the database
 *
 * "Seamless Graph" is a graph that provides a "seamless user experience" between graphs and frames.
 * The same data can be treated as frames one moment and as a graph the next without any import/export.
 *
 * @param graphMeta the graph meta data
 * @param frameMetas the vertex and edge frames owned by this graph (might be empty but never null)
 */
case class SeamlessGraphMeta(graphMeta: Graph, frameMetas: List[DataFrame]) {
  require(graphMeta != null, "graph is required")
  require(frameMetas != null, "frame is required, it can be empty but not null")
  require(graphMeta.storageFormat == "ia/frame", "Storage format should be ia/frame")
  frameMetas.foreach(frame => require(frame.graphId.isDefined, "frame should be owned by the graph, graphId is required"))
  frameMetas.foreach(frame => require(frame.graphId.get == graphMeta.id, "frame should be owned by the graph, graphId did not match"))

  /** Labels to frames */
  @transient private lazy val edgeFrameMetasMap = frameMetas.filter(frame => frame.isEdgeFrame)
    .map(frame => (frame.label.get, frame))
    .toMap[String, DataFrame]

  /** Labels to frames */
  @transient private lazy val vertexFrameMetasMap = frameMetas.filter(frame => frame.isVertexFrame)
    .map(frame => (frame.label.get, frame))
    .toMap[String, DataFrame]

  /** convenience method for getting the id of the graph */
  def id: Long = graphMeta.id

  /** Next unique id for edges and vertices */
  def nextId(): Long = {
    graphMeta.nextId()
  }

  /**
   * Convenience method for getting a reference to this graph
   */
  def graphReference: GraphReference = {
    GraphReference(id)
  }

  /**
   * Get the frame meta data for an edge list in this graph
   * @param label the type of edges
   * @return frame meta data
   */
  def edgeMeta(label: String): DataFrame = {
    edgeFrameMetasMap.getOrElse(label, throw new IllegalArgumentException(s"No edge frame with label $label in this graph.  Defined edge labels: $edgeLabelsAsString"))
  }

  /**
   * Get the frame meta data for an vertex list in this graph
   * @param label the type of vertices
   * @return frame meta data
   */
  def vertexMeta(label: String): DataFrame = {
    vertexFrameMetasMap.getOrElse(label, throw new IllegalArgumentException(s"No vertex frame with label $label in this graph.  Defined vertex labels: $vertexLabelsAsString"))
  }

  /**
   * True if the supplied label is already in use in this graph
   */
  def isVertexOrEdgeLabel(label: String): Boolean = {
    frameMetas.exists(frame => frame.label.get == label)
  }

  /**
   * True if the supplied label is a vertex label within this graph
   */
  def isVertexLabel(label: String): Boolean = {
    vertexFrameMetasMap.contains(label)
  }

  /**
   * Get a list of  all vertex frames for this graph
   * @return list of frame meta data
   */
  def vertexFrames: List[DataFrame] = {
    vertexFrameMetasMap.map {
      case (label: String, frame: DataFrame) => frame
    }.toList
  }

  /**
   * Get a list of  all vertex frames for this graph
   * @return list of frame meta data
   */
  def edgeFrames: List[DataFrame] = {
    edgeFrameMetasMap.map {
      case (label: String, frame: DataFrame) => frame
    }.toList
  }

  def vertexLabels: List[String] = {
    vertexFrames.map(frame => frame.label.get)
  }

  def vertexLabelsAsString: String = {
    vertexLabels.mkString(", ")
  }

  def edgeLabels: List[String] = {
    edgeFrames.map(frame => frame.label.get)
  }

  def edgeLabelsAsString: String = {
    edgeLabels.mkString(", ")
  }

  def vertexCount: Long = {
    vertexFrames.map(frame => frame.rowCount).reduce(_ + _)
  }

  def edgeCount: Long = {
    edgeFrames.map(frame => frame.rowCount).reduce(_ + _)
  }

  /**
   * Create a list of ElementIDName objects corresponding to the IDColumn of all Vertices in this graph.
   */
  def vertexIdColumnNames: List[ElementIDName] = {
    this.vertexFrameMetasMap.map {
      case (name, frame) => new ElementIDName(name, frame.schema.asInstanceOf[VertexSchema].idColumnName.getOrElse("_vid"))
    }.toList
  }

}

