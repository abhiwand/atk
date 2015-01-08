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
import com.intel.intelanalytics.domain.schema.{ Schema, VertexSchema }

/**
 * Wrapper for Seamless Graph Meta data stored in the database
 *
 * "Seamless Graph" is a graph that provides a "seamless user experience" between graphs and frames.
 * The same data can be treated as frames one moment and as a graph the next without any import/export.
 *
 * @param graphEntity the graph meta data
 * @param frameEntities the vertex and edge frames owned by this graph (might be empty but never null)
 */
case class SeamlessGraphMeta(graphEntity: Graph, frameEntities: List[DataFrame]) {
  require(graphEntity != null, "graph is required")
  require(frameEntities != null, "frame is required, it can be empty but not null")
  require(graphEntity.storageFormat == "ia/frame", "Storage format should be ia/frame")
  frameEntities.foreach(frame => require(frame.graphId.isDefined, "frame should be owned by the graph, graphId is required"))
  frameEntities.foreach(frame => require(frame.graphId.get == graphEntity.id, "frame should be owned by the graph, graphId did not match"))

  /** Labels to frames */
  @transient private lazy val edgeFrameMetasMap = frameEntities.filter(frame => frame.isEdgeFrame)
    .map(frame => (frame.label.get, frame))
    .toMap[String, DataFrame]

  /** Labels to frames */
  @transient private lazy val vertexFrameMetasMap = frameEntities.filter(frame => frame.isVertexFrame)
    .map(frame => (frame.label.get, frame))
    .toMap[String, DataFrame]

  /** convenience method for getting the id of the graph */
  def id: Long = graphEntity.id

  /** Next unique id for edges and vertices */
  def nextId(): Long = {
    graphEntity.nextId()
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
    frameEntities.exists(frame => frame.label.get == label)
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

  def vertexCount: Option[Long] = {
    countList(vertexFrames.map(frame => frame.rowCount))
  }

  def edgeCount: Option[Long] = {
    countList(edgeFrames.map(frame => frame.rowCount))
  }

  private def countList(list: List[Option[Long]]): Option[Long] = list match {
    case Nil => Some(0)
    case x :: xs => for {
      left <- x
      right <- countList(xs)
    } yield left + right
  }

  /**
   * Create a list of ElementIDName objects corresponding to the IDColumn of all Vertices in this graph.
   */
  def getFrameSchemaList: List[Schema] = {
    this.frameEntities.map {
      frame => frame.schema
    }.toList
  }

}
