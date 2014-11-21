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

package org.apache.spark.ia.graph

import com.intel.graphbuilder.elements.{ GBEdge, Property => GBProperty, GBVertex }
import com.intel.intelanalytics.domain.schema.{ EdgeSchema, DataTypes, Schema }
import com.intel.intelanalytics.engine.spark.frame.{ AbstractRow, RowWrapper }
import org.apache.spark.sql.Row

/**
 * Edge: self contained edge with complete schema information included.
 * Edge is used when you want RDD's of mixed edge types.
 */
case class Edge(override val schema: EdgeSchema, override var row: Row) extends AbstractEdge with Serializable

/**
 * EdgeWrapper: container that can be re-used to minimize memory usage but still provide a rich API
 * EdgeWrapper is used when you have RDD's of all one edge type (e.g. frame-like operations)
 * With a wrapper, the user sets the row data before each operation.
 * You never want to create RDD[EdgeWrapper] because it wouldn't have data you wanted.
 */
class EdgeWrapper(override val schema: EdgeSchema) extends AbstractEdge with Serializable {

  @transient override var row: Row = null

  /**
   * Set the data in this wrapper
   * @param row the data to set inside this Wrapper
   * @return this instance
   */
  def apply(row: Row): EdgeWrapper = {
    this.row = row
    this
  }

  def toEdge: Edge = {
    new Edge(schema, row)
  }

}

/**
 * AbstractEdge allows two implementations with slightly different trade-offs
 *
 * 1) Edge: self contained edge with complete schema information included.
 *    Edge is used when you want RDD's of mixed edge types.
 *
 * 2) EdgeWrapper: container that can be re-used to minimize memory usage but still provide a rich API
 *    EdgeWrapper is used when you have RDD's of all one edge type (e.g. frame-like operations)
 *    With a wrapper, the user sets the row data before each operation.
 *    You never want to create RDD[EdgeWrapper] because it wouldn't have data you wanted.
 *
 * This is the "common interface" for edges within our system.
 */
trait AbstractEdge extends AbstractRow with Serializable {
  require(schema.isInstanceOf[EdgeSchema], "schema should be for edges")
  require(schema.hasColumnWithType("_eid", DataTypes.int64), "schema did not have int64 _eid column: " + schema.columnTuples)
  require(schema.hasColumnWithType("_src_vid", DataTypes.int64), "schema did not have int64 _src_vid column: " + schema.columnTuples)
  require(schema.hasColumnWithType("_dest_vid", DataTypes.int64), "schema did not have int64 _dest_vid column: " + schema.columnTuples)
  require(schema.hasColumnWithType("_label", DataTypes.str), "schema did not have string _label column: " + schema.columnTuples)

  /**
   * Return id of the edge
   * @return edge id
   */
  def eid(): Long = longValue("_eid")

  /**
   * Return id of the source vertex
   * @return source vertex id
   */
  def srcVertexId(): Long = longValue("_src_vid")

  /**
   * Return id of the destination vertex
   * @return destination vertex id
   */
  def destVertexId(): Long = longValue("_dest_vid")

  /**
   * Return label of the edge
   * @return label of the edge
   */
  def label(): String = stringValue("_label")

  /**
   * Set the label on this vertex
   */
  def setLabel(label: String): Row = {
    setValue("_label", label)
  }

  def setSrcVertexId(vid: Long): Row = {
    setValue("_src_vid", vid)
  }

  def setDestVertexId(vid: Long): Row = {
    setValue("_dest_vid", vid)
  }

  /**
   * Convert this row to a GBEdge
   */
  def toGbEdge(): GBEdge = createGBEdge(false)

  /**
   * Convert this row to a GBEdge that has the source and destination vertices reversed
   */
  def toReversedGbEdge(): GBEdge = createGBEdge(true)

  /**
   * create a GBEdge object from this row
   * @param reversed: if true this will reverse the source and destination vids. This is used with a bidirect graph.
   *
   */
  private def createGBEdge(reversed: Boolean): GBEdge = {
    val filteredColumns = schema.columnsExcept(List("_label", "_src_vid", "_dest_vid"))
    val properties = filteredColumns.map(column => GBProperty(column.name, value(column.name)))
    // TODO: eid() will be included as a property, is that good enough?
    val srcProperty: GBProperty = GBProperty("_vid", srcVertexId())
    val destProperty: GBProperty = GBProperty("_vid", destVertexId())
    if (reversed)
      GBEdge(None, null, null, destProperty, srcProperty, schema.asInstanceOf[EdgeSchema].label, properties.toSet)
    else
      GBEdge(None, null, null, srcProperty, destProperty, schema.asInstanceOf[EdgeSchema].label, properties.toSet)
  }
}
