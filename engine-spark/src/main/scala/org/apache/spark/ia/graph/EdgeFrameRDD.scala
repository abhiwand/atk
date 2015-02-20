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

package org.apache.spark.ia.graph

import com.intel.graphbuilder.elements.{ GBEdge, GBVertex }
import com.intel.intelanalytics.domain.schema.{ EdgeSchema, GraphSchema, Schema }
import org.apache.spark.frame.FrameRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import scala.reflect.ClassTag

/**
 * Edge list for a "Seamless" Graph
 *
 * @param schema  the schema describing the columns of this frame
 * @param sqlContext a spark SQLContext
 * @param logicalPlan a logical plan describing the SchemaRDD
 */
class EdgeFrameRDD(schema: EdgeSchema,
                   sqlContext: SQLContext,
                   logicalPlan: LogicalPlan) extends FrameRDD(schema, sqlContext, logicalPlan) {

  def this(frameRDD: FrameRDD) = this(frameRDD.frameSchema.asInstanceOf[EdgeSchema], frameRDD.sqlContext, frameRDD.logicalPlan)

  def this(schema: Schema, rowRDD: RDD[sql.Row]) = this(schema.asInstanceOf[EdgeSchema], new SQLContext(rowRDD.context), FrameRDD.createLogicalPlanFromSql(schema, rowRDD))

  /** Edge wrapper provides richer API for working with Vertices */
  val edge = new EdgeWrapper(schema)

  /**
   * Map over edges
   * @param mapFunction map function that operates on a EdgeWrapper
   * @tparam U return type that will be the in resulting RDD
   */
  def mapEdges[U: ClassTag](mapFunction: (EdgeWrapper) => U): RDD[U] = {
    this.map(data => {
      mapFunction(edge(data))
    })
  }

  /**
   * Convert this RDD in match the schema provided
   * @param updatedSchema the new schema to take effect
   * @return the new RDD
   */
  override def convertToNewSchema(updatedSchema: Schema): EdgeFrameRDD = {
    if (schema == updatedSchema) {
      // no changes needed
      this
    }
    else {
      // map to new schema
      new EdgeFrameRDD(super.convertToNewSchema(updatedSchema))
    }
  }

  /**
   * Map over all edges and assign the label from the schema
   */
  def assignLabelToRows(): EdgeFrameRDD = {
    new EdgeFrameRDD(schema, mapEdges(edge => edge.setLabel(schema.label)))
  }

  /**
   * Append edges to the current frame:
   * - union the schemas to match, if needed
   * - no overwrite
   */
  def append(other: FrameRDD): EdgeFrameRDD = {
    val unionedSchema = schema.union(other.frameSchema).reorderColumns(GraphSchema.edgeSystemColumnNames)

    // TODO: better way to check for empty?
    if (take(1).length > 0) {
      val part1 = convertToNewSchema(unionedSchema)
      val part2 = new EdgeFrameRDD(other.convertToNewSchema(unionedSchema))
      new EdgeFrameRDD(part1.union(part2)).assignLabelToRows()
    }
    else {
      new EdgeFrameRDD(other.convertToNewSchema(unionedSchema)).assignLabelToRows()
    }
  }

  def toEdgeRDD: RDD[Edge] = {
    this.mapEdges(_.toEdge)
  }

  /**
   * Convert this EdgeFrameRDD to a GB Edge RDD
   */
  def toGbEdgeRDD: RDD[GBEdge] = {
    if (schema.directed)
      this.mapEdges(_.toGbEdge)
    else
      this.mapEdges(_.toGbEdge) union this.mapEdges(_.toReversedGbEdge)
  }

}
