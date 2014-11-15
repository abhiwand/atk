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

package com.intel.intelanalytics.engine.spark.frame

import org.apache.spark.rdd.{ UnionRDD, RDD }
import com.intel.intelanalytics.engine.Rows.Row
import com.intel.intelanalytics.domain.schema.{ DataTypes, SchemaUtil, Schema }
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.{ SQLContext, SchemaRDD }
import org.apache.spark.sql.catalyst.expressions.{ GenericRow, AttributeReference, GenericMutableRow }
import org.apache.spark.sql.execution.{ SparkLogicalPlan, ExistingRdd }
import org.apache.spark.{ SparkContext, Partition, TaskContext }

/**
 * A LegacyFrame RDD is an RDD of type Row with an associated schema
 * This was our primary representation of our RDDs and could be phased out.
 *
 * Please don't write new code against this legacy format:
 * - This format requires extra maps to read/write Parquet files.
 * - We'd rather use FrameRDD which extends SchemaRDD and can go direct to/from Parquet.
 *
 * @param schema the schema describing the columns of this frame
 */
class LegacyFrameRDD(val schema: Schema, val rows: RDD[Row]) extends RDD[Row](rows) {

  def this(schema: Schema, schemaRDD: SchemaRDD) = this(schema, schemaRDD.map(row => row.toArray))

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = rows.compute(split, context)

  override def getPartitions: Array[Partition] = rows.partitions

  /**
   * Union two LegacyFrame's merging schemas if needed
   *
   * @param other the other LegacyFrame
   */
  def union(other: LegacyFrameRDD): LegacyFrameRDD = {
    if (schema == other.schema)
      new LegacyFrameRDD(schema, rows.union(other.rows))
    else {
      val mergedSchema: Schema = SchemaUtil.mergeSchema(schema, other.schema)
      val leftData = rows.map(SchemaUtil.convertSchema(schema, mergedSchema, _))
      val rightData = other.rows.map(SchemaUtil.convertSchema(other.schema, mergedSchema, _))
      new LegacyFrameRDD(mergedSchema, leftData.union(rightData))
    }
  }

  /**
   * Converts the rows object from an RDD[Array[Any]] to a Frame RDD
   * @return A FrameRDD made of this schema and the rows RDD converted to a SchemaRDD
   */
  def toFrameRDD(): FrameRDD = {
    FrameRDD.toFrameRDD(this.schema, this.rows)
  }
}

