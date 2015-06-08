/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package com.intel.intelanalytics.engine.spark.frame

import org.apache.spark.frame.FrameRdd
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
 * - We'd rather use FrameRdd which extends SchemaRDD and can go direct to/from Parquet.
 *
 * @param schema the schema describing the columns of this frame
 */
@deprecated("use FrameRdd instead")
class LegacyFrameRdd(val schema: Schema, val rows: RDD[Row]) extends RDD[Row](rows) {

  def this(schema: Schema, schemaRDD: SchemaRDD) = this(schema, schemaRDD.map(row => row.toArray))

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = rows.compute(split, context)

  override def getPartitions: Array[Partition] = rows.partitions

  /**
   * Union two LegacyFrame's merging schemas if needed
   *
   * @param other the other LegacyFrame
   */
  def union(other: LegacyFrameRdd): LegacyFrameRdd = {
    if (schema == other.schema)
      new LegacyFrameRdd(schema, rows.union(other.rows))
    else {
      val mergedSchema: Schema = SchemaUtil.mergeSchema(schema, other.schema)
      val leftData = rows.map(SchemaUtil.convertSchema(schema, mergedSchema, _))
      val rightData = other.rows.map(SchemaUtil.convertSchema(other.schema, mergedSchema, _))
      new LegacyFrameRdd(mergedSchema, leftData.union(rightData))
    }
  }

  /**
   * Converts the rows object from an RDD[Array[Any]] to a Frame RDD
   * @return A FrameRdd made of this schema and the rows RDD converted to a SchemaRDD
   */
  def toFrameRdd(): FrameRdd = {
    FrameRdd.toFrameRdd(this.schema, this.rows)
  }
}
