package com.intel.intelanalytics.engine.spark.frame

import org.apache.spark.rdd.{ UnionRDD, RDD }
import com.intel.intelanalytics.engine.Rows.Row
import com.intel.intelanalytics.domain.schema.{ SchemaUtil, Schema }
import org.apache.spark.{ Partition, TaskContext }

/**
 * A Frame RDD is an RDD of type Row with an associated schema
 *
 * @param schema the schema describing the columns of this frame
 */
class FrameRDD(val schema: Schema, val rows: RDD[Row]) extends RDD[Row](rows) {

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = rows.compute(split, context)

  override def getPartitions: Array[Partition] = rows.partitions

  /**
   * Union two FrameRDD's merging schemas if needed
   *
   * @param other the other FrameRDD
   */
  def union(other: FrameRDD): FrameRDD = {
    if (schema == other.schema)
      new FrameRDD(schema, rows.union(other.rows))
    else {
      val mergedSchema: Schema = SchemaUtil.mergeSchema(schema, other.schema)
      val leftData = rows.map(SchemaUtil.convertSchema(schema, mergedSchema, _))
      val rightData = other.rows.map(SchemaUtil.convertSchema(other.schema, mergedSchema, _))
      new FrameRDD(mergedSchema, leftData.union(rightData))
    }
  }
}
