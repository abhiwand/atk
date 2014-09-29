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
 * A Frame RDD is an RDD of type Row with an associated schema
 *
 * @param schema the schema describing the columns of this frame
 */
class FrameRDD(val schema: Schema, val rows: RDD[Row]) extends RDD[Row](rows) {

  def this(schema: Schema, schemaRDD: SchemaRDD) = this(schema, schemaRDD.map(row => row.toArray))

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

  /**
   * Converts the rows object from an RDD[Array[Any]] to a Schema RDD
   * @return A SchemaRDD with a schema corresponding to the schema object
   */
  def toSchemaRDD(): SchemaRDD = {
    val sqlContext = new SQLContext(rows.sparkContext)
    val rdd = this.toRowRDD()

    val structType = this.schemaToStructType()
    val attributes = structType.fields.map(f => AttributeReference(f.name, f.dataType, f.nullable)())

    val logicalPlan = SparkLogicalPlan(ExistingRdd(attributes, rdd))

    new SchemaRDD(sqlContext, logicalPlan)
  }

  /**
   * Converts row object from an RDD[Array[Any]] to an RDD[Product] so that it can be used to create a SchemaRDD
   * @return RDD[org.apache.spark.sql.Row] with values equal to row object
   */
  def toRowRDD(): RDD[org.apache.spark.sql.Row] = {
    val rowRDD: RDD[org.apache.spark.sql.Row] = rows.map(row => {
      val mutableRow = new GenericMutableRow(row.length)
      row.zipWithIndex.map {
        case (o, i) => {
          o match {
            case null => null
            case _ => {
              val colType = schema.columns(i)._2
              val value = o.asInstanceOf[colType.ScalaType]
              mutableRow(i) = value
            }

          }
        }
      }
      mutableRow
    })
    rowRDD
  }

  /**
   * Converts the schema object to a StructType for use in creating a SchemaRDD
   * @return StructType with StructFields corresponding to the columns of the schema object
   */
  def schemaToStructType(): StructType = {
    val columns = if (schema != null) {
      schema.columns
    }
    else {
      0.to(rows.first().length).map { i =>
        (s"col ${i.toString}", DataTypes.string)
      }
    }
    val fields: Seq[StructField] = schema.columns.map {
      case (name, dataType) =>
        StructField(name.replaceAll("\\s", ""), dataType match {
          case x if x.equals(DataTypes.int32) => IntegerType
          case x if x.equals(DataTypes.int64) => LongType
          case x if x.equals(DataTypes.float32) => FloatType
          case x if x.equals(DataTypes.float64) => DoubleType
          case x if x.equals(DataTypes.string) => StringType
        }, nullable = true)
    }
    StructType(fields)
  }
}

