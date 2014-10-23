package com.intel.intelanalytics.engine.spark.frame

import com.intel.intelanalytics.domain.schema.{ DataTypes, Schema }
import com.intel.testutils.TestingSparkContextWordSpec
import org.apache.spark.sql.catalyst.types.{ StringType, IntegerType }
import org.scalatest.Matchers

class FrameRDDTest extends TestingSparkContextWordSpec with Matchers {
  "FrameRDD" should {

    "create an appropriate StructType from frames Schema" in {
      val schema = new Schema(List(("num", DataTypes.int32), ("name", DataTypes.string)))
      val structType = FrameRDD.schemaToStructType(schema.columns)
      structType.fields(0).name should be("num")
      structType.fields(0).dataType should be(IntegerType)

      structType.fields(1).name should be("name")
      structType.fields(1).dataType should be(StringType)
    }

    "allow a Row RDD in the construtor" in {
      val rows = sparkContext.parallelize((1 to 100).map(i => Array(i, i.toString)))
      val schema = new Schema(List(("num", DataTypes.int32), ("name", DataTypes.string)))
      val rdd = new FrameRDD(schema, rows)
      rdd.schema should be(schema)
      rdd.first should equal(rows.first)
    }

    "be convertible into a LegacyFrameRDD" in {
      val rows = sparkContext.parallelize((1 to 100).map(i => Array(i, i.toString)))
      val schema = new Schema(List(("num", DataTypes.int32), ("name", DataTypes.string)))
      val rdd = new FrameRDD(schema, rows)
      val legacy = rdd.toLegacyFrameRDD
      legacy.getClass should be(classOf[LegacyFrameRDD])
      legacy.schema should equal(schema)
      legacy.first should equal(rdd.first)
    }
  }
}
