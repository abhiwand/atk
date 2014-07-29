package com.intel.intelanalytics.engine.spark.statistics

import org.scalatest.FunSuite

import org.apache.spark.SparkException
import org.scalatest.Matchers
import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.engine.Rows._
import spray.json._
import DefaultJsonProtocol._
import com.intel.intelanalytics.domain.frame.{ ColumnMedianReturn, ColumnSummaryStatisticsReturn, ColumnFullStatisticsReturn, ColumnFullStatistics }
import com.intel.testutils.TestingSparkContextFlatSpec
import com.intel.testutils.TestingSparkContextFlatSpec
import org.apache.spark.SparkException
import org.scalatest.Matchers
/**
 * Exercises the column statistics functions. Primarily checks that correct column indices and options are piped
 * through to the underlying statistics engines. Thorough evaluation of the statistical operations is done by the
 * tests for the respective statistics engines.
 */
class ColumnStatisticsITest extends TestingSparkContextFlatSpec with Matchers {

  trait ColumnStatisticsTest {

    val epsilon = 0.000000001

    // Input data
    val row0: Row = Array[Any]("A", 1, 2.0f, 2, 3, 1.0f, 0, 0)
    val row1: Row = Array[Any]("B", 1, 2.0f, 1, 3, 2.0f, 0, 0)
    val row2: Row = Array[Any]("C", 1, 2.0f, 3, 2, 0.0f, 10, 0)
    val row3: Row = Array[Any]("D", 1, 2.0f, 6, 1, 1.0f, 0, 0)
    val row4: Row = Array[Any]("E", 1, 2.0f, 7, 1, 2.0f, 0, 0)

    val rowRDD: RDD[Row] = sparkContext.parallelize(List(row0, row1, row2, row3, row4))
  }

  "mode with no net weight" should "return none as json" in new ColumnStatisticsTest() {
    val testMode = ColumnStatistics.columnMode(0, DataTypes.string, Some(7), Some(DataTypes.int32), rowRDD)

    testMode.mode shouldBe None.asInstanceOf[Option[String]].toJson
  }

  "weighted mode" should "work" in new ColumnStatisticsTest() {

    val testMode = ColumnStatistics.columnMode(0, DataTypes.string, Some(3), Some(DataTypes.int32), rowRDD)

    testMode.mode shouldBe "E".toJson
  }

  "unweighted full statistics" should "work" in new ColumnStatisticsTest() {

    val stats: ColumnFullStatisticsReturn =
      ColumnStatistics.columnFullStatistics(2, DataTypes.float32, None, None, rowRDD)

    Math.abs(stats.mean - 2.0) should be < epsilon
  }

  "weighted full statistics" should "work" in new ColumnStatisticsTest() {

    val stats: ColumnFullStatisticsReturn =
      ColumnStatistics.columnFullStatistics(5, DataTypes.float32, Some(4), Some(DataTypes.int32), rowRDD)

    Math.abs(stats.mean - 1.2) should be < epsilon
  }

  "unweighted summary statistics" should "work" in new ColumnStatisticsTest() {

    val stats: ColumnSummaryStatisticsReturn = ColumnStatistics.columnSummaryStatistics(2, DataTypes.float32, None, None, rowRDD)

    Math.abs(stats.mean - 2.0) should be < epsilon
  }

  "weighted summary statistics" should "work" in new ColumnStatisticsTest() {

    val stats: ColumnSummaryStatisticsReturn =
      ColumnStatistics.columnSummaryStatistics(5, DataTypes.float32, Some(4), Some(DataTypes.int32), rowRDD)

    Math.abs(stats.mean - 1.2) should be < epsilon
  }

  "unweighted median" should "work" in new ColumnStatisticsTest() {

    val median: ColumnMedianReturn = ColumnStatistics.columnMedian(2, DataTypes.float32, None, None, rowRDD)

    median.value shouldBe 2.0f.toJson
  }

  "weighted median" should "work" in new ColumnStatisticsTest() {

    val median: ColumnMedianReturn =
      ColumnStatistics.columnMedian(5, DataTypes.float32, Some(6), Some(DataTypes.int32), rowRDD)

    median.value shouldBe 0.0f.toJson
  }

  "median with no net weights" should "return none as json" in new ColumnStatisticsTest() {
    val median = ColumnStatistics.columnMedian(0, DataTypes.string, Some(7), Some(DataTypes.int32), rowRDD)

    median.value shouldBe None.asInstanceOf[Option[Double]].toJson
  }
}
