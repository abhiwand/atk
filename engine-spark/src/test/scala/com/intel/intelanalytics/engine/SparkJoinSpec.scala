package com.intel.intelanalytics.engine.spark

import org.scalatest._
import org.apache.spark.SparkContext
import java.util.Date
import com.intel.intelanalytics.domain.DataTypes.{ string, int32, DataType }

class SparkJoinSpec extends FlatSpec with Matchers {

  "create2TupleForJoin" should "put first column in first entry" in {
    val data = Array("1", "2", 3, 4, "5")
    val result = SparkOps.create2TupleForJoin(data, 0)
    result._1 shouldBe "1"
    result._2 shouldBe Array("1", "2", 3, 4, "5")
  }

  "create2TupleForJoin" should "put third column in first entry" in {
    val data = Array("1", "2", 3, 4, "5")
    val result = SparkOps.create2TupleForJoin(data, 2)
    result._1 shouldBe 3
    result._2 shouldBe Array("1", "2", 3, 4, "5")
  }

  "join schema" should "keep field name the same when there is not name conflicts" in {
    val leftCols: List[(String, DataType)] = List(("a", int32), ("b", string))
    val rightCols: List[(String, DataType)] = List(("c", int32), ("d", string))
    val result = SparkEngine.resolveSchemaNamingConflicts(leftCols, rightCols)
    List(("a", int32), ("b", string), ("c", int32), ("d", string)) shouldBe result
  }

  "join schema" should "append l to the field from left and r to the field from right if field names conflict" in {
    val leftCols: List[(String, DataType)] = List(("a", int32), ("b", string), ("c", string))
    val rightCols: List[(String, DataType)] = List(("a", int32), ("c", string), ("d", string))
    val result = SparkEngine.resolveSchemaNamingConflicts(leftCols, rightCols)
    List(("a_l", int32), ("b", string), ("c_l", string), ("a_r", int32), ("c_r", string), ("d", string)) shouldBe result
  }
}
