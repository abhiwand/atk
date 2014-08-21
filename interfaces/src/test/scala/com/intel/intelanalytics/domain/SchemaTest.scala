package com.intel.intelanalytics.domain
import org.scalatest.{ Matchers, FlatSpec }
import com.intel.intelanalytics.domain.schema.DataTypes.{ float32, int64, DataType, string }
import com.intel.intelanalytics.domain.schema.Schema

class SchemaTest extends FlatSpec with Matchers {
  "columnIndex" should "find correct column index for single column" in {
    val columns: List[(String, DataType)] = List(("a", int64), ("b", float32), ("c", string))
    val schema = Schema(columns)
    schema.columnIndex("a") shouldBe 0
    schema.columnIndex("b") shouldBe 1
    schema.columnIndex("c") shouldBe 2
  }

  "columnIndex" should "find correct column index for two column" in {
    val columns: List[(String, DataType)] = List(("a", int64), ("b", float32), ("c", string))
    val schema = Schema(columns)
    schema.columnIndex(Seq("a", "b")) shouldBe List(0, 1)
    schema.columnIndex(Seq("a", "c")) shouldBe List(0, 2)
  }

  "columnIndex" should "find correct column index for all columns when input columns is empty" in {
    val columns: List[(String, DataType)] = List(("a", int64), ("b", float32), ("c", string))
    val schema = Schema(columns)
    schema.columnIndex(Seq()) shouldBe List(0, 1, 2)
  }

}
