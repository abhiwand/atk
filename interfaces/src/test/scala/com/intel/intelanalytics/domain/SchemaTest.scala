package com.intel.intelanalytics.domain

import org.scalatest.{ WordSpec, Matchers, FlatSpec }
import com.intel.intelanalytics.domain.schema.DataTypes.{ float32, int64, DataType, string }
import com.intel.intelanalytics.domain.schema.Schema

class SchemaTest extends WordSpec with Matchers {

  val columns: List[(String, DataType)] = List(("a", int64), ("b", float32), ("c", string))
  val abcSchema = new Schema(columns)

  "Schema" should {
    "find correct column index for single column" in {
      abcSchema.columnIndex("a") shouldBe 0
      abcSchema.columnIndex("b") shouldBe 1
      abcSchema.columnIndex("c") shouldBe 2
    }

    "find correct column index for two column" in {
      abcSchema.columnIndices(Seq("a", "b")) shouldBe List(0, 1)
      abcSchema.columnIndices(Seq("a", "c")) shouldBe List(0, 2)
    }

    "find correct column index for all columns when input columns is empty" in {
      abcSchema.columnIndices(Seq()) shouldBe List(0, 1, 2)
    }

    "be able to report column data types for first column" in {
      abcSchema.columnDataType("a") shouldBe int64
    }

    "be able to report column data types for last column" in {
      abcSchema.columnDataType("c") shouldBe string
    }

    "be able to add columns" in {
      val added = abcSchema.addColumn("str", string)
      added.columns.length shouldBe 4
      added.column("str").dataType shouldBe string
      added.column("str").index shouldBe 3
    }

    "be able to validate a column has a given type" in {
      abcSchema.hasColumnWithType("a", int64) shouldBe true
      abcSchema.hasColumnWithType("a", string) shouldBe false

    }

    "not have a label for plain old schemas" in {
      abcSchema.label.isEmpty shouldBe true
    }

    "be able to drop columns" in {
      println(abcSchema.dropColumn("b"))
      abcSchema.dropColumn("b").hasColumn("b") shouldBe false
    }

    "be able to copy a subset of columns" in {
      abcSchema.copySubset(Seq("a", "c")).columns.length shouldBe 2
    }

    "be able to rename columns" in {
      val renamed = abcSchema.renameColumn("b", "foo")
      renamed.hasColumn("b") shouldBe false
      renamed.hasColumn("foo") shouldBe true
      renamed.columnNames shouldBe List("a", "foo", "c")
    }

    "be able to reorder columns" in {
      val reordered = abcSchema.reorderColumns(List("b", "c", "a"))
      reordered.columnIndex("a") shouldBe 2
      reordered.columnIndex("b") shouldBe 0
      reordered.columnIndex("c") shouldBe 1
    }

    "be able to reorder columns with partial list" in {
      val reordered = abcSchema.reorderColumns(List("c", "a"))
      reordered.columnIndex("a") shouldBe 1
      reordered.columnIndex("b") shouldBe 2
      reordered.columnIndex("c") shouldBe 0
    }

    "return a subset of the columns as a List[Column]" in {
      val excluded = abcSchema.columnsExcept(List("a", "b"))
      excluded.length should be(1)
      excluded(0).name should be("c")
    }

  }

}
