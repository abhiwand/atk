package com.intel.intelanalytics.domain

import org.scalatest.{ WordSpec, Matchers, FlatSpec }
import com.intel.intelanalytics.domain.schema.DataTypes._
import com.intel.intelanalytics.domain.schema.{ FrameSchema, Column, Schema }

class SchemaTest extends WordSpec with Matchers {

  val columns = List(Column("a", int64), Column("b", float32), Column("c", string))
  val abcSchema = new FrameSchema(columns)

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

    def testDropColumn(columnName: String): Unit = {
      val result = abcSchema.dropColumn(columnName)
      assert(result.columns.length == 2, "length was not 2: " + result)
      assert(!result.hasColumn(columnName), "column was still present: " + result)
    }

    "be able to drop a column a" in {
      testDropColumn("a")
    }

    "be able to drop a column b" in {
      testDropColumn("b")
    }

    "be able to drop a column c" in {
      testDropColumn("c")
    }

    "be able to drop multiple columns 1" in {
      val result = abcSchema.dropColumns(List("a", "c"))
      assert(result.columns.length == 1)
      assert(result.hasColumn("b"))
    }

    "be able to drop multiple columns 2" in {
      val result = abcSchema.dropColumns(List("a", "b"))
      assert(result.columns.length == 1)
      assert(result.hasColumn("c"))
    }

    "be able to drop multiple columns 3" in {
      val result = abcSchema.dropColumns(List("b", "c"))
      assert(result.columns.length == 1)
      assert(result.hasColumn("a"))
    }

    "be able to drop multiple columns with list of 1" in {
      val result = abcSchema.dropColumns(List("a"))
      assert(result.columns.length == 2)
      assert(result.hasColumn("b"))
      assert(result.hasColumn("c"))
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

    "be able to drop 'ignore' columns" in {
      val schema = new FrameSchema(List(Column("a", int64), Column("b", ignore), Column("c", string))).dropIgnoreColumns()
      assert(schema.columnTuples == List(("a", int64), ("c", string)))
    }

    "be able to select a subset and rename in one step" in {
      val schema = abcSchema.copySubsetWithRename(Map(("a", "a_renamed"), ("c", "c_renamed")))
      assert(schema.columns.length == 2)
      assert(schema.column("a_renamed").index == 0)
      assert(schema.column(1).name == "c_renamed")
    }

    "be able to select a subset and rename to same names" in {
      val schema = abcSchema.copySubsetWithRename(Map(("a", "a"), ("c", "c")))
      assert(schema.columns.length == 2)
      assert(schema.column("a").index == 0)
      assert(schema.column(1).name == "c")
    }
  }

}
