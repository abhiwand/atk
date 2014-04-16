package com.intel.graphbuilder.parser

import org.specs2.mutable.Specification

class InputSchemaSpec extends Specification {

  "ColumnDef" should {

    "require a non-empty column name" in {
      new ColumnDef("", null, null) must throwA[IllegalArgumentException]
    }

    "require a non-null column name" in {
      new ColumnDef(null, null, null) must throwA[IllegalArgumentException]
    }

    "require an index greater than or equal to zero" in {
      new ColumnDef("any", null, -1) must throwA[IllegalArgumentException]
    }
  }

  "InputSchema" should {

    val columnDefs = List(new ColumnDef("col1", classOf[String], null), new ColumnDef("col2", classOf[Int], null))
    val inputSchema = new InputSchema(columnDefs)

    "keep track of the number of columns" in {
      inputSchema.size mustEqual 2
    }

    "report dataType correctly for String" in {
      inputSchema.columnType("col1") mustEqual classOf[String]
    }

    "report dataType correctly for Int" in {
      inputSchema.columnType("col2") mustEqual classOf[Int]
    }

    "calculate and report the index of 1st column correctly" in {
      inputSchema.columnIndex("col1") mustEqual 0
    }

    "calculate and report the index of 2nd column correctly" in {
      inputSchema.columnIndex("col2") mustEqual 1
    }

    "accept indexes provided as part of the column definition" in {
      val columnDefs = List(new ColumnDef("col1", classOf[String], 1), new ColumnDef("col2", classOf[Int], 0))
      val inputSchema = new InputSchema(columnDefs)
      inputSchema.columnIndex("col1") mustEqual 1
      inputSchema.columnIndex("col2") mustEqual 0
    }
  }
}
