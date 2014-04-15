package com.intel.graphbuilder.parser.rule

import com.intel.graphbuilder.parser.{InputRow, ColumnDef, InputSchema}
import java.util.Date
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification

class DataTypeResolverSpec extends Specification with Mockito {

  val columnDef1 = new ColumnDef("columnName1", classOf[Date], 0)
  val columnDef2 = new ColumnDef("columnName2", null, 0)
  val inputSchema = new InputSchema(List(columnDef1, columnDef2))
  val dataTypeResolver = new DataTypeResolver(inputSchema)

  "DataTypeParser" should {

    "handle constant values" in {
      dataTypeResolver.get(new ConstantValue("someString")) mustEqual classOf[String]
      dataTypeResolver.get(new ConstantValue(new Date())) mustEqual classOf[Date]
    }

    "handle parsed values" in {
      dataTypeResolver.get(new ParsedValue("columnName1")) mustEqual classOf[Date]
    }

    "throw exception for parsed values without dataType" in {
      dataTypeResolver.get(new ParsedValue("columnName2")) must throwA[RuntimeException]
    }

    "handle compound values as Strings" in {
      val compoundValue = new CompoundValue(new ConstantValue(new Date()), new ConstantValue(1000))
      dataTypeResolver.get(compoundValue) mustEqual classOf[String]
    }

    "throw exception for other types not yet implemented" in {
      val unsupportedValue = new Value {
        override def value(row: InputRow): Any = null
        override def value: Any = null
        override def in(row: InputRow): Boolean = false
        override def isParsed: Boolean = false
      }

      dataTypeResolver.get(unsupportedValue) must throwA[RuntimeException]
    }
  }
}
