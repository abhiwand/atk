//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

package com.intel.graphbuilder.parser.rule

import com.intel.graphbuilder.parser.{ InputRow, ColumnDef, InputSchema }
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
