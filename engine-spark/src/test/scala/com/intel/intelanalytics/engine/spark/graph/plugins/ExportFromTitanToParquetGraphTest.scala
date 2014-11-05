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

package com.intel.intelanalytics.engine.spark.graph.plugins

import org.scalatest.{ Matchers, FlatSpec }
import com.intel.graphbuilder.elements.Property
import com.intel.intelanalytics.domain.schema.Column
import com.intel.intelanalytics.domain.schema.DataTypes._
import com.intel.graphbuilder.parser.{ ColumnDef, InputSchema }
import com.intel.graphbuilder.parser.rule.{ EdgeRule, VertexRule }
import com.intel.graphbuilder.parser.rule.RuleParserDSL._
import com.intel.graphbuilder.parser.InputSchema
import com.intel.graphbuilder.parser.ColumnDef
import com.intel.intelanalytics.domain.schema.Column
import org.apache.spark.rdd.RDD
import com.intel.graphbuilder.driver.spark.titan.{ GraphBuilder, GraphBuilderConfig }
import com.thinkaurelius.titan.core.{ TitanKey, TitanGraph }
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import com.intel.intelanalytics.domain.schema.Column

class ExportFromTitanToParquetGraphTest extends FlatSpec with Matchers with MockitoSugar {
  "getPropertiesValueByColumns" should "get property values by column sequence" in {
    val properties = Set(Property("col4", 2f), Property("col1", 1), Property("col2", "2"), Property("col3", true))
    val result = ExportFromTitanToParquetGraph.getPropertiesValueByColumns(List("col1", "col2", "col3", "col4"), properties)
    result shouldBe Array(1, "2", true, 2f)
  }

  "getSchemaFromProperties" should "get schema by column name and property values" in {
    val graph = mock[TitanGraph]

    val key1 = mock[TitanKey]
    doReturn(classOf[java.lang.Integer]).when(key1).getDataType
    val key2 = mock[TitanKey]
    doReturn(classOf[java.lang.String]).when(key2).getDataType
    val key3 = mock[TitanKey]
    doReturn(classOf[java.lang.Float]).when(key3).getDataType

    when(graph.getType("col1")).thenReturn(key1)
    when(graph.getType("col2")).thenReturn(key2)
    when(graph.getType("col3")).thenReturn(key3)

    val columns = ExportFromTitanToParquetGraph.getSchemaFromProperties(List("col1", "col2", "col3"), graph)
    columns(0) shouldBe Column("col1", int32)
    columns(1) shouldBe Column("col2", string)
    columns(2) shouldBe Column("col3", float32)
  }

  "javaTypeToIATType" should "get type from java type object" in {
    ExportFromTitanToParquetGraph.javaTypeToIATType(new java.lang.Integer(3).getClass) shouldBe int32
    ExportFromTitanToParquetGraph.javaTypeToIATType(new java.lang.Long(3).getClass) shouldBe int64
    ExportFromTitanToParquetGraph.javaTypeToIATType(new java.lang.Float(3.0).getClass) shouldBe float32
    ExportFromTitanToParquetGraph.javaTypeToIATType(new java.lang.Double(3).getClass) shouldBe float64
  }
}
