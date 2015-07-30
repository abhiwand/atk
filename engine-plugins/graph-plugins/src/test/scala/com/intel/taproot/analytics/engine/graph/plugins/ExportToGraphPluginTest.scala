/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package com.intel.taproot.analytics.engine.graph.plugins

import org.scalatest.{ Matchers, FlatSpec }
import com.intel.taproot.analytics.graphbuilder.elements.{ GBVertex, Property }
import com.intel.taproot.analytics.domain.schema.Column
import com.intel.taproot.analytics.domain.schema.DataTypes._
import com.intel.taproot.analytics.graphbuilder.parser.{ ColumnDef, InputSchema }
import com.intel.taproot.analytics.graphbuilder.parser.rule.{ EdgeRule, VertexRule }
import com.intel.taproot.analytics.graphbuilder.parser.rule.RuleParserDSL._
import com.intel.taproot.analytics.graphbuilder.parser.InputSchema
import com.intel.taproot.analytics.graphbuilder.parser.ColumnDef
import com.intel.taproot.analytics.domain.schema.Column
import org.apache.spark.rdd.RDD
import com.intel.taproot.analytics.graphbuilder.driver.spark.titan.{ GraphBuilder, GraphBuilderConfig }
import com.thinkaurelius.titan.core.TitanGraph
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import com.intel.taproot.analytics.domain.schema.Column

class ExportToGraphPluginTest extends FlatSpec with Matchers with MockitoSugar {

  "getPropertiesValueByColumns" should "get property values by column sequence" in {
    val properties = Set(Property("col4", 2f), Property("col1", 1), Property("col2", "2"), Property("col3", true))
    val vertex = GBVertex(1, Property("gbId", "1"), properties)
    val result = vertex.getPropertiesValueByColumns(List("col1", "col2", "col3", "col4"), properties)
    result shouldBe Array(1, "2", true, 2f)
  }

}
