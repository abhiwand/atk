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

package com.intel.graphbuilder.driver.spark.rdd

import com.intel.graphbuilder.testutils.TestingSparkContext
import com.intel.graphbuilder.parser.{CombinedParser, InputSchema, ColumnDef}
import com.intel.graphbuilder.parser.rule.{EdgeRuleParser, VertexRuleParser, EdgeRule, VertexRule}
import com.intel.graphbuilder.parser.rule.RuleParserDSL._
import org.apache.spark.rdd.RDD
import org.specs2.mutable.Specification
import GraphBuilderRDDImplicits._
import org.specs2.mock.Mockito

class ParserRDDFunctionsITest extends Specification with Mockito {

  "ParserRDDFunctions" should {

    "support a combined parser (one that goes over the input in one step)" in new TestingSparkContext {

      // Input Data
      val inputRows = List(
        List("1", "{(1)}", "1", "Y", "1", "Y"),
        List("2", "{(1)}", "10", "Y", "2", "Y"),
        List("3", "{(1)}", "11", "Y", "3", "Y"),
        List("4", "{(1),(2)}", "100", "N", "4", "Y"),
        List("5", "{(1)}", "101", "Y", "5", "Y")
      )

      // Input Schema
      val inputSchema = new InputSchema(List(
        new ColumnDef("cf:number", classOf[String]),
        new ColumnDef("cf:factor", classOf[String]),
        new ColumnDef("binary", classOf[String]),
        new ColumnDef("isPrime", classOf[String]),
        new ColumnDef("reverse", classOf[String]),
        new ColumnDef("isPalindrome", classOf[String])
      ))

      // Parser Configuration
      val vertexRules = List(VertexRule(gbId("cf:number"), List(property("isPrime"))), VertexRule(gbId("reverse")))
      val edgeRules = List(EdgeRule(gbId("cf:number"), gbId("reverse"), constant("reverseOf")))

      val vertexParser = new VertexRuleParser(inputSchema, vertexRules)
      val edgeParser = new EdgeRuleParser(inputSchema, edgeRules)
      val parser = new CombinedParser(inputSchema, vertexParser, edgeParser)

      // Setup data in Spark
      val inputRdd = sc.parallelize(inputRows.asInstanceOf[Seq[_]]).asInstanceOf[RDD[Seq[_]]]

      // invoke method under test
      val outputRdd = inputRdd.parse(parser)

      // verification
      outputRdd.count() mustEqual 15
      outputRdd.filterVertices().count() mustEqual 10
      outputRdd.filterEdges().count() mustEqual 5
    }
  }

}
