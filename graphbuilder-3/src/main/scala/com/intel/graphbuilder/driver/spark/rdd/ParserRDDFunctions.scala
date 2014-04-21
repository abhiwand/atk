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

import com.intel.graphbuilder.elements._
import com.intel.graphbuilder.parser.Parser
import org.apache.spark.rdd.RDD

/**
 * Functions for RDD's that can act as input to GraphBuilder.
 * <p>
 * This is best used by importing GraphBuilderRDDImplicits._
 * </p>
 * @param self input that these functions are applicable to
 */

class ParserRDDFunctions(self: RDD[Seq[_]]) {

  /**
   * Parse the raw rows of input into Vertices
   *
   * @param vertexParser the parser to use
   */
  def parseVertices(vertexParser: Parser[Vertex]): RDD[Vertex] = {
    self.flatMap(row => vertexParser.parse(row))
  }

  /**
   * Parse the raw rows of input into Edges
   *
   * @param edgeParser the parser to use
   */
  def parseEdges(edgeParser: Parser[Edge]): RDD[Edge] = {
    self.flatMap(row => edgeParser.parse(row))
  }

  /**
   * Parse the raw rows of input into GraphElements.
   * <p>
   * This method is useful if you want to make a single pass over the input
   * to parse both Edges and Vertices..
   * </p>
   * @param parser the parser to use
   */
  def parse(parser: Parser[GraphElement]): RDD[GraphElement] = {
    self.flatMap(row => parser.parse(row))
  }

}
