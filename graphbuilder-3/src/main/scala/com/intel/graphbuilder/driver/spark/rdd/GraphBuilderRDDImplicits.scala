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
import org.apache.spark.rdd.RDD

/**
 * These implicits can be imported to add GraphBuilder related functions to RDD's
 */
object GraphBuilderRDDImplicits {

  /**
   * Functions applicable to RDD's that can be input to GraphBuilder Parsers
   */
  implicit def inputRDDToParserRDDFunctions(rdd: RDD[Seq[_]]) = new ParserRDDFunctions(rdd)

  /**
   * Functions applicable to Vertex RDD's
   */
  implicit def vertexRDDToVertexRDDFunctions(rdd: RDD[Vertex]) = new VertexRDDFunctions(rdd)

  /**
   * Functions applicable to Edge RDD's
   */
  implicit def edgeRDDToEdgeRDDFunctions(rdd: RDD[Edge]) = new EdgeRDDFunctions(rdd)

  /**
   * Functions applicable to GraphElement RDD's
   */
  implicit def graphElementRDDToGraphElementRDDFunctions(rdd: RDD[GraphElement]) = new GraphElementRDDFunctions(rdd)

}
