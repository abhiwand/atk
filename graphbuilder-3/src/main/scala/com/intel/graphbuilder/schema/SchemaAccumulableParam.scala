//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
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

package com.intel.graphbuilder.schema

import org.apache.spark.AccumulableParam
import com.intel.graphbuilder.elements.{ GraphElement, GBVertex, GBEdge }

/**
 * Implements AccumulableParam to allow inferring graph schema from data.
 */
class SchemaAccumulableParam extends AccumulableParam[InferSchemaFromData, GraphElement] {

  /**
   * Add a new edge or vertex to infer schema object accumulator
   *
   * @param schema the current infer schema object
   * @param element the graph element to add to the current infer schema object
   * @return
   */
  override def addAccumulator(schema: InferSchemaFromData, element: GraphElement): InferSchemaFromData = {
    element match {
      case v: GBVertex => schema.add(v)
      case e: GBEdge => schema.add(e)
    }
    schema
  }

  /**
   * Merges two accumulated graph schemas (as InferSchemaFromData) together
   *
   * @param schemaOne accumulated infer schema object one
   * @param schemaTwo accumulated infer schema object two
   * @return the merged infer schema object
   */
  override def addInPlace(schemaOne: InferSchemaFromData, schemaTwo: InferSchemaFromData): InferSchemaFromData = {
    schemaOne.edgeLabelDefsMap ++= schemaTwo.edgeLabelDefsMap
    schemaOne.propertyDefsMap ++= schemaTwo.propertyDefsMap
    schemaOne
  }

  override def zero(initialValue: InferSchemaFromData): InferSchemaFromData = {
    initialValue
  }

}
