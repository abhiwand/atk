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

package com.intel.intelanalytics.shared

import com.intel.intelanalytics.domain.frame.FrameReference
import com.intel.intelanalytics.schema.{ JsonSchema, ArraySchema, StringSchema, NumberSchema, ObjectSchema }
import org.scalatest.{ Matchers, FlatSpec }

case class SchemaSample(other_frame: FrameReference, int: Int, long: Long,
                        string: String, frame: FrameReference, array: Array[String], option: Option[Int])

class JsonSchemaSpec extends FlatSpec with Matchers {
  val schema = JsonSchemaExtractor.getProductSchema(manifest[SchemaSample])
  val reference = ObjectSchema(properties = Some(Map("int" -> JsonSchema.int.asInstanceOf[JsonSchema],
    "long" -> JsonSchema.long,
    "string" -> new StringSchema(),
    "array" -> ArraySchema(),
    "option" -> JsonSchema.int,
    "frame" -> StringSchema(format = Some("ia/dataframe"), self = Some(true)),
    "other_frame" -> StringSchema(format = Some("ia/graph"))
  )),
    required = Some(Array("int", "long", "string", "array", "frame", "other_frame")))

  "JsonSchemaExtractor" should "find all the case class vals" in {
    schema.properties.get.keys should equal(reference.properties.get.keys)
  }

  it should "support ints" in {
    schema.properties.get("int") should equal(reference.properties.get("int"))
  }
  it should "support Option[Int]s as ints" in {
    schema.properties.get("option") should equal(reference.properties.get("int"))
  }
  it should "make Options not required" in {
    schema.properties.get("option") should equal(reference.properties.get("option"))
    schema.required.get should not contain ("option")
  }
  it should "make non-Option types required" in {
    schema.properties.get("string") should equal(reference.properties.get("string"))
    schema.required.get should contain("string")
  }

  it should "detect frames in first position as self members" in {
    schema.properties.get.get("other_frame").get.asInstanceOf[StringSchema].self should equal(Some(true))
  }

  it should "treat frames in positions other than first as non-self members" in {
    schema.properties.get.get("frame").get.asInstanceOf[StringSchema].self should equal(None)
  }

  it should "capture the order of the fields" in {
    schema.order.get should equal(Array("other_frame", "int", "long",
      "string", "frame", "array", "option"))
  }
}
