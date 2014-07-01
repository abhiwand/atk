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

package com.intel.intelanalytics.schema

import java.beans.{ Introspector, BeanInfo }
import java.net.URI

import spray.json.{ JsValue, JsonFormat }

trait JsonSchema {
  def id: Option[URI]
  def title: Option[String]
  //def $schema: Option[String]
  def description: Option[String]
  def `type`: Option[String]
}

object JsonSchema {
  val empty: JsonSchema = new JsonSchema {
    def id = None
    def title = None
    //def $schema = None
    def description = None
    def `type` = None
  }
}

sealed trait Primitive extends JsonSchema {
  //def $schema = None
}

case class ObjectSchema(
    id: Option[URI] = None,
    title: Option[String] = None,
    //$schema: Option[String] = Some("http://intel.com/iat/schema/json-schema-04"),
    description: Option[String] = None,
    maxProperties: Option[Int] = None,
    minProperties: Option[Int] = None,
    required: Option[Array[String]] = None,
    additionalProperties: Option[Boolean] = None,
    properties: Option[Map[String, JsonSchema]] = None,
    patternProperties: Option[Map[String, JsonSchema]] = None,
    definitions: Option[Map[String, JsonSchema]] = None,
    `type`: Option[String] = Some("object")) extends JsonSchema {
}

case class StringSchema(
    id: Option[URI] = None,
    title: Option[String] = None,
    description: Option[String] = None,
    maxLength: Option[Int] = None,
    minLength: Option[Int] = None,
    pattern: Option[String] = None,
    format: Option[String] = None,
    self: Option[Boolean] = None,
    `type`: Option[String] = Some("string")) extends Primitive {
  require(maxLength.isEmpty || maxLength.get > 0, "maxLength must be greater than zero")
  require(minLength.isEmpty || maxLength.get >= 0, "minLength must be greater than or equal to zero")
  require(minLength.getOrElse(0) <= maxLength.getOrElse(Int.MaxValue), "maximum must be at least equal to minimum")
}

case class ArraySchema(id: Option[URI] = None,
                       title: Option[String] = None,
                       description: Option[String] = None,
                       additionalItems: Option[Either[Boolean, ObjectSchema]] = None,
                       items: Option[Either[ObjectSchema, Array[ObjectSchema]]] = None,
                       maxItems: Option[Int] = None,
                       minItems: Option[Int] = None,
                       uniqueItems: Option[Boolean] = None,
                       `type`: Option[String] = Some("array")) extends Primitive {
  require(maxItems.isEmpty || maxItems.get >= 0, "maxItems may not be less than zero")
  require(minItems.isEmpty || minItems.get >= 0, "minItems may not be less than zero")
}

case class NumberSchema(id: Option[URI] = None,
                        title: Option[String] = None,
                        description: Option[String] = None,
                        minimum: Option[Double] = None,
                        exclusiveMinimum: Option[Double] = None,
                        maximum: Option[Double] = None,
                        exclusiveMaximum: Option[Double] = None,
                        multipleOf: Option[Double] = None,
                        `type`: Option[String] = Some("number")) extends Primitive {
}

