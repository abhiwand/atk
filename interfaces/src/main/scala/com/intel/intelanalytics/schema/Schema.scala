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

import java.net.URI

trait Schema {
  def id: Option[URI]
  def title: Option[String]
  def $schema: Option[String]
  def description: Option[String]
}

sealed trait Primitive extends Schema {
  def id = None
  def $schema = None
}

case class ObjectSchema(
                   id: Option[URI] = None,
                   title: Option[String] = None,
                   $schema: String = "http://json-schema.org/draft-04/schema#",
                   description: Option[String] = None,
                   fields: Map[String,Schema]
                   ) extends Schema

case class StringSchema(
            title: Option[String],
            description: Option[String],
            maxLength: Option[Int] = None,
            minLength: Option[Int] = None,
            pattern: Option[String]
                         ) extends Primitive {
  def `type` = "string"
  require(maxLength.isEmpty || maxLength > 0, "maxLength must be greater than zero")
  require(minLength.isEmpty || maxLength >= 0, "minLength must be greater than or equal to zero")
  require(minLength.getOrElse(0) <= maxLength.getOrElse(Int.MaxValue), "maximum must be at least equal to minimum")
}

case class ArraySchema(title: Option[String],
                       description: Option[String],
                       additionalItems: Option[Either[Boolean,ObjectSchema]],
                       items: Option[Either[ObjectSchema,Array[ObjectSchema]]]) extends Primitive {

}

case class IntSchema(title: Option[String],
                     description: Option[String]) extends Primitive {

}