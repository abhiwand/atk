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

import java.net.URI

import com.intel.intelanalytics.domain.frame.FrameReference
import com.intel.intelanalytics.schema.{ JsonSchema, NumberSchema, StringSchema, ObjectSchema }
import org.joda.time.DateTime
import spray.json.{ AdditionalFormats, StandardFormats, ProductFormats }

private[intelanalytics] class ProductFormatsAccessor extends ProductFormats
    with StandardFormats
    with AdditionalFormats {
  override def extractFieldNames(classManifest: ClassManifest[_]): Array[String] =
    super.extractFieldNames(classManifest)
}

object JsonSchemaExtractor {

  val schemaRoot = new ObjectSchema().$schema

  val fieldHelper = new ProductFormatsAccessor()

  def getProductSchema[T](manifest: ClassManifest[T]): ObjectSchema = {
    val names = fieldHelper.extractFieldNames(manifest)
    val get = getFieldSchema(manifest)(_)
    val properties = names.map(n => n -> get(n)).toMap
    ObjectSchema(properties = Some(properties))
  }

  def getFieldSchema(manifest: ClassManifest[_])(propertyName: String): JsonSchema = {
    val theClass = manifest.erasure
    val field = theClass.getField(propertyName)
    assert(field != null, s"Field $propertyName not found on ${theClass.getName}")
    val schema = field.getType match {
      case t if t == classOf[URI] => StringSchema(format = Some("uri"))
      case t if t == classOf[String] => StringSchema()
      case t if t == classOf[Int] => NumberSchema(maximum = Some(Int.MaxValue),
        minimum = Some(Int.MinValue))
      case t if t == classOf[Long] => NumberSchema(maximum = Some(Long.MaxValue),
        minimum = Some(Long.MinValue))
      case t if t == classOf[DateTime] => StringSchema(format = Some("date-time"))
      case t if t == classOf[FrameReference] =>
        val s = StringSchema(format = Some("uri/iat-frame"))
        if (propertyName.toLowerCase == "frame"
          || propertyName.toLowerCase == "dataframe") {
          s.copy(self = Some(true))
        }
        else s
      case t => JsonSchema.empty
    }
    schema
  }
}
