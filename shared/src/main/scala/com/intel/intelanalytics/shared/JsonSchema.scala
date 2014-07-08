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
import com.intel.intelanalytics.domain.graph.GraphReference
import com.intel.intelanalytics.schema._
import org.joda.time.DateTime
import spray.json.{ AdditionalFormats, StandardFormats, ProductFormats }
import scala.reflect.api.JavaUniverse
import scala.reflect.runtime.{ universe => ru }
import ru._

import scala.reflect.ClassTag

private[intelanalytics] class ProductFormatsAccessor extends ProductFormats
    with StandardFormats
    with AdditionalFormats {
  override def extractFieldNames(classManifest: ClassManifest[_]): Array[String] =
    super.extractFieldNames(classManifest)
}

object JsonSchemaExtractor {

  val fieldHelper = new ProductFormatsAccessor()

  def getProductSchema[T](tag: ClassTag[T]): ObjectSchema = {
    val manifest: ClassManifest[T] = tag
    val names = fieldHelper.extractFieldNames(manifest)
    val mirror = ru.runtimeMirror(tag.runtimeClass.getClassLoader)
    val typ: ru.Type = mirror.classSymbol(tag.runtimeClass).toType
    val members = typ.members.filter(m => !m.isMethod)
    val func = getFieldSchema(typ)(_)
    val propertyInfo = members.map(n => n.name.decoded -> func(n))
    val required = propertyInfo.filter { case (name, (_, optional)) => !optional }.map { case (n, _) => n }.toArray
    val properties = propertyInfo.map { case (name, (schema, _)) => name -> schema }.toMap
    ObjectSchema(properties = Some(properties), required = Some(required))
  }

  def getFieldSchema(clazz: ru.Type)(symbol: ru.Symbol): (JsonSchema, Boolean) = {
    val typeSignature: ru.Type = symbol.typeSignatureIn(clazz)
    val name = symbol.name.decoded.toLowerCase
    val schema = getSchemaForType(name, typeSignature)
    schema
  }

  def getSchemaForType(name: String, typeSignature: ru.Type): (JsonSchema, Boolean) = {
    val schema = typeSignature match {
      case t if t =:= typeTag[URI].tpe => StringSchema(format = Some("uri"))
      case t if t =:= typeTag[String].tpe => StringSchema()
      case t if t =:= typeTag[Int].tpe => NumberSchema(maximum = Some(Int.MaxValue),
        minimum = Some(Int.MinValue))
      case t if t =:= typeTag[Long].tpe => NumberSchema(maximum = Some(Long.MaxValue),
        minimum = Some(Long.MinValue))
      case t if t =:= typeTag[DateTime].tpe => StringSchema(format = Some("date-time"))
      case t if t =:= typeTag[FrameReference].tpe =>
        val s = StringSchema(format = Some("uri/ia-frame"))
        if (name == "frame" || name.toLowerCase == "dataframe") {
          s.copy(self = Some(true))
        }
        else s
      case t if t =:= typeTag[GraphReference].tpe =>
        val s = StringSchema(format = Some("uri/ia-graph"))
        if (name == "graph") {
          s.copy(self = Some(true))
        }
        else s
      case t if t.erasure =:= typeTag[Option[Any]].tpe =>
        val (subSchema, _) = getSchemaForType(name, t.asInstanceOf[TypeRefApi].args.head)
        subSchema
      case t if t.erasure =:= typeTag[Map[Any, Any]].tpe => ObjectSchema()
      case t if t.erasure =:= typeTag[Seq[Any]].tpe => ArraySchema()
      case t if t.erasure =:= typeTag[Iterable[Any]].tpe => ArraySchema()
      case t if t.erasure =:= typeTag[List[Any]].tpe => ArraySchema()
      case t if t.erasure =:= typeTag[Array[Any]].tpe => ArraySchema()
      case t => JsonSchema.empty
    }
    (schema, typeSignature.erasure =:= typeTag[Option[Any]].tpe)
  }
}
