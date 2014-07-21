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
import spray.json.{ AdditionalFormats, StandardFormats }
import com.intel.intelanalytics.spray.json.{ JsonPropertyNameConverter, CustomProductFormats }
import org.joda.time.DateTime
import scala.reflect.api.JavaUniverse
import scala.reflect.runtime.{ universe => ru }
import ru._
import scala.reflect.ClassTag

/**
 * Helper to allow access to spray-json utility so that we can ensure we're
 * accessing case class vals in exactly the same way that it will
 */
private[intelanalytics] class ProductFormatsAccessor extends CustomProductFormats
    with StandardFormats
    with AdditionalFormats {
  override def extractFieldNames(classManifest: ClassManifest[_]): Array[String] =
    super.extractFieldNames(classManifest)
}

/**
 * Generates `JsonSchema` objects to represent case classes
 */
private[intelanalytics] object JsonSchemaExtractor {

  val fieldHelper = new ProductFormatsAccessor()

  /**
   * Entry point for generating schema information for a case class
   * @param tag extended type information for the given type
   * @tparam T the type for which to generate a JSON schema
   */
  def getProductSchema[T](tag: ClassTag[T]): ObjectSchema = {
    // double check that Spray serialization will work
    val manifest: ClassManifest[T] = tag
    fieldHelper.extractFieldNames(manifest)

    val mirror = ru.runtimeMirror(tag.runtimeClass.getClassLoader)
    val typ: ru.Type = mirror.classSymbol(tag.runtimeClass).toType
    val members: Array[ru.Symbol] = typ.members.filter(m => !m.isMethod).toArray.reverse
    val func = getFieldSchema(typ)(_, _)
    val ordered = Array.tabulate(members.length) { i => (members(i), i) }
    val propertyInfo = ordered.map({
      case (sym, i) => JsonPropertyNameConverter.camelCaseToUnderscores(sym.name.decoded) -> func(sym, i)
    })
    val required = propertyInfo.filter { case (name, (_, optional)) => !optional }.map { case (n, _) => n }.toArray
    val properties = propertyInfo.map { case (name, (schema, _)) => name -> schema }.toMap
    ObjectSchema(properties = Some(properties),
      required = Some(required),
      order = Some(members.map(sym => JsonPropertyNameConverter.camelCaseToUnderscores(sym.name.decoded)).toArray))
  }

  /**
   * Get the schema for one particular field
   *
   * @param clazz the parent class
   * @param symbol the field
   * @param order the numeric (0 based) order of this field in the class
   * @return a pair containing the schema, plus a flag indicating if the field is optional or not
   */
  private def getFieldSchema(clazz: ru.Type)(symbol: ru.Symbol, order: Int): (JsonSchema, Boolean) = {
    val typeSignature: ru.Type = symbol.typeSignatureIn(clazz)
    val schema = getSchemaForType(typeSignature, order)
    schema
  }

  /**
   * Returns the schema for a particular type.
   *
   * FrameReference and GraphReference types that appear at position zero are marked
   * as "self" arguments.
   *
   * @param typeSignature the type
   * @param order the numeric order of the field within its containing class
   * @return
   */
  def getSchemaForType(typeSignature: ru.Type, order: Int): (JsonSchema, Boolean) = {
    val schema = typeSignature match {
      case t if t =:= typeTag[URI].tpe => StringSchema(format = Some("uri"))
      case t if t =:= typeTag[String].tpe => StringSchema()
      case t if t =:= typeTag[Int].tpe => JsonSchema.int
      case t if t =:= typeTag[Long].tpe => JsonSchema.long
      case t if t =:= typeTag[DateTime].tpe => JsonSchema.dateTime
      case t if t =:= typeTag[FrameReference].tpe =>
        val s = JsonSchema.frame
        if (order == 0) {
          s.copy(self = Some(true))
        }
        else s
      case t if t =:= typeTag[GraphReference].tpe =>
        val s = JsonSchema.graph
        if (order == 0) {
          s.copy(self = Some(true))
        }
        else s
      case t if t.erasure =:= typeTag[Option[Any]].tpe =>
        val (subSchema, _) = getSchemaForType(t.asInstanceOf[TypeRefApi].args.head, order)
        subSchema
      //parameterized types need special handling
      case t if t.erasure =:= typeTag[Map[Any, Any]].tpe => ObjectSchema()
      case t if t.erasure =:= typeTag[Seq[Any]].tpe => ArraySchema()
      case t if t.erasure =:= typeTag[Iterable[Any]].tpe => ArraySchema()
      case t if t.erasure =:= typeTag[List[Any]].tpe => ArraySchema()
      //array type system works a little differently
      case t if t.typeConstructor =:= typeTag[Array[Any]].tpe.typeConstructor => ArraySchema()
      case t => JsonSchema.empty
    }
    (schema, typeSignature.erasure =:= typeTag[Option[Any]].tpe)
  }
}
