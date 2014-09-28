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

package com.intel.intelanalytics.engine

import com.intel.intelanalytics.domain.UriReference

import scala.reflect.runtime.{ universe => ru }
import ru._
import scala.reflect.{ ClassTag, classTag }

/**
 * Utilities for working with Scala reflection.
 *
 * Note that in Scala 2.10, reflection is not thread safe, so all methods
 * of this object are synchronized.
 */
object Reflection {

  /**
   * Returns all the members of the type that are vals, and not something
   * esoteric created by the compiler. Just the regular vals.
   */
  def getVals[T: TypeTag](): Seq[(String, ru.Type)] = synchronized {
    val tag = classTag[T]
    val mirror = ru.runtimeMirror(tag.runtimeClass.getClassLoader)
    val typ: ru.Type = mirror.classSymbol(tag.runtimeClass).toType
    //IntelliJ will report an error on this line, it's safe to ignore - the real compiler does fine with it.
    val members: Array[ru.Symbol] = typ.members.filter(m => !m.isMethod
      && m.asTerm.isVal
      && !m.isImplementationArtifact
      && !m.isSynthetic)
      .toArray
      .reverse
    val namedTypes = members.map(sym => (sym.name.decoded, sym.typeSignatureIn(typ)))
    namedTypes
  }

  /**
   * Returns val members whose type is a subtype (<:<) of the given member type.
   * @tparam ObjectType the type to inspect
   * @tparam MemberType the member type to look for
   */
  def getMembersWithTypeLike[ObjectType: TypeTag, MemberType: TypeTag]() = synchronized {
    val namedTypes = getVals[ObjectType]()
    val references = namedTypes.filter { case (name, sig) => sig <:< typeTag[MemberType].tpe }
    references

  }

  /**
   * Returns information about the vals in the type that are subclasses of UriReference.
   */
  def getUriReferenceTypes[T: TypeTag](): Seq[(String, ru.Type)] = getMembersWithTypeLike[T, UriReference]()

  /**
   * Returns a function that takes any number of parameters in the same order as they appear
   * in the class's constructor, and returns a new instance.
   * @tparam T the type to construct
   */
  def getConstructor[T: TypeTag](): Seq[Any] => T = {
    val tag = typeTag[T]
    val m = ru.runtimeMirror(tag.mirror.runtimeClass(tag.tpe).getClassLoader)
    val classT = ru.typeOf[T].typeSymbol.asClass
    val cm = m.reflectClass(classT)
    val ctor = tag.tpe.declaration(ru.nme.CONSTRUCTOR).asMethod
    val ctorMirror = cm.reflectConstructor(ctor)
    args => ctorMirror(args: _*).asInstanceOf[T]
  }

  /**
   * Converts a TypeTag[T] into a ClassTag[T]
   */
  implicit def typeTagToClassTag[T: TypeTag]: ClassTag[T] = {
    val tag = typeTag[T]
    ClassTag[T](tag.mirror.runtimeClass(tag.tpe))
  }

  /**
   * Returns a function that a map of parameters and applies the parameters to the
   * constructor by matching map keys to parameter names.
   *
   * @tparam T the type to construct
   */
  def getConstructorMap[T: TypeTag](): Map[String, Any] => T = {
    val tag = typeTag[T]
    val m = ru.runtimeMirror(classTag[T].runtimeClass.getClassLoader)
    val classT = ru.typeOf[T].typeSymbol.asClass
    val cm = m.reflectClass(classT)
    val ctor = tag.tpe.declaration(ru.nme.CONSTRUCTOR).asMethod
    val ctorMirror = cm.reflectConstructor(ctor)

    val parms = ctor.paramss match {
      case List(ps) => ps
      case _ => throw new IllegalArgumentException("Only constructors with a single set of arguments are supported")
    }

    (args: Map[String,Any]) => {
      val applied = parms.map(p => {
        val decoded: String = p.name.decodedName.decoded
        args.getOrElse(decoded, throw new IllegalArgumentException(s"Missing value for parameter '$decoded'"))
      })

      ctorMirror(applied: _*).asInstanceOf[T]
    }
  }

}
