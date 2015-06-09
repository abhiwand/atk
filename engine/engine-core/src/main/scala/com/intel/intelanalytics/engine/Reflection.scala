/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package com.intel.intelanalytics.engine

import com.intel.intelanalytics.domain.UriReference

import scala.reflect.runtime.{ universe => ru }
import ru._
import scala.reflect.{ ClassTag, classTag }
import scala.util.control.NonFatal

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
  def getVals[T: TypeTag](): Seq[(String, ru.Type)] = {
    try {
      synchronized {
        val tag = classTag[T]
        val mirror = ru.runtimeMirror(tag.runtimeClass.getClassLoader)
        val typ: ru.Type = mirror.classSymbol(tag.runtimeClass).toType
        val members = typ.members.filter(m => !m.isMethod
          && m.asTerm.isVal
          && !m.isImplementationArtifact
          && !m.isSynthetic)
          .toArray
          .reverse
        val namedTypes = members.map(sym => (sym.name.decoded, sym.typeSignatureIn(typ)))
        namedTypes
      }
    }
    catch {
      case NonFatal(e) => throw new Exception(s"Could not get vals for type ${typeTag[T]} due to:", e)
    }
  }

  /**
   * Returns val members whose type is a subtype (<:<) of the given member type.
   * @tparam ObjectType the type to inspect
   * @tparam MemberType the member type to look for
   */
  def getMembersWithTypeLike[ObjectType: TypeTag, MemberType: TypeTag]() = synchronized {
    val namedTypes = getVals[ObjectType]()
    val references = namedTypes.filter { case (name, sig) => sig <:< typeTag[MemberType].tpe }
    references.map { case (name, sig) => (name.trim, sig) }
  }

  /**
   * Returns information about the vals in the type that are subclasses of UriReference.
   */
  def getUriReferenceTypes[T: TypeTag](instance: T): Seq[(String, ru.Type)] = getMembersWithTypeLike[T, UriReference]()

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
    (args: Seq[Any]) => (ctorMirror(args: _*).asInstanceOf[T])
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

    (args: Map[String, Any]) => {
      val applied = parms.map(p => {
        val decoded: String = p.name.decodedName.decoded
        args.getOrElse(decoded, throw new IllegalArgumentException(s"Missing value for parameter '$decoded'"))
      })

      ctorMirror(applied: _*).asInstanceOf[T]
    }
  }

}
