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

package com.intel.intelanalytics.engine.spark.command

import shapeless._
import spray.json._

import scala.reflect.runtime.{ universe => ru }
import ru._

object Typeful {

  //Inspired by deepsearch examples from the shapeless project.

  /**
   * Things that can be searched
   * @tparam T the type of things that can be searched in
   * @tparam Element the type of things that can be found
   */
  trait Searchable[T, Element] {
    def findAll(predicate: Element => Boolean)(t: T): Seq[Element]
  }

  trait LowPrioritySearchable {
    implicit def hlistishSearchable[T: TypeTag, L <: HList: TypeTag, Element: TypeTag](implicit gen: Generic.Aux[T, L],
                                                                                       s: Searchable[L, Element]) = new Searchable[T, Element] {
      //println(s"hlistish: T = $typeTag[T], L = $typeTag[L], Element = $typeTag[Element]")
      def findAll(predicate: Element => Boolean)(t: T) = {
        s.findAll(predicate)(gen to t)
      }
    }
  }

  object Searchable extends LowPrioritySearchable {
    implicit def elemSearchable[Element] = new Searchable[Element, Element] {
      def findAll(predicate: Element => Boolean)(element: Element) = {
        if (predicate(element)) Seq(element) else Seq.empty
      }
    }

    implicit def listSearchable[T, Element](implicit s: Searchable[T, Element]) =
      new Searchable[List[T], Element] {
        def findAll(predicate: Element => Boolean)(list: List[T]) = {
          list.flatMap(s.findAll(predicate))
        }
      }

    implicit def mapValueSearchable[K, T, Element](implicit s: Searchable[T, Element]) =
      new Searchable[Map[K, T], Element] {
        def findAll(predicate: Element => Boolean)(map: Map[K, T]) = {
          map.values.toSeq.flatMap(s.findAll(predicate))
        }
      }

    implicit def hnilSearchable[Element] = new Searchable[HNil, Element] {
      def findAll(predicate: Element => Boolean)(a: HNil) = {
        Seq.empty
      }
    }

    implicit def hlistSearchable[H, T <: HList, Element](
      implicit hs: Searchable[H, Element] = null,
      ts: Searchable[T, Element]) =
      new Searchable[H :: T, Element] {
        def findAll(predicate: Element => Boolean)(a: H :: T) = {
          val seq = hs match {
            case null =>
              Seq()
            case _ =>
              Seq(hs)
          }
          seq.flatMap(_.findAll(predicate)(a.head)) ++ ts.findAll(predicate)(a.tail)
        }
      }

    implicit def jsString =
      new Searchable[JsString, String] {
        override def findAll(predicate: (String) => Boolean)(t: JsString): Seq[String] = t match {
          case JsString(s) if predicate(s) => Seq(s)
          case _ => Seq.empty
        }
      }

    def jsNumber[Element](implicit convert: BigDecimal => Element) =
      new Searchable[JsNumber, Element] {
        override def findAll(predicate: (Element) => Boolean)(t: JsNumber): Seq[Element] = t match {
          case JsNumber(n) if predicate(convert(n)) => Seq(convert(n))
          case _ => Seq.empty
        }
      }

    implicit def jsValueBigDecimal = jsNumber(identity)

    implicit def jsValueDouble = jsNumber(_.toDouble)

    implicit def jsValueBigInt = jsNumber(_.toBigInt)

    implicit def jsValueInt = jsNumber(_.toInt)

    implicit def jsValueLong = jsNumber(_.toLong)

    implicit def jsValueShort = jsNumber(_.toShort)

    implicit def jsValueByte = jsNumber(_.toByte)

    implicit def jsValueBool = new Searchable[JsBoolean, Boolean] {
      override def findAll(predicate: (Boolean) => Boolean)(t: JsBoolean): Seq[Boolean] = t match {
        case JsBoolean(b) if (predicate(b)) => Seq(b)
        case _ => Seq()
      }
    }

    def findIfAvailable[T, E](searchable: Searchable[T, E], predicate: E => Boolean, value: T): Seq[E] =
      Option(searchable).map(_.findAll(predicate)(value)).getOrElse(List.empty)

    implicit def jsValueSearchable[Element](implicit b: Searchable[JsBoolean, Element] = null,
                                            jss: Searchable[JsString, Element] = null,
                                            jsn: Searchable[JsNumber, Element] = null) = new Searchable[JsValue, Element] {
      override def findAll(predicate: (Element) => Boolean)(t: JsValue): Seq[Element] = t match {
        case x: JsObject => jsObjectSearchable(this).findAll(predicate)(x)
        case x: JsArray => x.elements.flatMap(a => findAll(predicate)(a))
        case x: JsNumber => findIfAvailable(jsn, predicate, x)
        case x: JsString => findIfAvailable(jss, predicate, x)
        case x: JsBoolean => findIfAvailable(b, predicate, x)
        case JsNull => Seq()
        case _ => throw new IllegalArgumentException("Unknown JSON value type: " + t.getClass)
      }
    }

    implicit def jsObjectSearchable[Element](implicit js: Searchable[JsValue, Element]) =
      new Searchable[JsObject, Element] {
        override def findAll(predicate: (Element) => Boolean)(t: JsObject): Seq[Element] = {
          listSearchable[JsValue, Element](js).findAll(predicate)(t.fields.values.toList)
        }
      }

    case class SearchableWrapper[A](a: A) {
      def deepFind[Q](p: Q => Boolean)(implicit s: Searchable[A, Q]) =
        s.findAll(p)(a)
    }

    implicit def wrapSearchable[A](a: A) = SearchableWrapper(a)

  }

}
