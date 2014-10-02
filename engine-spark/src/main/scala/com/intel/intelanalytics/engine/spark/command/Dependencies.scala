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

package com.intel.intelanalytics.engine.spark.command

import com.intel.intelanalytics.domain.{ ReferenceResolver, OnDemand, UriReference }
import shapeless._
import spray.json.JsObject

import scalax.collection.Graph
import scalax.collection.GraphEdge.DiEdge

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
    implicit def hlistishSearchable[T, L <: HList, Element](implicit gen: Generic.Aux[T, L],
                                                            s: Searchable[L, Element]) = new Searchable[T, Element] {
      def findAll(predicate: Element => Boolean)(t: T) = s.findAll(predicate)(gen to t)
    }
  }

  object Searchable extends LowPrioritySearchable {
    implicit def elemSearchable[Element] = new Searchable[Element, Element] {
      def findAll(predicate: Element => Boolean)(element: Element) = if (predicate(element)) Seq(element) else Seq.empty
    }

    implicit def listSearchable[T, Element](implicit s: Searchable[T, Element]) =
      new Searchable[List[T], Element] {
        def findAll(predicate: Element => Boolean)(list: List[T]) = list.flatMap(s.findAll(predicate))
      }

    implicit def hnilSearchable[Element] = new Searchable[HNil, Element] {
      def findAll(predicate: Element => Boolean)(a: HNil) = Seq.empty
    }

    implicit def hlistSearchable[H, T <: HList, Element](
      implicit hs: Searchable[H, Element] = null, ts: Searchable[T, Element]) = new Searchable[H :: T, Element] {
      def findAll(predicate: Element => Boolean)(a: H :: T) =
        Seq(hs).flatMap(_.findAll(predicate)(a.head)) ++ ts.findAll(predicate)(a.tail)
    }

    case class SearchableWrapper[A](a: A) {
      def deepFind[Q](p: Q => Boolean)(implicit s: Searchable[A, Q]) =
        s.findAll(p)(a)
    }

    implicit def wrapSearchable[A](a: A) = SearchableWrapper(a)

    //    // An example predicate:
    //    val p = (_: String) endsWith "o"
    //
    //    // On strings:
    //    assert("hello".deepFind(p) == Some("hello"))
    //    assert("hell".deepFind(p) == None)
    //
    //    // On lists:
    //    assert(List("yes", "maybe", "no").deepFind(p) == Some("no"))
    //
    //    // On arbitrarily sized and nested tuples:
    //    assert(("yes", "maybe", ("no", "why")).deepFind(p) == Some("no"))
    //    assert(("a", ("b", "c"), "d").deepFind(p) == None)
    //
    //    // On tuples with non-string elements:
    //    assert((1, "two", ('three, '4')).deepFind(p) == Some("two"))
    //
    //    // Search the same tuple for a specific character instead:
    //    assert((1, "two", ('three, '4')).deepFind((_: Char) == 52) == Some('4'))
    //
    //    // Our case class:
    //    case class Foo(a: String, b: String, c: List[String])
    //
    //    // And it works:
    //    assert(Foo("four", "three", List("two", "one")).deepFind(p) == Some("two"))
    //    assert(Foo("a", "b", "c" :: Nil).deepFind(p) == None)

  }

  //  /**
  //   * Searching arbitrarily nested case classes, tuples, and lists.
  //   *
  //   * @author Travis Brown
  //   */
  //  object DeepSearchExamples extends App {
  //
  //    // Evidence that an A is something that we can look around in for Qs that
  //    // satisfy some predicate.
  //    trait Searchable[A, Q] {
  //      def find(p: Q => Boolean)(a: A): Option[Q]
  //    }
  //
  //    trait LowPrioritySearchable {
  //      implicit def hlistishSearchable[A, L <: HList, Q](
  //                                                         implicit gen: Generic.Aux[A, L], s: Searchable[L, Q]
  //                                                         ) = new Searchable[A, Q] {
  //        def find(p: Q => Boolean)(a: A) = s.find(p)(gen to a)
  //      }
  //    }
  //
  //    object Searchable extends LowPrioritySearchable {
  //      implicit def elemSearchable[A] = new Searchable[A, A] {
  //        def find(p: A => Boolean)(a: A) = if (p(a)) Some(a) else None
  //      }
  //
  //      implicit def listSearchable[A, Q](implicit s: Searchable[A, Q]) =
  //        new Searchable[List[A], Q] {
  //          def find(p: Q => Boolean)(a: List[A]) = a.flatMap(s.find(p)).headOption
  //        }
  //
  //      implicit def hnilSearchable[Q] = new Searchable[HNil, Q] {
  //        def find(p: Q => Boolean)(a: HNil) = None
  //      }
  //
  //      implicit def hlistSearchable[H, T <: HList, Q](
  //                                                      implicit hs: Searchable[H, Q] = null, ts: Searchable[T, Q]
  //                                                      ) = new Searchable[H :: T, Q] {
  //        def find(p: Q => Boolean)(a: H :: T) =
  //          Option(hs).flatMap(_.find(p)(a.head)) orElse ts.find(p)(a.tail)
  //      }
  //    }
  //
  //    case class SearchableWrapper[A](a: A) {
  //      def deepFind[Q](p: Q => Boolean)(implicit s: Searchable[A, Q]) =
  //        s.find(p)(a)
  //    }
  //
  //    implicit def wrapSearchable[A](a: A) = SearchableWrapper(a)
  //
  //    // An example predicate:
  //    val p = (_: String) endsWith "o"
  //
  //    // On strings:
  //    assert("hello".deepFind(p) == Some("hello"))
  //    assert("hell".deepFind(p) == None)
  //
  //    // On lists:
  //    assert(List("yes", "maybe", "no").deepFind(p) == Some("no"))
  //
  //    // On arbitrarily sized and nested tuples:
  //    assert(("yes", "maybe", ("no", "why")).deepFind(p) == Some("no"))
  //    assert(("a", ("b", "c"), "d").deepFind(p) == None)
  //
  //    // On tuples with non-string elements:
  //    assert((1, "two", ('three, '4')).deepFind(p) == Some("two"))
  //
  //    // Search the same tuple for a specific character instead:
  //    assert((1, "two", ('three, '4')).deepFind((_: Char) == 52) == Some('4'))
  //
  //    // Our case class:
  //    case class Foo(a: String, b: String, c: List[String])
  //
  //    // And it works:
  //    assert(Foo("four", "three", List("two", "one")).deepFind(p) == Some("two"))
  //    assert(Foo("a", "b", "c" :: Nil).deepFind(p) == None)
  //  }

}

object Dependencies {

  import shapeless._

  type Computable = UriReference with OnDemand

  type DependencyGraph = Graph[Computable, DiEdge]

  def getDirectDependencies(source: Computable): Seq[Computable] = {
    val command = source.command()
    //    val results = command.arguments.map(getDirectDependencies).toList
    ??? //
  }

  def build(startWith: Seq[Computable]): DependencyGraph = {
    ??? //var graph = Graph.from(startWith, )
  }

  def force(ref: Seq[Computable], graph: DependencyGraph): ReferenceResolver = {
    ???
  }
}
