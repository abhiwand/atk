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

package com.intel.intelanalytics.engine.spark.command

import com.intel.intelanalytics.domain.command.Command
import com.intel.intelanalytics.domain._
import com.intel.intelanalytics.engine.{ ReferenceResolver, Reflection }
import com.intel.intelanalytics.engine.plugin.{ Invocation, CommandPlugin }
import com.intel.intelanalytics.engine.spark.command.Typeful.Searchable
import spray.json.{ JsNumber, JsValue, JsString, JsObject }
import scala.reflect.runtime.{ universe => ru }
import ru._

/**
 * Tools for retrieving dependencies of system entities such as frames and graphs,
 * tracing those dependencies through to the commands that produced them, and
 * obtaining a list of commands to run to generate the needed data.
 */
object Dependencies {

  import Typeful.Searchable._

  /**
   * System entities that might need to be lazily computed
   */
  type Computable = UriReference with OnDemand

  /**
   * Returns all the [[UriReference]] elements from a JsObject, which really means
   * extracting all the string values that are recognized as valid entity URIs.
   */
  def getUriReferencesFromJsObject(source: JsObject)(implicit invocation: Invocation): Seq[UriReference] = {
    require(invocation != null, "invocation is required")
    val results = source.deepFind((s: String) => ReferenceResolver.isReferenceUriFormat(s))
    results.flatMap(js => ReferenceResolver.resolve(js).toOption.toList)
  }

  /**
   * Returns all the [[Computable]] objects from a case class
   */
  def getComputables[A <: Product: TypeTag](arguments: A): Seq[Computable] = {
    //val refs = arguments.deepFind((_: UriReference) => true)
    val refs = Reflection.getUriReferenceTypes[A]()
    refs.flatMap {
      case ref: OnDemand => List(ref.asInstanceOf[Computable])
      case _ => List.empty
    }
  }

  /**
   * Helper method for the {{build}} method
   */
  private def buildHelper(startWith: Seq[Computable], resolver: ReferenceResolver, accumulator: List[Set[Command]])(implicit invocation: Invocation): List[Set[Command]] = {
    val unfulfilled = startWith.filter(obj => obj.computeStatus != Complete)
    val commands = unfulfilled.map(obj => obj.command()).toSet
    val deps = commands.map(c => c.arguments.get)
      .flatMap(getUriReferencesFromJsObject)
      .map((ref: UriReference) => {
        resolver.resolve[ref.type with HasMetaData](ref).asInstanceOf[Computable]
      })
    buildHelper(deps.toSeq, resolver, commands :: accumulator)
  }

  /**
   * Builds a list of commands that need to be executed in order to populate the entities
   * in the startWith parameter.
   *
   * Each Set[Command] represents a set of commands that can be run in parallel (they have no
   * dependencies among themselves). All the commands in a given set should be run before
   * proceeding to the next set.
   *
   * @param startWith the set of resources that need to be loaded
   * @param resolver the ReferenceResolver to use to look up information about the objects in the system.
   */
  def build(startWith: Seq[Computable], resolver: ReferenceResolver = ReferenceResolver)(implicit invocation: Invocation): Seq[Set[Command]] =
    buildHelper(startWith, resolver, List.empty)

}
