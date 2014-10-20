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

import com.intel.intelanalytics.domain.command.Command
import com.intel.intelanalytics.domain._
import com.intel.intelanalytics.engine.Reflection
import com.intel.intelanalytics.engine.plugin.CommandPlugin
import com.intel.intelanalytics.engine.spark.command.Typeful.Searchable
import spray.json.{ JsNumber, JsValue, JsString, JsObject }

import scala.reflect.runtime.{ universe => ru }
import ru._
import scalax.collection.Graph
import scalax.collection.GraphEdge.DiEdge

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
  def getUriReferencesFromJsObject(source: JsObject): Seq[UriReference] = {
    val results = source.deepFind((s: String) => ReferenceResolver.isReferenceUriFormat(s))
    results.flatMap(js => ReferenceResolver.resolve(js).toOption.toList)
  }

  /**
   * Returns all the [[Computable]] objects from a case class
   */
  def getComputables[A <: Product](arguments: A): Seq[Computable] = {
    //val refs = arguments.deepFind((_: UriReference) => true)
    val refs = Reflection.getUriReferenceTypes()
    refs.flatMap {
      case ref: OnDemand => List(ref.asInstanceOf[Computable])
      case _ => List.empty
    }
  }

  /**
   * Helper method for the {{build}} method
   */
  private def buildHelper(startWith: Seq[Computable], resolver: ReferenceResolver, accumulator: List[Set[Command]]): List[Set[Command]] = {
    val unfulfilled = startWith.filter(obj => obj.computeStatus != Complete)
    val commands = unfulfilled.map(obj => obj.command()).toSet
    val deps = commands.map(c => c.arguments.get)
      .flatMap(getUriReferencesFromJsObject)
      .toSet
      .map((ref: UriReference) => {
        val management: EntityManagement = ref.entity.asInstanceOf[EntityManagement]
        management.getMetaData(ref.asInstanceOf[management.type#Reference]).asInstanceOf[Computable]
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
  def build(startWith: Seq[Computable], resolver: ReferenceResolver = ReferenceResolver): Seq[Set[Command]] =
    buildHelper(startWith, resolver, List.empty)

}
