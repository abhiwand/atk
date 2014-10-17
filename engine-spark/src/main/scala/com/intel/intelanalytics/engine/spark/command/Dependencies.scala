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

object Dependencies {

  import Typeful.Searchable._

  type Computable = UriReference with OnDemand

  type DependencyGraph = Graph[Computable, DiEdge]

  def getUriReferencesFromJsObject(source: JsObject): Seq[UriReference] = {

    val results = source.deepFind((s: String) => ReferenceResolver.isReferenceUriFormat(s))
    results.flatMap(js => ReferenceResolver.resolve(js).toOption.toList)

  }

  def getUriReferences[A <: Product](arguments: A)(implicit ev: Searchable[A, UriReference]) = {
    val refs = arguments.deepFind((_: UriReference) => true)
    refs
  }

  def getDirectDependencies(source: Computable): Seq[UriReference] = {
    val command = source.command()
    val results = command.arguments.toList.flatMap(getUriReferencesFromJsObject)
    results
  }

  def build(startWith: Seq[Computable], resolver: ReferenceResolver = ReferenceResolver): DependencyGraph = {
    val unfulfilled = startWith.filter(obj => obj.computeStatus != Complete)
    val commands = unfulfilled.map(obj => obj.command()).toSet
    val args = commands.map(c => c.arguments.get)
                        .flatMap(getUriReferencesFromJsObject)
                        .toSet
                        .map((ref: UriReference) => ref.entity.asInstanceOf[EntityManagement])
//    if (unfulfilled.isEmpty)
//      Graph.from()

    ???
  }

  def force(ref: Seq[Computable], graph: DependencyGraph): ReferenceResolver = {
    ???
  }
}
