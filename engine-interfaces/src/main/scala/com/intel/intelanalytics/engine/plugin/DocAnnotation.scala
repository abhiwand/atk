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
package com.intel.intelanalytics.engine

import scala.annotation.meta.field

// References:
// http://stackoverflow.com/questions/13944819/how-do-you-get-java-method-annotations-to-work-in-scala
// http://stackoverflow.com/questions/17792383/how-to-list-all-fields-with-a-custom-annotation-using-scalas-reflection-at-runt
// http://stackoverflow.com/questions/20710269/getting-scala-2-10-annotation-values-at-runtime
// http://www.veebsbraindump.com/2013/01/reflecting-annotations-in-scala-2-10/

package object plugin {
  type ArgDoc = ArgDocAnnotation @field
  type PluginDoc = PluginDocAnnotation
}

/**
 * Annotation to provide documentation for individual fields of plugin Arguments and Return
 * @param description text describing the arg (do not provide the name or type as they are provided with reflection)
 */
case class ArgDocAnnotation(description: String = "") extends scala.annotation.StaticAnnotation

/**
 * Annotation to provide documentation for a plugin
 * @param oneLine concise one-liner, ends with a period
 * @param extended rich description, can be several paragraphs
 * @param returns optional description for the return object.  Uses empty string for null value, instead of null
 *                for the sake of serialization, and instead of Option for the sake of reflection
 */
case class PluginDocAnnotation(oneLine: String, extended: String, returns: String = "") extends scala.annotation.StaticAnnotation {

  /**
   * Get description text of returns as Option, accounting for its empty semantics
   * @return
   */
  def getReturnsDescription: Option[String] = {
    if (returns.nonEmpty) { Some(returns) } else { None }
  }
}
