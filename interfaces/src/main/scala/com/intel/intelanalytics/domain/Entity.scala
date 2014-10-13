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

package com.intel.intelanalytics.domain

/**
 * Describes the structure of a name, including both singular and plural,
 * for use in constructing URIs and other identifiers for entities in the system.
 * @param name the singular name for the entity
 * @param plural the plural name for the entity
 */
case class EntityName(name: String, plural: String)

/**
 * Entities in the system, things which can be named in URIs and other identifiers.
 *
 * Examples include [[com.intel.intelanalytics.domain.graph.Graph]] and
 * [[com.intel.intelanalytics.domain.frame.DataFrame]].
 */
trait Entity {
  /**
   * The standard name for this entity
   */
  def name: EntityName

  /**
   * Other names that are also recognized / accepted in incoming requests,
   * but not generally used for constructing outgoing responses.
   */
  def alternatives: Seq[EntityName] = Seq()
}

