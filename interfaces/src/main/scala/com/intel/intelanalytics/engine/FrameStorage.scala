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

import com.intel.event.EventContext
import com.intel.intelanalytics.domain.frame.{ FrameEntity, DataFrameTemplate, _ }
import com.intel.intelanalytics.domain.schema.{ Schema, DataTypes }
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.engine.Rows._
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.domain.CreateEntityArgs

trait FrameStorage {

  type Context
  type Data

  def expectFrame(frameId: Long)(implicit invocation: Invocation): FrameEntity
  def expectFrame(frameRef: FrameReference)(implicit invocation: Invocation): FrameEntity

  @deprecated("please use expectFrame() instead")
  def lookup(id: Long)(implicit invocation: Invocation): Option[FrameEntity]
  def lookupByName(name: Option[String])(implicit invocation: Invocation): Option[FrameEntity]
  def getFrames()(implicit invocation: Invocation): Seq[FrameEntity]
  def create(arguments: CreateEntityArgs)(implicit invocation: Invocation): FrameEntity
  def renameFrame(frame: FrameEntity, newName: String)(implicit invocation: Invocation): FrameEntity
  def renameColumns(frame: FrameEntity, name_pairs: Seq[(String, String)])(implicit invocation: Invocation): FrameEntity
  def getRows(frame: FrameEntity, offset: Long, count: Long)(implicit invocation: Invocation): Iterable[Row]
  def drop(frame: FrameEntity)(implicit invocation: Invocation)
  def loadFrameData(context: Context, frame: FrameEntity)(implicit invocation: Invocation): Data
  def saveFrameData(frame: FrameReference, data: Data)(implicit invocation: Invocation): FrameEntity

  def prepareForSave(createEntity: CreateEntityArgs)(implicit invocation: Invocation): FrameEntity
  def postSave(originalFrameRef: Option[FrameReference], targetFrameRef: FrameReference, schema: Schema)(implicit invocation: Invocation): FrameEntity

  /**
   * Get the error frame of the supplied frame or create one if it doesn't exist
   * @param frame the 'good' frame
   * @return the parse errors for the 'good' frame
   */
  def lookupOrCreateErrorFrame(frame: FrameEntity)(implicit invocation: Invocation): (FrameEntity, FrameEntity)

  /**
   * Get the error frame of the supplied frame
   * @param frame the 'good' frame
   * @return the parse errors for the 'good' frame
   */
  def lookupErrorFrame(frame: FrameEntity)(implicit invocation: Invocation): Option[FrameEntity]
}