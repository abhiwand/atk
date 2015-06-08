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
  def getSizeInBytes(frameEntity: FrameEntity)(implicit invocation: Invocation): Option[Long]

  def scheduleDeletion(frame: FrameEntity)(implicit invocation: Invocation): Unit
}
