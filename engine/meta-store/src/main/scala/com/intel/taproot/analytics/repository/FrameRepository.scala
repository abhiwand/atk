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

package com.intel.taproot.analytics.repository

import com.intel.taproot.analytics.domain.frame.{ FrameEntity, DataFrameTemplate }
import com.intel.taproot.analytics.domain.schema.DataTypes.DataType
import com.intel.taproot.analytics.domain.schema.{ Schema, DataTypes }
import DataTypes.DataType
import com.intel.taproot.analytics.security.UserPrincipal

import scala.util.Try

trait FrameRepository[Session] extends Repository[Session, DataFrameTemplate, FrameEntity] with NameableRepository[Session, FrameEntity] with GarbageCollectableRepository[Session, FrameEntity] {

  def insert(frame: FrameEntity)(implicit session: Session): FrameEntity

  def updateRowCount(frame: FrameEntity, rowCount: Option[Long])(implicit session: Session): FrameEntity

  def updateSchema(frame: FrameEntity, schema: Schema)(implicit session: Session): FrameEntity

  /** Update the errorFrameId column */
  def updateErrorFrameId(frame: FrameEntity, errorFrameId: Option[Long])(implicit session: Session): FrameEntity

  /**
   * Return all the frames
   * @param session current session
   * @return all the dataframes
   */
  def scanAll()(implicit session: Session): Seq[FrameEntity]

  def lookupByGraphId(graphId: Long)(implicit session: Session): Seq[FrameEntity]

  def isLive(frame: FrameEntity): Boolean
}