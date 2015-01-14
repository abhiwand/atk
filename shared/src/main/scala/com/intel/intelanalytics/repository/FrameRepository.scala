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

package com.intel.intelanalytics.repository

import com.intel.intelanalytics.domain.frame.{ FrameEntity, DataFrameTemplate }
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.domain.schema.{ Schema, DataTypes }
import DataTypes.DataType
import com.intel.intelanalytics.security.UserPrincipal

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
}