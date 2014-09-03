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

import com.intel.intelanalytics.domain.frame.{ DataFrame, DataFrameTemplate, _ }
import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.engine.Rows._
import com.intel.intelanalytics.security.UserPrincipal

trait FrameStorage {

  def lookup(id: Long): Option[DataFrame]
  def lookupByName(name: String)(implicit user: UserPrincipal): Option[DataFrame]
  def getFrames(offset: Int, count: Int)(implicit user: UserPrincipal): Seq[DataFrame]
  def create(frameTemplate: DataFrameTemplate)(implicit user: UserPrincipal): DataFrame
  def dropColumns(frame: DataFrame, columnIndex: Seq[Int])(implicit user: UserPrincipal): DataFrame
  def renameFrame(frame: DataFrame, newName: String): DataFrame
  def renameColumns(frame: DataFrame, name_pairs: Seq[(String, String)]): DataFrame
  def getRows(frame: DataFrame, offset: Long, count: Int)(implicit user: UserPrincipal): Iterable[Row]
  def drop(frame: DataFrame)
  //def updateName(frame: DataFrame, newName: String)(implicit user: UserPrincipal): DataFrame
  def updateSchema(frame: DataFrame, columns: List[(String, DataType)]): DataFrame

  /**
   * Get the error frame of the supplied frame or create one if it doesn't exist
   * @param frame the 'good' frame
   * @return the parse errors for the 'good' frame
   */
  def lookupOrCreateErrorFrame(frame: DataFrame): DataFrame

  /**
   * Get the error frame of the supplied frame
   * @param frame the 'good' frame
   * @return the parse errors for the 'good' frame
   */
  def lookupErrorFrame(frame: DataFrame): Option[DataFrame]
}