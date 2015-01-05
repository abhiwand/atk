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
import com.intel.intelanalytics.domain.frame.{ DataFrame, DataFrameTemplate, _ }
import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.engine.Rows._
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.security.UserPrincipal

trait FrameStorage {

  type Context
  type Data

  def expectFrame(frameId: Long)(implicit invocation: Invocation): DataFrame
  def expectFrame(frameRef: FrameReference)(implicit invocation: Invocation): DataFrame

  def lookup(id: Long)(implicit invocation: Invocation): Option[DataFrame]
  def lookupByName(name: String)(implicit invocation: Invocation): Option[DataFrame]
  def getFrames()(implicit invocation: Invocation): Seq[DataFrame]
  def create(frameTemplate: DataFrameTemplate)(implicit invocation: Invocation): DataFrame
  def renameFrame(frame: DataFrame, newName: String)(implicit invocation: Invocation): DataFrame
  def renameColumns(frame: DataFrame, name_pairs: Seq[(String, String)])(implicit invocation: Invocation): DataFrame
  def getRows(frame: DataFrame, offset: Long, count: Int)(implicit invocation: Invocation): Iterable[Row]
  def drop(frame: DataFrame)(implicit invocation: Invocation)
  def loadFrameData(context: Context, frame: DataFrame)(implicit invocation: Invocation): Data
  def saveFrameData(frame: FrameReference, data: Data)(implicit invocation: Invocation): DataFrame

  /**
   * Get the error frame of the supplied frame or create one if it doesn't exist
   * @param frame the 'good' frame
   * @return the parse errors for the 'good' frame
   */
  def lookupOrCreateErrorFrame(frame: DataFrame)(implicit invocation: Invocation): (DataFrame, DataFrame)

  /**
   * Get the error frame of the supplied frame
   * @param frame the 'good' frame
   * @return the parse errors for the 'good' frame
   */
  def lookupErrorFrame(frame: DataFrame)(implicit invocation: Invocation): Option[DataFrame]
}