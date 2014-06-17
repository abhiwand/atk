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

package models.database

import play.api.db.slick.Config.driver.simple._

/**
 * Session entry.
 * @param Id
 * @param uid
 * @param data
 * @param timestamp
 */
case class SessionRow(Id: String, uid: Long, data: String, var timestamp: Long)

/**
 * Table mapping for Sessions table.
 */
object SessionTable extends Table[SessionRow]("Sessions") {
  def Id = column[String]("Id", O.PrimaryKey)
  def uid = column[Long]("uid", O.NotNull)
  def data = column[String]("data")
  def timeStamp = column[Long]("timeStamp")
  def * = Id ~ uid ~ data ~ timeStamp <> (SessionRow, SessionRow.unapply _)
}
