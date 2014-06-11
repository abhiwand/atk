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
 * User entry.
 * @param uid
 * @param givenName
 * @param familyName
 * @param email
 * @param registered
 * @param ipythonUrl
 * @param clusterId
 */
case class UserRow(uid: Option[Long], givenName: String, familyName: String, email: String, registered: Boolean,
                ipythonUrl: Option[String], clusterId: Option[String], secret: Option[String])

/**
 * Table mapping for user_info table.
 */
object UserTable extends Table[UserRow]("user_info") {

    def uid = column[Long]("uid", O.PrimaryKey, O.AutoInc)

    def givenName = column[String]("given_name")

    def familyName = column[String]("family_name")

    def email = column[String]("email", O.NotNull)

    def registered = column[Boolean]("registered")

    def ipythonUrl = column[String]("ipythonUrl")

    def clusterId = column[String]("cluster_id", O.Nullable)

    def secret = column[String]("secret", O.Nullable)

    def * = uid.? ~ givenName ~ familyName ~ email ~ registered ~ ipythonUrl.? ~ clusterId.? ~ secret.? <>(UserRow, UserRow.unapply _)
}



