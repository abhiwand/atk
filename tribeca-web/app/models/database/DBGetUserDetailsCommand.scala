//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2013 Intel Corporation All Rights Reserved.
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

import models.database

import play.api.Play.current
import play.api.db.slick.Config.driver.simple._

import play.api.db.slick.DB


object DBGetUserDetailsCommand extends GetUserDetailsCommand {
    /**
     *
     * @param uid
     * @return
     */
    def executeById(uid: Long): Option[(UserRow, WhiteListRow)] = DB.withSession {
        implicit session: scala.slick.session.Session =>
            val users = getByUid(uid).list
            if (users.length > 0) {
                Some(users.last)
            } else {
                None
            }
    }

    def executeByEmail(email: String): Option[UserRow] = DB.withSession {
        implicit session: scala.slick.session.Session =>
            val getResult = getByEmail(email).list
            if (getResult.length > 0) {
                Some(getResult.last)
            } else {
                None
            }
    }

    /**
     *
     * @param uid
     * @return
     */
    private def getByUid(uid: Long): Query[(database.UserTable.type, database.WhiteListTable.type), (UserRow, WhiteListRow)] = DB.withSession {
        implicit session: scala.slick.session.Session =>
            return for {(u, w) <- database.UserTable leftJoin database.WhiteListTable on (_.uid === _.uid) if u.uid === uid} yield (u, w)
    }

    /**
     *
     * @param email
     * @return
     */
    private def getByEmail(email: String): Query[database.UserTable.type, database.UserRow] = DB.withSession {
        implicit session: scala.slick.session.Session =>
            for {u <- database.UserTable if u.email === email} yield u
    }
}
