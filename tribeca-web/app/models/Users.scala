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

package models

import models.database._
import play.api.Play.current
import play.api.db.slick.Config.driver.simple._

import play.api.db.slick.DB

import java.sql.{ResultSet, Types}

/**
 * Singleton object to provide user related services.
 */
object Users {

    /**
     *
     * @param user
     * @param registrationForm
     * @param statementGenerator
     * @return
     */
    def register(user: UserRow, registrationForm: RegistrationFormMapping, statementGenerator: StatementGenerator, registerCommand : RegisterCommand): RegistrationOutput =
    {
        registerCommand.execute(user, registrationForm, statementGenerator)
    }

    /**
     *
     * @param rs
     * @return
     */
    private def getUidFromResultSet(rs: ResultSet): Int = {
        var uid = 0
        if (rs.next()) uid = rs.getInt("uid")
        return uid
    }

    /**
     *
     * @param email
     * @param statementGenerator
     * @return
     */
    def login(email: String, statementGenerator: StatementGenerator, loginCommand: LoginCommand): LoginOutput =
    {
        loginCommand.execute(email, statementGenerator)
    }

    /**
     *
     * @param uid
     * @return
     */
    def getByUid(uid: Long): Query[(database.UserTable.type, database.WhiteListTable.type), (UserRow, WhiteListRow)] = DB.withSession {
        implicit session: scala.slick.session.Session =>
            return for {(u, w) <- database.UserTable leftJoin database.WhiteListTable on (_.uid === _.uid) if u.uid === uid} yield (u, w)
    }

    /**
     *
     * @param email
     * @return
     */
    def getByEmail(email: String): Query[database.UserTable.type, database.UserRow] = DB.withSession {
        implicit session: scala.slick.session.Session =>
            for {u <- database.UserTable if u.email === email} yield u
    }

    /**
     *
     * @return
     */
    def anonymousUser(): UserRow = {
        UserRow(Some(0), "", "", "", false, None, None, None)
    }

    /**
     *
     * @param email
     * @return
     */
    def exists(email: String): Boolean = DB.withSession {
        implicit session: scala.slick.session.Session =>
            val usersResult = readByEmail(email)
            if (usersResult != null && usersResult.uid.get > 0) {
                true
            } else {
                false
            }
    }

    /**
     *
     * @param user
     * @return
     */
    def create(user: database.UserRow): Long = DB.withSession {
        implicit session: scala.slick.session.Session =>
            database.UserTable.insert(user)
    }

    /**
     *
     * @param uid
     * @return
     */
    def readByUid(uid: Long): (database.UserRow, database.WhiteListRow) = DB.withSession {
        implicit session: scala.slick.session.Session =>
            val users = getByUid(uid).list
            if (users.length > 0) {
                users.last
            } else {
                null
            }
    }

    /**
     *
     * @param email
     * @return
     */
    def readByEmail(email: String): database.UserRow = DB.withSession {
        implicit session: scala.slick.session.Session =>
            val getResult = getByEmail(email).list
            if (getResult.length > 0) {
                getResult.last
            } else {
                null
            }
    }


}
