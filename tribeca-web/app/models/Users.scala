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
    def register(user: User, registrationForm: RegistrationFormMapping, statementGenerator: StatementGenerator): RegistrationOutput = DB.withSession {
        implicit session: scala.slick.session.Session =>

            val callString = "{call sp_register(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)}";
            val cStmt = statementGenerator.getCallStatement(session, callString)
            cStmt.setString("myName", registrationForm.name)
            cStmt.setString("given_name", user.givenName)
            cStmt.setString("family_name", user.familyName)
            cStmt.setString("email", user.email)
            cStmt.setString("organization_name", registrationForm.organization_name)
            cStmt.setString("organization_phone", registrationForm.organization_phone)
            cStmt.setString("organization_email", "")
            cStmt.setInt("experience", registrationForm.experience)
            cStmt.setString("role", registrationForm.role)
            cStmt.setString("why_participate", registrationForm.whyParticipate)
            cStmt.setString("what_tools", registrationForm.whatTools)
            cStmt.registerOutParameter("loginAfterRegister", Types.BIGINT)
            cStmt.registerOutParameter("errorCode", Types.BIGINT)
            cStmt.registerOutParameter("errorMessage", Types.VARCHAR)
            val hadResults = cStmt.execute()

            val loginAfterRegister = cStmt.getInt("loginAfterRegister")
            val errorCode = cStmt.getInt("errorCode")
            val errorMessage = cStmt.getString("errorMessage")

            var uid = 0
            if (hadResults) {
                uid = getUidFromResultSet(cStmt.getResultSet())
            }

            val output = new RegistrationOutput(errorCode, errorMessage, loginAfterRegister, uid)
            return output
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
    def login(email: String, statementGenerator: StatementGenerator): LoginOutput = DB.withSession {
        implicit session: scala.slick.session.Session =>

            val callString = "{call sp_login(?, ?, ?, ?)}";
            val cStmt = statementGenerator.getCallStatement(session, callString)
            cStmt.setString("email", email)
            cStmt.registerOutParameter("loginSuccessful", Types.BIGINT)
            cStmt.registerOutParameter("errorCode", Types.BIGINT)
            cStmt.registerOutParameter("errorMessage", Types.VARCHAR)
            val hadResults = cStmt.execute()

            val loginSuccessful = cStmt.getInt("loginSuccessful")
            val errorCode = cStmt.getInt("errorCode")
            val errorMessage = cStmt.getString("errorMessage")

            var uid = 0
            if (hadResults) {
                uid = getUidFromResultSet(cStmt.getResultSet())
            }

            val output = new LoginOutput(errorCode, errorMessage, loginSuccessful, uid)
            return output
    }

    /**
     *
     * @param uid
     * @return
     */
    def getByUid(uid: Long): Query[(database.Users.type, database.WhiteLists.type), (User, WhiteList)] = DB.withSession {
        implicit session: scala.slick.session.Session =>
            return for {(u, w) <- database.Users leftJoin database.WhiteLists on (_.uid === _.uid) if u.uid === uid} yield (u, w)
    }

    /**
     *
     * @param email
     * @return
     */
    def getByEmail(email: String): Query[database.Users.type, database.User] = DB.withSession {
        implicit session: scala.slick.session.Session =>
            for {u <- database.Users if u.email === email} yield u
    }

    /**
     *
     * @return
     */
    def anonymousUser(): User = {
        User(Some(0), "", "", "", false, None, None)
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
    def create(user: database.User): Long = DB.withSession {
        implicit session: scala.slick.session.Session =>
            database.Users.insert(user)
    }

    /**
     *
     * @param uid
     * @return
     */
    def readByUid(uid: Long): (database.User, database.WhiteList) = DB.withSession {
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
    def readByEmail(email: String): database.User = DB.withSession {
        implicit session: scala.slick.session.Session =>
            val getResult = getByEmail(email).list
            if (getResult.length > 0) {
                getResult.last
            } else {
                null
            }
    }


}
