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


/**
 * Singleton object to provide user related services.
 */
object Users {

    /**
     * register the user.
     * @param user
     * @param registrationForm
     * @param statementGenerator
     * @return register action output
     */
    def register(user: UserRow, registrationForm: RegistrationFormMapping, statementGenerator: StatementGenerator, registerCommand: RegisterCommand): RegistrationOutput = {
        registerCommand.execute(user, registrationForm, statementGenerator)
    }

    /**
     * log in the user.
     * @param email
     * @param statementGenerator
     * @return login action output
     */
    def login(email: String, statementGenerator: StatementGenerator, loginCommand: LoginCommand): LoginOutput = {
        loginCommand.execute(email, statementGenerator)
    }


    /**
     * generate an anonymous user.
     * @return anonymouse user instance
     */
    def anonymousUser(): UserRow = {
        UserRow(Some(0), "", "", "", false, None, None, None)
    }

    /**
     * check whether user with the specific email exists.
     * @param email
     * @return flag to indicate found or not found
     */
    def exists(email: String, getUserCommand: GetUserDetailsCommand): Boolean = {

        val usersResult = readByEmail(email, getUserCommand)
        if (usersResult != None && usersResult.get.uid.get > 0)
            true
        else
            false
    }

    /**
     * create a user entry in the table.
     * @param user
     * @return
     */
    def create(user: database.UserRow): Long = DB.withSession {
        implicit session: scala.slick.session.Session =>
            database.UserTable.insert(user)
    }

    /**
     * get user info by id.
     * @param uid
     * @return tuple of (userRow, whiteListRow)
     */
    def readByUid(uid: Long, getUserCommand: GetUserDetailsCommand): Option[UserDetails] = {
        getUserCommand.executeById(uid)
    }

    /**
     * get user info by email.
     * @param email
     * @return user info
     */
    def readByEmail(email: String, getUserCommand: GetUserDetailsCommand): Option[database.UserRow] = {
        getUserCommand.executeByEmail(email)
    }
}
