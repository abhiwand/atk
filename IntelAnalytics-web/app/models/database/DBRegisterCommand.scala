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

import play.api.db.slick._
import models.RegistrationFormMapping
import java.sql.{ResultSet, Types}
import play.api.Play.current

/**
 * Implementation of registration logic
 */
object DBRegisterCommand extends RegisterCommand {

    /**
     * see RegisterCommand
     */
    def execute(user: UserRow, registrationForm: RegistrationFormMapping, statementGenerator: StatementGenerator): RegistrationOutput = DB.withSession {
        implicit session: scala.slick.session.Session =>

            val callString = "{call sp_register(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)}";
            val cStmt = statementGenerator.getCallStatement(session, callString)
            cStmt.setString("myName", registrationForm.name)
            cStmt.setString("given_name", user.givenName)
            cStmt.setString("family_name", user.familyName)
            cStmt.setString("email", user.email)
            cStmt.setString("organization_name", registrationForm.organization_name)
            cStmt.setString("organization_phone", registrationForm.organization_phone)
            cStmt.setString("organization_email", registrationForm.organization_email)
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
     * find uid from result set
     * @param rs result set
     * @return uid
     */
    private def getUidFromResultSet(rs: ResultSet): Int = {
        var uid = 0
        if (rs.next()) uid = rs.getInt("uid")
        return uid
    }
}
