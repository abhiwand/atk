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

import play.api.Play.current
import play.api.db.slick.Config.driver.simple._
import play.api.db.slick.DB
import models.database.Session

object Sessions {
    var table = database.Sessions

    def getById(sessionId: String): Query[database.Sessions.type, database.Session] = DB.withSession {
        implicit session: scala.slick.session.Session =>
            return for {se <- table if se.Id === sessionId} yield se
    }

    //crud
    def create(uid: Long): String = DB.withSession {
        implicit session: scala.slick.session.Session =>
            val sessionId = createSessionId
            val successful = table.insert(Session(sessionId, uid, "", System.currentTimeMillis / 1000))
            if (successful == 1) {
                return sessionId
            } else {
                return null
            }
    }

    def read(sessionId: String): Option[database.Session] = DB.withSession {
        implicit session: scala.slick.session.Session =>
            val userSessions = getById(sessionId).list

            if (userSessions.length > 0)
                Some(userSessions.last)
            else
                None
    }

    def update(userSession: models.database.Session) = DB.withSession {
        implicit session: scala.slick.session.Session =>
            val userSes = getById(userSession.Id)
            userSes.update(userSession)
    }

    def delete(sessionId: String) = DB.withSession {
        implicit session: scala.slick.session.Session =>
            val userSes = getById(sessionId)
            userSes.delete
    }


    def createSessionId(): String = {
        java.util.UUID.randomUUID().toString()
    }
}
