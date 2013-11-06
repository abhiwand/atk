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
import scala.Some

/**
 * Singleton object to provide white list services
 */
object Whitelists {
    def anonymousWhitelist(): database.WhiteList = {
        database.WhiteList(Some(0), Some(""))
    }

    /**
     *
     * @param email
     * @return
     */
    def exists(email: String): Boolean = {
        val whiteListResult = read(email)
        if (whiteListResult != null && whiteListResult.uid.get > 0) {
            true
        } else {
            false
        }
    }

    /**
     *
     * @param email
     * @return
     */
    def getByEmail(email: String): Query[database.WhiteLists.type, database.WhiteList] = DB.withSession {
        implicit session: scala.slick.session.Session =>
            for {w <- database.WhiteLists if w.email === email} yield w
    }

    /**
     *
     * @param email
     * @return
     */
    def read(email: String): database.WhiteList = DB.withSession {
        implicit session: scala.slick.session.Session =>
            val result = getByEmail(email).list
            if (result != null && result.length > 0) {
                result.last
            } else {
                null
            }
    }
}
