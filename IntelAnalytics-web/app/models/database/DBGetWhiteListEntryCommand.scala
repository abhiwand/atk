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

/**
 * Command to find white list entry
 */
object DBGetWhiteListEntryCommand extends GetWhiteListEntryCommand {

    /**
     * see GetWhiteListEntryCommand
     */
    def execute(email: String): Option[WhiteListRow] = DB.withSession {
        implicit session: scala.slick.session.Session =>
            val result = getByEmail(email).list
            if (result != null && result.length > 0) {
                Some(result.last)
            } else {
                None
            }
    }


    /**
     * find white list entry by email.
     * @param email
     * @return
     */
    private def getByEmail(email: String): Query[database.WhiteListTable.type, database.WhiteListRow] = DB.withSession {
        implicit session: scala.slick.session.Session =>
            for {w <- database.WhiteListTable if w.email === email} yield w
    }
}
