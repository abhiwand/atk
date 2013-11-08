package models.database

import models.database
import play.api.Play.current
import play.api.db.slick.Config.driver.simple._
import play.api.db.slick.DB

/**
 * Command to find white list entry
 */
object DBGetWhiteListEntryCommand extends GetWhiteListEntryCommand {

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
