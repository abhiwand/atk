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
