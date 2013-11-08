package models.database

import models.database


/**
 * Command to find user by id
 */
trait GetUserDetailsCommand {
    /**
     *
     * @param uid
     * @return
     */
    def executeById(uid: Long): Option[(database.UserRow, database.WhiteListRow)]
    def executeByEmail(email: String): Option[database.UserRow]
}
