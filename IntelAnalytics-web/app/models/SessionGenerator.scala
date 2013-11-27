package models
import scala.slick.lifted.Query

/**
 * Generate session object
 */
trait SessionGenerator {
    /**
     * get session by id.
     * @param sessionId
     * @return tutple of (session)
     */
    def getById(sessionId: String): Query[database.SessionTable.type, database.SessionRow]

    /**
     * create session.
     * @param uid
     * @return session id
     */
    def create(uid: Long): Option[String]

    /**
     * get session info.
     * @param sessionId
     * @return sessionRow
     */
    def read(sessionId: String): Option[database.SessionRow]

    /**
     * update session info.
     * @param userSession
     * @return
     */
    def update(userSession: models.database.SessionRow)

    /**
     * delete session.
     * @param sessionId
     * @return
     */
    def delete(sessionId: String)
    /**
     * generate session id.
     * @return
     */
    def createSessionId(): String
}
