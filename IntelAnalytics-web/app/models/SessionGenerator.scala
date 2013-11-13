package models
import scala.slick.lifted.Query

/**
 * Created with IntelliJ IDEA.
 * User: schen55
 * Date: 11/6/13
 * Time: 11:06 AM
 * To change this template use File | Settings | File Templates.
 */
trait SessionGenerator {
    /**
     *
     * @param sessionId
     * @return
     */
    def getById(sessionId: String): Query[database.SessionTable.type, database.SessionRow]

    /**
     *
     * @param uid
     * @return
     */
    def create(uid: Long): Option[String]

    /**
     *
     * @param sessionId
     * @return
     */
    def read(sessionId: String): Option[database.SessionRow]

    /**
     *
     * @param userSession
     * @return
     */
    def update(userSession: models.database.SessionRow)

    /**
     *
     * @param sessionId
     * @return
     */
    def delete(sessionId: String)
    /**
     *
     * @return
     */
    def createSessionId(): String
}
