package models.database


/**
 * Created with IntelliJ IDEA.
 * User: schen55
 * Date: 11/8/13
 * Time: 3:07 PM
 * To change this template use File | Settings | File Templates.
 */
trait GetWhiteListEntryCommand {
    def execute(email: String): Option[WhiteListRow]
}
