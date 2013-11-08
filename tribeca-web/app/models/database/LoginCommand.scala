package models.database

/**
 * Created with IntelliJ IDEA.
 * User: schen55
 * Date: 11/8/13
 * Time: 10:55 AM
 * To change this template use File | Settings | File Templates.
 */
trait LoginCommand {
    def execute(email: String, statementGenerator: StatementGenerator): LoginOutput
}
