package models.database

/**
 * Command for login operation
 */
trait LoginCommand {

    /**
     * execute login command.
     * @param email
     * @param statementGenerator
     * @return
     */
    def execute(email: String, statementGenerator: StatementGenerator): LoginOutput
}
