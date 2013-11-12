package models.database

import models.RegistrationFormMapping

/**
 * command for register operation
 */
trait RegisterCommand {
    /**
     * execute register command.
     * @param user
     * @param registrationForm
     * @param statementGenerator
     * @return
     */
    def execute(user: UserRow, registrationForm: RegistrationFormMapping, statementGenerator: StatementGenerator): RegistrationOutput
}
