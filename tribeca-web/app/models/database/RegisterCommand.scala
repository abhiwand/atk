package models.database

import models.RegistrationFormMapping

/**
 * Created with IntelliJ IDEA.
 * User: schen55
 * Date: 11/8/13
 * Time: 10:44 AM
 * To change this template use File | Settings | File Templates.
 */
trait RegisterCommand {
    def execute(user: UserRow, registrationForm: RegistrationFormMapping, statementGenerator: StatementGenerator): RegistrationOutput
}
