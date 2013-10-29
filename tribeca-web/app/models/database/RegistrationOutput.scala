package models.database

/**
 * Created with IntelliJ IDEA.
 * User: schen55
 * Date: 10/28/13
 * Time: 3:11 PM
 * To change this template use File | Settings | File Templates.
 */
class RegistrationOutput(code : Int, message : String, loginAfterRegistration : Int, userId : Long) extends ProcedureOutput {
    val uid = userId
    val login = loginAfterRegistration
    val errorCode = code
    val errorMessage = message
}
