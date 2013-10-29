package models.database

/**
 * Created with IntelliJ IDEA.
 * User: schen55
 * Date: 10/28/13
 * Time: 3:11 PM
 * To change this template use File | Settings | File Templates.
 */
class RegistrationOutput(code : Int, message : String, loginAfterRegistration : Int, userId : BigInt) extends ProcedureOutput {
    var uid = userId
    var login = loginAfterRegistration
    def errorCode: Int = code
    def errorMessage : String = message
}
