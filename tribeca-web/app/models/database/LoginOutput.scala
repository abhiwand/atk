package models.database

/**
 * Created with IntelliJ IDEA.
 * User: schen55
 * Date: 10/29/13
 * Time: 9:42 AM
 * To change this template use File | Settings | File Templates.
 */
class LoginOutput(code : Int, message : String, loginSuccessful : Int, userId : Long) extends ProcedureOutput{
    val uid = userId
    val success = loginSuccessful
    val errorCode = code
    val errorMessage = message
}
