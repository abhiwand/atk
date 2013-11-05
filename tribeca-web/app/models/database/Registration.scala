package models.database

import play.api.db.slick.Config.driver.simple._

case class Registration(uid:Long,name:String,
                        organization_name: String,  organization_phone: String,organization_email: String,
                        experience:Int, role:String, whyParticipate:String, whatTools:String )

object Registrations extends Table[Registration]("user_registration"){
  def uid = column[Long]("uid", O.PrimaryKey)
  def name = column[String]("name")
  def organization_name = column[String]("organization_name")
  def organization_phone = column[String]("organization_phone")
  def organization_email = column[String]("organization_email")
  def experience = column[Int]("experience")
  def role = column[String]("role")
  def whyParticipate = column[String]("why_participate")
  def whatTools = column[String]("what_tools")
  def * = uid ~ name ~ organization_name ~ organization_phone ~ organization_email ~ experience ~ role ~ whyParticipate ~ whatTools <> (Registration, Registration.unapply _)
}
