//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

package models.database

import play.api.db.slick.Config.driver.simple._

/**
 * Mapping to data submitted from registration form.
 * @param uid
 * @param myName
 * @param organization_name
 * @param organization_phone
 * @param organization_email
 * @param experience
 * @param role
 * @param whyParticipate
 * @param whatTools
 */
case class Registration(uid:Long,myName:String,
                        organization_name: String,  organization_phone: String,organization_email: String,
                        experience:Int, role:String, whyParticipate:String, whatTools:String )

/**
 * Table mapping for user_registration table.
 */
object Registrations extends Table[Registration]("user_registration"){
  def uid = column[Long]("uid", O.PrimaryKey)
  def myName = column[String]("myName")
  def organization_name = column[String]("organization_name")
  def organization_phone = column[String]("organization_phone")
  def organization_email = column[String]("organization_email")
  def experience = column[Int]("experience")
  def role = column[String]("role")
  def whyParticipate = column[String]("why_participate")
  def whatTools = column[String]("what_tools")
  def * = uid ~ myName ~ organization_name ~ organization_phone ~ organization_email ~ experience ~ role ~ whyParticipate ~ whatTools <> (Registration, Registration.unapply _)
}
