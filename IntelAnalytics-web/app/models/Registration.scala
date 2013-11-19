//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2013 Intel Corporation All Rights Reserved.
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

package models

import play.api.libs.json.{Json, JsValue}
import play.api.data._
import play.api.data.Forms._
import play.api.Play.current
import play.api.db.slick.Config.driver.simple._
import play.api.db.slick.DB
import models.database.{Registration, SessionRow}


case class RegistrationFormMapping(name:String,organization_name: String, organization_phone: String,
                        experience:Int, role:String, whyParticipate:String,
                        whatTools:String, terms: String, authResult: String)

object Registrations{
  val RegistrationFormValidation = Form(
    mapping(
      "name" -> nonEmptyText,
      "organization_name" -> nonEmptyText,
      "organization_phone" -> nonEmptyText,
      "experience" -> number,
      "role" ->nonEmptyText,
      "whyParticipate" -> nonEmptyText,
      "whatTools" -> nonEmptyText,
      "terms" -> nonEmptyText,
      "authResult" -> nonEmptyText
    )(RegistrationFormMapping.apply)(RegistrationFormMapping.unapply)
  )

  //crud
  def createRegistration(registration:database.Registration): Long = DB.withSession{implicit session: scala.slick.session.Session =>
    database.Registrations.insert(registration)
  }
}