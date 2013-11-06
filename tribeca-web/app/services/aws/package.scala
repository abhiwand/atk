package services

import play.api.Play
import play.api.Play.current

package object aws {
  val access_key = Play.application.configuration.getString("aws.access_key").get
  val secret_access_key = Play.application.configuration.getString("aws.secret_access_key").get

}
