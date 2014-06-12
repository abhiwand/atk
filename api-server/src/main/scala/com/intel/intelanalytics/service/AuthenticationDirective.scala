package com.intel.intelanalytics.service

//TODO: Is this right execution context for us?

import scala.concurrent._
import ExecutionContext.Implicits.global

import spray.http.HttpHeader
import scala.PartialFunction._
import scala.concurrent._
import com.intel.intelanalytics.security.UserPrincipal
import scala.Some
import spray.routing._
import com.intel.intelanalytics.domain.{DomainJsonProtocol, User}
import spray.json._
import com.intel.intelanalytics.repository.MetaStore
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._
import com.intel.intelanalytics.shared.EventLogging

/**
 * Uses authorization HTTP header and metaStore to authenticate a user
 */
class AuthenticationDirective(val metaStore: MetaStore) extends Directives with EventLogging {

  val config = ConfigFactory.load()
  val defaultTimeout: FiniteDuration = config.getInt("intel.analytics.api.defaultTimeout").seconds

  /**
   * Gets authorization header and authenticates a user
   * @return the authenticated user
   */
  def authenticateKey: Directive1[UserPrincipal] =
  //TODO: proper authorization with spray authenticate directive in a manner similar to S3.
    optionalHeaderValue(getUserPrincipalFromHeader).flatMap {
      case Some(p) => provide(p)
      case None => reject(AuthenticationFailedRejection(AuthenticationFailedRejection.CredentialsMissing, List()))
    }

  protected def getUserPrincipalFromHeader(header: HttpHeader): Option[UserPrincipal] =
    condOpt(header) {
      case h if h.is("authorization") => Await.result(getUserPrincipal(h.value), defaultTimeout)
    }

  protected def getUserPrincipal(apiKey: String): Future[UserPrincipal] = {
    future {
      metaStore.withSession("Getting user principal") { implicit session =>
        val users: List[User] = metaStore.userRepo.retrieveByColumnValue("api_key", apiKey)
        users match {
          case Nil => {
            import DomainJsonProtocol._
            metaStore.userRepo.scan().foreach(u => info(u.toJson.prettyPrint))
            throw new SecurityException("User not found")
          }
          case users if users.length > 1 => throw new SecurityException("Problem accessing user credentials")
          case user => {
            val userPrincipal: UserPrincipal = new UserPrincipal(users(0), List("user")) //TODO need role definitions
            info("Authenticated user " + userPrincipal)
            userPrincipal
          }
        }
      }
    }
  }
}
