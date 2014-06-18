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
import com.intel.intelanalytics.domain.{ DomainJsonProtocol, User }
import spray.json._
import com.intel.intelanalytics.repository.MetaStore
import scala.concurrent.duration._
import com.intel.intelanalytics.shared.EventLogging
import org.apache.commons.lang.StringUtils

/**
 * Uses authorization HTTP header and metaStore to authenticate a user
 */
class AuthenticationDirective(val metaStore: MetaStore) extends Directives with EventLogging {

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
      case h if h.is("authorization") => Await.result(getUserPrincipal(h.value), ApiServiceConfig.defaultTimeout)
    }

  protected def getUserPrincipal(apiKey: String): Future[UserPrincipal] = {
    future {
      metaStore.withSession("Getting user principal") { implicit session =>
        if (StringUtils.isBlank(apiKey)) {
          throw new SecurityException("Api key was not provided")
        }
        val users: List[User] = metaStore.userRepo.retrieveByColumnValue("api_key", apiKey)
        users match {
          case Nil => {
            import DomainJsonProtocol._
            metaStore.userRepo.scan().foreach(u => info(u.toJson.prettyPrint))
            throw new SecurityException("User not found with apiKey:" + apiKey)
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
