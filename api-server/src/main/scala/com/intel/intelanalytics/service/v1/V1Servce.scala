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

package com.intel.intelanalytics.service.v1

import spray.routing._
import com.intel.intelanalytics._
import com.intel.intelanalytics.domain._
import akka.event.Logging
import spray.json._
import spray.http.{ HttpHeader, Uri, StatusCodes, MediaTypes }
import scala.Some
import com.intel.intelanalytics.domain.DataFrame
import com.intel.intelanalytics.repository.{ MetaStoreComponent, Repository }
import com.intel.intelanalytics.service.EventLoggingDirectives
import com.intel.intelanalytics.service.v1.viewmodels._
import com.intel.intelanalytics.engine.{ EngineComponent }
import scala.util._
import scala.concurrent._
import spray.util.LoggingContext
import com.typesafe.config.ConfigFactory
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.domain.User
import scala.util.Success
import com.intel.intelanalytics.security.UserPrincipal
import scala.util.Failure
import PartialFunction._
import spray.routing.authentication.BasicAuth
import shapeless._
import spray.routing._
import Directives._

//TODO: Is this right execution context for us?

import ExecutionContext.Implicits.global
import com.intel.intelanalytics.domain.DataFrameTemplate
import com.intel.intelanalytics.domain.DataFrame
import com.intel.intelanalytics.service.v1.viewmodels.DecoratedDataFrame
import scala.util.control.NonFatal
import scala.util.Failure
import com.intel.intelanalytics.domain.DataFrameTemplate
import scala.util.Success
import com.intel.intelanalytics.domain.DataFrame
import com.intel.intelanalytics.service.v1.viewmodels.JsonTransform
import com.intel.intelanalytics.service.v1.viewmodels.DecoratedDataFrame
import scala.concurrent.duration._

trait V1Service extends Directives with EventLoggingDirectives {
  this: V1Service with MetaStoreComponent with EngineComponent =>

  val config = ConfigFactory.load()
  val defaultCount = config.getInt("intel.analytics.api.defaultCount")
  val defaultTimeout: FiniteDuration = config.getInt("intel.analytics.api.defaultTimeout") seconds

  //TODO: internationalization

  def getErrorMessage[T](value: Try[T]): String = value match {
    case Success(x) => ""
    case Failure(ex) => ex.getMessage
  }

  def errorHandler = {
    ExceptionHandler {
      case e: IllegalArgumentException => {
        error("An error occurred during request processing.", exception = e)
        complete(StatusCodes.BadRequest, "Bad request: " + e.getMessage)
      }
      case NonFatal(e) => {
        error("An error occurred during request processing.", exception = e)
        complete(StatusCodes.InternalServerError, "An internal server error occurred")
      }
    }
  }

  def getUserPrincipalFromHeader(header: HttpHeader): Option[UserPrincipal] =
    condOpt(header) {
      case h if h.is("authorization") => Await.result(getUserPrincipal(h.value), defaultTimeout)
    }

  def authenticateKey: Directive1[UserPrincipal] =
    //TODO: proper authorization with spray authenticate directive in a manner similar to S3.
    optionalHeaderValue(getUserPrincipalFromHeader).flatMap {
      case Some(p) => provide(p)
      case None => reject(AuthenticationFailedRejection(AuthenticationFailedRejection.CredentialsMissing, List()))
    }

  def std(eventCtx: String): Directive1[UserPrincipal] = {
    eventContext(eventCtx) &
      handleExceptions(errorHandler) &
      logResponse(eventCtx, Logging.InfoLevel) &
      authenticateKey
  }

  import ViewModelJsonProtocol._

  //TODO: needs to be updated for the distinction between Foos and FooTemplates
  //This code is likely to be useful for CRUD operations that need to work with the
  //metastore, such as web hooks. However, nothing is using it yet, so it's commented out.
  //  def crud[Entity <: HasId : RootJsonFormat : TypeTag,
  //            Index : RootJsonFormat,
  //            Decorated : RootJsonFormat]
  //          (prefix: String,
  //           repo: Repository[metaStore.Session, Entity],
  //           decorator: EntityDecorator[Entity, Index, Decorated]): Route = {
  //    require(prefix != null)
  //    require(repo != null)
  //    require(decorator != null)
  //    path (prefix) {
  //      val typeName = typeOf[Entity].typeSymbol.name
  //      std(get, prefix) { uri =>
  //        complete {
  //          metaStore.withSession("list " + typeName) { implicit session =>
  //            decorator.decorateForIndex(uri.toString, repo.scan())
  //          }
  //        }
  //      } ~
  //      std(post, prefix) { uri =>
  //        entity(as[Entity]) { entity =>
  //          metaStore.withSession("create " +  typeName) { implicit session =>
  //            val copy = repo.insert(entity).get
  //            val id = copy.id
  //            val links = List(Rel.self(uri + "/" + id))
  //            complete {
  //              decorator.decorateEntity(uri.toString, links, copy)
  //            }
  //          }
  //        }
  //      }
  //    } ~
  //    pathPrefix(prefix / LongNumber) { id =>
  //      std(get, prefix) { uri =>
  //        val typeName = typeOf[Entity].typeSymbol.name
  //        metaStore.withSession("get " +  typeName) { implicit session =>
  //          repo.lookup(id) match {
  //            case Some(f) => {
  //              val links = List(Rel.self(uri + "/" + id))
  //              complete {decorator.decorateEntity(uri.toString, links, f)}
  //            }
  //            case _ => reject()
  //          }
  //        }
  //      }
  //    }
  //  }
  val frameIdRegex = "/dataframes/(\\d+)".r

  def getFrameId(url: String): Option[Long] = {
    val id = frameIdRegex.findFirstMatchIn(url).map(m => m.group(1))
    id.map(s => s.toLong)
  }

  def getUserPrincipal(apiKey: String): Future[UserPrincipal] = {
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
