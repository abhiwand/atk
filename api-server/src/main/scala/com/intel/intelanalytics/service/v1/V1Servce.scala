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
import spray.http.{Uri, StatusCodes, MediaTypes}
import scala.Some
import com.intel.intelanalytics.domain.DataFrame
import com.intel.intelanalytics.repository.{MetaStoreComponent, Repository}
import com.intel.intelanalytics.service.EventLoggingDirectives
import com.intel.intelanalytics.service.v1.viewmodels._
import com.intel.intelanalytics.engine.{EngineComponent}
import scala.util._
import scala.concurrent.ExecutionContext
import spray.util.LoggingContext

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

trait V1Service extends Directives with EventLoggingDirectives {
  this: V1Service
    with MetaStoreComponent
    with EngineComponent =>

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

  def std(eventCtx: String) = {
    eventContext(eventCtx) &
      handleExceptions(errorHandler) &
      logResponse(eventCtx, Logging.InfoLevel)
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
}
