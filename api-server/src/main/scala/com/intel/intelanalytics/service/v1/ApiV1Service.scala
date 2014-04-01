package com.intel.intelanalytics.service.v1

import spray.routing._
import com.intel.intelanalytics._
import com.intel.intelanalytics.domain._
import akka.event.Logging
import spray.json._
import spray.http.MediaTypes
import scala.Some
import com.intel.intelanalytics.domain.DataFrame
import scala.reflect.runtime.universe._
import com.intel.intelanalytics.repository.{MetaStoreComponent, Repository, HasId}
import com.intel.intelanalytics.service.EventLoggingDirectives
import com.intel.intelanalytics.service.v1.viewmodels.{DecoratedDataFrame, DataFrameHeader, ViewModelJsonProtocol, Rel}


trait ApiV1Service extends Directives with EventLoggingDirectives {
                                              this:ApiV1Service
                                                with MetaStoreComponent =>

  import ViewModelJsonProtocol._

  def std(method: Directive0, eventCtx: String) = {
    method &  eventContext(eventCtx) & logResponse(eventCtx, Logging.InfoLevel) & requestUri
  }

  def crud[Entity <: HasId : RootJsonFormat : TypeTag,
            Index : RootJsonFormat,
            Decorated : RootJsonFormat]
          (prefix: String,
           repo: Repository[metaStore.Session, Entity],
           decorator: EntityDecorator[Entity, Index, Decorated]): Route = {
    require(prefix != null)
    require(repo != null)
    require(decorator != null)
    path (prefix) {
      val typeName = typeOf[Entity].typeSymbol.name
      std(get, prefix) { uri =>
        complete {
          metaStore.withSession("list " + typeName) { implicit session =>
            decorator.decorateForIndex(uri.toString, repo.scan())
          }
        }
      } ~
      std(post, prefix) { uri =>
        entity(as[Entity]) { entity =>
          metaStore.withSession("create " +  typeName) { implicit session =>
            val copy = repo.insert(entity).get
            val id = copy.id.get
            val links = List(Rel.self(uri + "/" + id))
            complete {
              decorator.decorateEntity(uri.toString, links, copy)
            }
          }
        }
      }
    } ~
    pathPrefix(prefix / LongNumber) { id =>
      std(get, prefix) { uri =>
        val typeName = typeOf[Entity].typeSymbol.name
        metaStore.withSession("get " +  typeName) { implicit session =>
          repo.lookup(id) match {
            case Some(f) => {
              val links = List(Rel.self(uri + "/" + id))
              complete {decorator.decorateEntity(uri.toString, links, f)}
            }
            case _ => reject()
          }
        }
      }
    }
  }

  def frameRoutes() = {
    import ViewModelJsonProtocol._
    import com.intel.intelanalytics.domain.DomainJsonProtocol._
    crud[DataFrame,
          DataFrameHeader,
          DecoratedDataFrame]("dataframes", metaStore.frameRepo, Decorators.frames)
  }

  def apiV1Service: Route = {
//      clusters ~
//      users ~
      frameRoutes()
  }
}
