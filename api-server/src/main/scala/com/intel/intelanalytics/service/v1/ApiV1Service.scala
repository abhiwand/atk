package com.intel.intelanalytics.service.v1

import spray.routing._
import com.intel.intelanalytics._
import com.intel.intelanalytics.domain._
import akka.event.Logging
import spray.json._
import spray.http.{Uri, StatusCodes, MediaTypes}
import scala.Some
import com.intel.intelanalytics.domain.DataFrame
import scala.reflect.runtime.universe._
import com.intel.intelanalytics.repository.{MetaStoreComponent, Repository}
import com.intel.intelanalytics.service.EventLoggingDirectives
import com.intel.intelanalytics.service.v1.viewmodels._
import com.intel.intelanalytics.engine.{Builtin, Functional, EngineComponent}
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext
import ExecutionContext.Implicits.global
import scala.util.Failure
import com.intel.intelanalytics.domain.DataFrameTemplate
import scala.util.Success
import com.intel.intelanalytics.domain.DataFrame
import com.intel.intelanalytics.service.v1.viewmodels.DecoratedDataFrame
import scala.util.control.NonFatal

//TODO: Is this right execution context for us?


trait ApiV1Service extends Directives with EventLoggingDirectives {
                                              this:ApiV1Service
                                                with MetaStoreComponent
                                                with EngineComponent =>

  import ViewModelJsonProtocol._

  def std(method: Directive0, eventCtx: String) = {
    method &  eventContext(eventCtx) & logResponse(eventCtx, Logging.InfoLevel) & requestUri
  }

  //TODO: are there parts of this that are worth using for things
  // that aren't stored in the metastore, e.g. dataframes?
  //TODO: needs to be updated for the distinction between Foos and FooTemplates
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

  def frameRoutes() = {
    //TODO: standardize and extract error handling and messaging
    import ViewModelJsonProtocol._
//    crud[DataFrame,
//          DataFrameHeader,
//          DecoratedDataFrame]("dataframes", metaStore.frameRepo, Decorators.frames)
    val prefix = "dataframes"

    def decorate(uri: Uri, frame: DataFrame): DecoratedDataFrame = {
      val links = List(Rel.self(uri.toString))
      Decorators.frames.decorateEntity(uri.toString, links, frame)
    }

    def getErrorMessage[T](value: scala.util.Try[T]): String = value match {
      case Success(x) => ""
      case Failure(ex) => ex.getMessage
    }


    pathPrefix(prefix / LongNumber) { id =>
      std(post, "transforms") { uri =>
        entity(as[JsonTransform]) { xform =>
          (xform.language, xform.name) match {
            case ("builtin", "load") =>  {
              val args = scala.util.Try { xform.arguments.get.convertTo[LoadFile] }
              validate(args.isSuccess, "Failed to parse file load descriptor: " + getErrorMessage(args)) {
                onComplete(
                  for {
                    frame <- engine.getFrame(id)
                    res <- engine.appendFile(frame, args.get.source, new Builtin("line/csv"))
                  } yield res) {
                  case Success(r) => complete(decorate(uri, r))
                  case Failure(ex) => complete(StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}")
                }
              }
            }
            case (x,y) => complete(StatusCodes.NotImplemented, "This transformation is not yet supported")
          }
        }
      } ~
      std(get, "data") { uri =>
        parameters('offset.as[Int], 'count.as[Int]) { (offset, count) =>
          onComplete(for { r <- engine.getRows(id, offset, count) } yield r) {
            case Success(rows:Iterable[Array[Array[Byte]]]) => {
              import DefaultJsonProtocol._
              val strings : List[List[String]] = rows.map(r => r.map(bytes => new String(bytes)).toList).toList
              complete(strings)
            }
            case Failure(ex) => complete(StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}")
        }
      }~
          std(get, prefix) { uri =>
            onComplete(engine.getFrame(id)) {
              case Success(frame) => {
                val decorated = decorate(uri, frame)
                complete {decorated}
              }
              case _ => reject()
            }
          } ~
          std(delete, prefix) { uri => {
            onComplete(for {
              f <- engine.getFrame(id)
              res <- engine.delete(f)
            } yield res) {
              case Success(frames) => complete("OK")
              case Failure(ex) => complete(StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}")
            }
          }
          }
      }
    } ~
    std(get, prefix) { uri =>
      //TODO: cursor
      onComplete(engine.getFrames(0, 20)) {
        case Success(frames) => complete(Decorators.frames.decorateForIndex(uri.toString, frames))
        case Failure(ex) => complete(StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}")
      }
    } ~
    std(post, prefix) { uri =>
      import DomainJsonProtocol._
      entity(as[DataFrameTemplate]) { frame =>
        onComplete(engine.create(frame)) {
          case Success(frame) => complete(decorate(uri, frame))
          case Failure(ex) => complete(StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}")
        }
      }
    }
  }

  def apiV1Service: Route = {
//      clusters ~
//      users ~
    frameRoutes()

  }
}
