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

import com.intel.intelanalytics.service.v1.viewmodels.Rel
import scala.util.Try
import com.intel.intelanalytics.domain._
import spray.json.JsObject
import com.intel.intelanalytics.repository.MetaStoreComponent
import com.intel.intelanalytics.engine.EngineComponent
import com.intel.intelanalytics.service.v1.viewmodels.ViewModelJsonProtocol._
import scala.concurrent._
import spray.http.Uri
import spray.routing.Route
import com.intel.intelanalytics.service.v1.viewmodels.DecoratedCommand
import scala.util.Failure
import scala.Some
import scala.util.Success
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.service.v1.viewmodels.JsonTransform
import com.intel.intelanalytics.domain.LoadLines
import com.intel.intelanalytics.domain.Command

//TODO: Is this right execution context for us?

import ExecutionContext.Implicits.global

/**
 * Trait for classes that implement the Intel Analytics V1 REST API Command Service
 */

trait V1CommandService extends V1Service {
  this: V1Service with MetaStoreComponent with EngineComponent =>

  /**
   * Creates a view model for return through the HTTP protocol
   *
   * @param uri The link representing the command.
   * @param command The command being decorated
   * @return View model of the command.
   */
  def decorate(uri: Uri, command: Command): DecoratedCommand = {
    //TODO: add other relevant links
    val links = List(Rel.self(uri.toString()))
    CommandDecorator.decorateEntity(uri.toString(), links, command)
  }

  /**
   * The spray routes defining the command service.
   */
  def commandRoutes() = {
    std("commands") { implicit principal: UserPrincipal =>
      pathPrefix("commands" / LongNumber) {
        id =>
          pathEnd {
            requestUri {
              uri =>
                get {
                  onComplete(engine.getCommand(id)) {
                    case Success(Some(command)) => {
                      val decorated = decorate(uri, command)
                      complete {
                        decorated
                      }
                    }
                    case _ => reject()
                  }
                }
            }
          }
      } ~
        (path("commands") & pathEnd) {
          requestUri {
            uri =>

              get {
                //TODO: cursor
                onComplete(engine.getCommands(0, defaultCount)) {
                  case Success(commands) => complete(CommandDecorator.decorateForIndex(uri.toString(), commands))
                  case Failure(ex) => throw ex
                }
              } ~
                post {
                  entity(as[JsonTransform]) {
                    xform => runCommand(uri, xform)
                  }
                }
          }
        }
    }
  }

  //TODO: disentangle the command dispatch from the routing
  /**
   * Command dispatcher that translates from HTTP pathname to command invocation
   * @param uri Path of command.
   * @param xform Argument parser.
   * @param user IMPLICIT. The user running the command.
   * @return Spray Route for command invocation
   */
  def runCommand(uri: Uri, xform: JsonTransform)(implicit user: UserPrincipal): Route = {
    xform.name match {
      //TODO: genericize function resolution and invocation
      case ("dataframe/load") => runFrameLoad(uri, xform)
      case ("graph/load") => runGraphLoad(uri, xform)
      case ("graph/ml/als") => runAls(uri, xform)
      case ("dataframe/filter") => runFilter(uri, xform)
      case ("dataframe/removecolumn") => runFrameRemoveColumn(uri, xform)
      case ("dataframe/addcolumn") => runFrameAddColumn(uri, xform)
      case ("dataframe/project") => runFrameProject(uri, xform)
      case ("dataframe/renamecolumn") => runFrameRenameColumn(uri, xform)
      case _ => ???
    }
  }

  def runFrameLoad(uri: Uri, xform: JsonTransform)(implicit user: UserPrincipal) = {
    {
      val test = Try {
        import DomainJsonProtocol._
        xform.arguments.get.convertTo[LoadLines[JsObject, String]]
      }
      val idOpt = test.toOption.flatMap(args => getFrameId(args.destination))
      (validate(test.isSuccess, "Failed to parse file load descriptor: " + getErrorMessage(test))
        & validate(idOpt.isDefined, "Destination is not a valid data frame URL")) {
          val args = test.get
          val id = idOpt.get
          onComplete(
            for {
              frame <- engine.getFrame(id)
              (c, f) = engine.load(LoadLines[JsObject, Long](args.source, id,
                skipRows = args.skipRows, overwrite = args.overwrite, lineParser = args.lineParser))
            } yield c) {
              case Success(c) => complete(decorate(uri + "/" + c.id, c))
              case Failure(ex) => throw ex
            }
        }
    }
  }

  /**
   * Translates tabular data into graph form and load it into a graph database.
   * @param uri Command path.
   * @param transform Translates arguments from HTTP/JSon to internal case classes.
   * @param user The user
   * @return A Spray route.
   */
  def runGraphLoad(uri: Uri, transform: JsonTransform)(implicit user: UserPrincipal): Route = {
    val test = Try {
      import DomainJsonProtocol._
      transform.arguments.get.convertTo[GraphLoad[JsObject, String, String]]
    }
    val frameIDOpt = test.toOption.flatMap(args => getFrameId(args.sourceFrameRef))
    val graphIDOpt = test.toOption.flatMap(args => getGraphId(args.graphRef))

    (validate(test.isSuccess, "Failed to parse graph load descriptor: " + getErrorMessage(test))
      & validate(frameIDOpt.isDefined, "Source dataframe is not a valid data frame URL")
      & validate(graphIDOpt.isDefined, "Target graph is not a valid graph URL")) {

        val args = test.get
        val sourceFrameID = frameIDOpt.get
        val graphID = graphIDOpt.get

        val graphLoad = GraphLoad(graphID,
          sourceFrameID,
          args.outputConfig,
          args.vertexRules,
          args.edgeRules,
          args.retainDanglingEdges,
          args.bidirectional)

        onComplete(
          for {
            frame <- engine.getFrame(sourceFrameID)
            graph <- engine.getGraph(graphID)
            (c, f) = engine.loadGraph(graphLoad)
          } yield c) {
            case Success(c) => complete(decorate(uri + "/" + c.id, c))
            case Failure(ex) => throw ex
          }
      }
  }

  def runAls(uri: Uri, transform: JsonTransform)(implicit user: UserPrincipal): Route = {
    val test = Try {
      import DomainJsonProtocol._
      transform.arguments.get.convertTo[Als[String]]
    }
    val idOpt = test.toOption.flatMap(args => getGraphId(args.graph))
    (validate(test.isSuccess, "Failed to parse file load descriptor: " + getErrorMessage(test))
      & validate( /*idOpt.isDefined*/ true, "Destination is not a valid graph URL")) {
        val args = test.get
        val id = 1 //idOpt.get
        val (c, f) = engine.runAls(args.copy[Long](graph = 1))
        complete(decorate(uri + "/" + c.id, c))
        //      onComplete(
        //          for {
        //            graph <- engine.getGraph(id)
        //            (c, f) = engine.runAls(args.copy[Long](graph = graph.id))
        //          } yield c) {
        //            case Success(c) => complete(decorate(uri + "/" + c.id, c))
        //            case Failure(ex) => throw ex
        //          }
      }
  }

  def runFilter(uri: Uri, xform: JsonTransform)(implicit user: UserPrincipal) = {
    {
      val test = Try {
        import DomainJsonProtocol._
        xform.arguments.get.convertTo[FilterPredicate[JsObject, String]]
      }
      val idOpt = test.toOption.flatMap(args => getFrameId(args.frame))
      (validate(test.isSuccess, "Failed to parse file load descriptor: " + getErrorMessage(test))
        & validate(idOpt.isDefined, "Destination is not a valid data frame URL")) {
          val args = test.get
          val id = idOpt.get
          onComplete(
            for {
              frame <- engine.getFrame(id)
              (c, f) = engine.filter(FilterPredicate[JsObject, Long](id, args.predicate))
            } yield c) {
              case Success(c) => complete(decorate(uri + "/" + c.id, c))
              case Failure(ex) => throw ex
            }
        }
    }
  }

  def runFrameRenameColumn(uri: Uri, xform: JsonTransform)(implicit user: UserPrincipal) = {
    {
      val test = Try {
        import DomainJsonProtocol._
        xform.arguments.get.convertTo[FrameRenameColumn[JsObject, String]]
      }
      val idOpt = test.toOption.flatMap(args => getFrameId(args.frame))
      (validate(test.isSuccess, "Failed to parse file load descriptor: " + getErrorMessage(test))
        & validate(idOpt.isDefined, "Destination is not a valid data frame URL")) {
          val args = test.get
          val id = idOpt.get
          onComplete(
            for {
              frame <- engine.getFrame(id)
              (c, f) = engine.renameColumn(FrameRenameColumn[JsObject, Long](id, args.originalcolumn, args.renamedcolumn))
            } yield c) {
              case Success(c) => complete(decorate(uri + "/" + c.id, c))
              case Failure(ex) => throw ex
            }
        }
    }
  }

  def runFrameAddColumn(uri: Uri, xform: JsonTransform)(implicit user: UserPrincipal) = {
    {
      val test = Try {
        import DomainJsonProtocol._
        xform.arguments.get.convertTo[FrameAddColumn[JsObject, String]]
      }
      val idOpt = test.toOption.flatMap(args => getFrameId(args.frame))
      (validate(test.isSuccess, "Failed to parse file load descriptor: " + getErrorMessage(test))
        & validate(idOpt.isDefined, "Destination is not a valid data frame URL")) {
          val args = test.get
          val id = idOpt.get
          onComplete(
            for {
              frame <- engine.getFrame(id)
              (c, f) = engine.addColumn(FrameAddColumn[JsObject, Long](id, args.columnname, args.columntype, args.expression))
            } yield c) {
              case Success(c) => complete(decorate(uri + "/" + c.id, c))
              case Failure(ex) => throw ex
            }
        }
    }
  }

  def runFrameRemoveColumn(uri: Uri, xform: JsonTransform)(implicit user: UserPrincipal) = {
    {
      val test = Try {
        import DomainJsonProtocol._
        xform.arguments.get.convertTo[FrameRemoveColumn[JsObject, String]]
      }
      val idOpt = test.toOption.flatMap(args => getFrameId(args.frame))
      (validate(test.isSuccess, "Failed to parse file load descriptor: " + getErrorMessage(test))
        & validate(idOpt.isDefined, "Destination is not a valid data frame URL")) {
          val args = test.get
          val id = idOpt.get
          onComplete(
            for {
              frame <- engine.getFrame(id)
              (c, f) = engine.removeColumn(FrameRemoveColumn[JsObject, Long](id, args.column))
            } yield c) {
              case Success(c) => complete(decorate(uri + "/" + c.id, c))
              case Failure(ex) => throw ex
            }
        }
    }
  }

  def runFrameProject(uri: Uri, xform: JsonTransform)(implicit user: UserPrincipal) = {
    {
      val test = Try {
        import DomainJsonProtocol._
        xform.arguments.get.convertTo[FrameProject[JsObject, String]]
      }
      val idOpt = test.toOption.flatMap(args => getFrameId(args.frame))
      val originalIdOpt = test.toOption.flatMap(args => getFrameId(args.originalframe))
      (validate(test.isSuccess, "Failed to parse file load descriptor: " + getErrorMessage(test))
        & validate(idOpt.isDefined, "Destination is not a valid data frame URL")) {
          val args = test.get
          val id = idOpt.get
          val originalFrameID = originalIdOpt.get
          onComplete(
            for {
              frame <- engine.getFrame(id)
              originalFrame <- engine.getFrame(originalFrameID)
              (c, f) = engine.project(FrameProject[JsObject, Long](id, originalFrameID, args.column))
            } yield c) {
              case Success(c) => complete(decorate(uri + "/" + c.id, c))
              case Failure(ex) => throw ex
            }
        }
    }
  }
}
