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

import scala.util.Try
import com.intel.intelanalytics.domain._
import com.intel.intelanalytics.engine.Engine
import scala.concurrent._
import spray.http.Uri
import spray.routing.{ ValidationRejection, Directives, Route }
import scala.util.Failure
import scala.util.Success
import com.intel.intelanalytics.security.UserPrincipal
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._
import com.intel.intelanalytics.service.v1.viewmodels._
import com.intel.intelanalytics.service.v1.viewmodels.ViewModelJsonImplicits._
import com.intel.intelanalytics.domain.command.{ CommandPost, Execution, CommandTemplate, Command }
import com.intel.intelanalytics.service.{ ApiServiceConfig, CommonDirectives }
import com.intel.intelanalytics.service.v1.decorators.CommandDecorator
import com.intel.intelanalytics.spray.json.IADefaultJsonProtocol
import com.intel.event.EventLogging

//TODO: Is this right execution context for us?
import ExecutionContext.Implicits.global

/**
 * REST API Command Service
 */
class CommandService(commonDirectives: CommonDirectives, engine: Engine) extends Directives with EventLogging {

  /**
   * Creates a view model for return through the HTTP protocol
   *
   * @param uri The link representing the command.
   * @param command The command being decorated
   * @return View model of the command.
   */
  def decorate(uri: Uri, command: Command): GetCommand = {
    //TODO: add other relevant links
    val links = List(Rel.self(uri.toString()))
    CommandDecorator.decorateEntity(uri.toString(), links, command)
  }

  /**
   * The spray routes defining the command service.
   */
  def commandRoutes() = {
    commonDirectives("commands") { implicit principal: UserPrincipal =>
      pathPrefix("commands" / LongNumber) {
        id =>
          pathEnd {
            requestUri {
              uri =>
                get {
                  onComplete(engine.getCommand(id)) {
                    case Success(Some(command)) => complete(decorate(uri, command))
                    case _ => reject()
                  }
                } ~
                  post {
                    entity(as[JsonTransform]) {
                      xform =>
                        {
                          val action = xform.arguments.get.convertTo[CommandPost]
                          action.status match {
                            case "cancel" => onComplete(engine.cancelCommand(id)) {
                              case Success(command) => complete("Command cancelled by client")
                              case _ => reject()
                            }
                          }
                        }
                    }

                  }
            }
          }
      } ~
        pathPrefix("commands") {
          path("definitions") {
            get {
              import IADefaultJsonProtocol.listFormat
              import DomainJsonProtocol.commandDefinitionFormat
              complete(engine.getCommandDefinitions().toList)
            }
          } ~
            pathEnd {
              requestUri {
                uri =>
                  get {
                    //TODO: cursor
                    import spray.json._
                    import ViewModelJsonImplicits._
                    onComplete(engine.getCommands(0, ApiServiceConfig.defaultCount)) {
                      case Success(commands) => complete(CommandDecorator.decorateForIndex(uri.toString(), commands))
                      case Failure(ex) => throw ex
                    }
                  } ~
                    post {
                      entity(as[JsonTransform]) {
                        xform =>
                          val template = CommandTemplate(name = xform.name, arguments = xform.arguments)
                          info(s"Received command template for execution: $template")
                          try {
                            engine.execute(template) match {
                              case Execution(command, futureResult) =>
                                complete(decorate(uri + "/" + command.id, command))
                            }
                          }
                          catch {
                            case e: DeserializationException =>
                              reject(ValidationRejection(
                                s"Incorrectly formatted JSON found while parsing command '${xform.name}':" +
                                  s" ${e.getMessage}", Some(e)))
                          }
                      }
                    }
              }

            }
        }
    }
  }

  //TODO: internationalization
  def getErrorMessage[T](value: Try[T]): String = value match {
    case Success(x) => ""
    case Failure(ex) => ex.getMessage
  }
}
