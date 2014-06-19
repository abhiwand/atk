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

import com.intel.intelanalytics.domain.DomainJsonProtocol
import com.intel.intelanalytics.domain.graph.{Graph, GraphTemplate}
import com.intel.intelanalytics.engine.Engine
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.service.CommonDirectives
import com.intel.intelanalytics.service.v1.decorators.GraphDecorator
import com.intel.intelanalytics.service.v1.viewmodels.{Rel, ViewModelJsonImplicits, _}
import com.intel.intelanalytics.shared.EventLogging
import spray.http.Uri
import spray.json._
import spray.routing.Directives

import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

/**
 * REST API Graph Service
 */
class GraphService(commonDirectives: CommonDirectives, engine: Engine) extends Directives with EventLogging {

  /**
   * The spray routes defining the Graph service.
   */
  def graphRoutes() = {
    import com.intel.intelanalytics.service.v1.viewmodels.ViewModelJsonImplicits._
    val prefix = "graphs"

    /**
     * Creates "decorated graph" for return on HTTP protocol
     * @param uri handle of graph
     * @param graph graph metadata
     * @return Decorated graph for HTTP protocol return
     */
    def decorate(uri: Uri, graph: Graph): GetGraph = {
      //TODO: add other relevant links
      val links = List(Rel.self(uri.toString))
      GraphDecorator.decorateEntity(uri.toString, links, graph)
    }

    //TODO: none of these are yet asynchronous - they communicate with the engine
    //using futures, but they keep the client on the phone the whole time while they're waiting
    //for the engine work to complete. Needs to be updated to a) register running jobs in the metastore
    //so they can be queried, and b) support the web hooks.
    // note: delete is partly asynchronous - it wraps its backend delete in a future but blocks on deleting the graph
    // from the metastore

    commonDirectives(prefix) { implicit userProfile: UserPrincipal ⇒
      (path(prefix) & pathEnd) {
        requestUri { uri ⇒
          get {
            //TODO: cursor
            onComplete(engine.getGraphs(0, 20)) {
              case Success(graphs) ⇒ complete(GraphDecorator.decorateForIndex(uri.toString(), graphs))
              case Failure(ex) ⇒ throw ex
            }
          } ~
            post {
              import com.intel.intelanalytics.domain.DomainJsonProtocol._
              entity(as[GraphTemplate]) {
                graph ⇒
                  onComplete(engine.createGraph(graph)) {
                    case Success(graph) ⇒ complete(decorate(uri + "/" + graph.id, graph))
                    case Failure(ex) ⇒ throw ex
                  }
              }
            }
        }
      } ~
        pathPrefix(prefix / LongNumber) { id ⇒
          pathEnd {
            requestUri { uri ⇒
              get {
                onComplete(engine.getGraph(id)) {
                  case Success(graph) ⇒ {
                    val decorated = decorate(uri, graph)
                    complete {
                      decorated
                    }
                  }
                  case _ ⇒ reject()
                }
              } ~
                delete {
                  onComplete(for {
                    graph ← engine.getGraph(id)
                    res ← engine.deleteGraph(graph)
                  } yield res) {
                    case Success(frames) ⇒ complete("OK")
                    case Failure(ex) ⇒ throw ex
                  }
                } ~
                (path("vertices") & get) {
                  parameters('qname.as[String], 'offset.as[Int], 'count.as[Int]) { (queryName, offset, count) ⇒
                    parameterMap { params ⇒
                      onComplete(for { r ← engine.getVertices(id, offset, count, queryName, params) } yield r) {
                        case Success(rows: Iterable[Array[Any]]) ⇒ {
                          import com.intel.intelanalytics.domain.DomainJsonProtocol._
                          val strings = rows.map(r ⇒ r.map(a ⇒ a.toJson).toList).toList
                          complete(strings)
                        }
                        case Failure(ex) ⇒ throw ex
                      }
                    }
                  }
                }
            }
          }
        }
    }
  }

}
