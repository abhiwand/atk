package com.intel.intelanalytics.domain

import com.intel.intelanalytics.domain.DomainJsonProtocol.PatternIndex
import com.intel.intelanalytics.domain.frame.FrameReference
import com.intel.intelanalytics.domain.graph.GraphReference

trait IAUri {
  def id: Long
  def entity: String

  def uri: String = {
    val ia_uri: String = "ia://" + entity + "/" + id
    return ia_uri
  }
}

object IAUriFactory {
  def getReference(uri: String, entityName: Option[String] = None): HasId = {

    val validateUri = """.+//frame/(\d+)|.+//graph/(\d+)""".r
    var entity: String = ""
    /* Validate uri structure and retrieve the entity and id from the uri*/
    if ((validateUri findFirstIn uri).mkString(",").isEmpty) {
      throw new IllegalArgumentException("Invalid uri")
    }
    else {

      entityName match {
        case Some(entityName) => {
          entity = ("graph|frame".r findFirstIn uri).mkString
          if (!(entity equals Some(entityName))) {
            throw new IllegalArgumentException("Inconsistent entity name")
          }
        }
        case None => entity = ("graph|frame".r findFirstIn uri).mkString
      }

      val id: Long = ("""(\d+)""".r findFirstIn uri).mkString.toLong

      /* Instantiate appropriate Reference depending on the entity */
      entity match {
        case "graph" =>
          GraphReference(id)

        case "frame" =>
          FrameReference(id)

        case _ => throw new IllegalArgumentException("Invalid uri")
      }
    }
  }
}
