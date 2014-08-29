package com.intel.intelanalytics.domain


import com.intel.intelanalytics.domain.DomainJsonProtocol.PatternIndex
import com.intel.intelanalytics.domain.frame.FrameReference
import com.intel.intelanalytics.domain.graph.GraphReference


trait IAUri {
  def id: Long
  def entity: String

  def getUri: String = {
    val ia_uri: String = "ia://" + entity + "/" + id
    return ia_uri
  }
}

object IAUriFactory {
//TODO: Add check entity with entityO and test cases
  def getReference(uri: String, entityName: Option[String]=""): HasId = {
    val validateUri = """.+//frame/(\d+)|.+//graph/(\d+)""".r

  /* Validate uri structure and retrieve the entity and id from the uri*/
    if ((validateUri findFirstIn uri).mkString(",").nonEmpty) {
      val entity: String = entityName.getOrElse(("graph|frame".r findFirstIn uri).mkString)

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

    else
    {
      throw new IllegalArgumentException("Invalid uri")
    }
  }
 }
