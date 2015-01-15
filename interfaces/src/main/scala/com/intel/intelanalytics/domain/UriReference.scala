package com.intel.intelanalytics.domain

import java.net.URI

import com.intel.intelanalytics.engine.{ ReferenceResolver, EntityTypeRegistry }
import com.intel.intelanalytics.engine.plugin.Invocation

import scala.util.Try

import scala.reflect.runtime.{ universe => ru }
import ru._

/**
 * Things that can be referenced with a simple URI of the form scheme://entity/id.
 */
trait UriReference extends HasId {

  /** URI scheme. Default is "ia" */
  def scheme: String = "ia"

  /** The entity id */
  def id: Long

  /** The entity name */
  def name: String = null

  /** The entity type */
  def entityType: EntityType

  /** The full URI */
  def uri: String = {
    val ia_uri: String = s"$scheme://${entityType.name.singular}/$id"
    ia_uri
  }

  override def hashCode(): Int = uri.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case x: UriReference => this.uri == x.uri
    case _ => false
  }
}

