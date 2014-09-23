package com.intel.intelanalytics.domain

import java.net.URI

import scala.util.Try

/**
 * Things that can be referenced with a simple URI of the form scheme://entity/id.
 */
trait UriReference extends HasId {

  /** URI scheme. Default is "ia" */
  def scheme: String = "ia"

  /** The entity id */
  def id: Long

  /** The entity type */
  def entity: Entity

  /** The full URI */
  def uri: String = {
    val ia_uri: String = s"$scheme://${entity.name.name}/$id"
    ia_uri
  }

  override def hashCode(): Int = uri.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case x: UriReference => this.uri == x.uri
    case _ => false
  }

  /**
   * Is this reference known to be valid at the time it was created?
   *
   * None indicates this is unknown.
   */
  def exists: Option[Boolean]
}

/**
 * Provides a way to get access to arbitrary objects in the system by using an URI.
 *
 * The methods in this class are thread safe.
 *
 * Note that this class is generally used through the companion singleton.
 */
class ReferenceResolver {
  var resolvers: Map[String, Long => UriReference] = Map()
  var regex = """<invalid>""".r

  /**
   * Registers an URI resolver that can provide objects of a certain type
   * @param entity the name of the entity type, e.g. "graph"
   * @param resolver a function that returns an object given an ID
   */
  def register(entity: Entity, resolver: Long => UriReference): Unit = {
    synchronized {
      for (name <- Seq(entity.name) ++ entity.alternatives) {
        resolvers += entity.name.name -> resolver
        resolvers += entity.name.plural -> resolver
      }
      regex = resolvers.keys.map(k => s"""($k/[0-9]+)""").mkString(""".+/""", "|", "").r
    }
  }

  /**
   * Returns a reference for the given URI if possible.
   *
   * @throws IllegalArgumentException if no suitable resolver can be found for the entity type in the URI
   */
  def resolve(uri: String): UriReference = {
    new URI(uri) //validate this is actually a URI at all
    val regexMatch = regex.findFirstMatchIn(uri)
      .getOrElse(throw new IllegalArgumentException("Could not find entity name in " + uri))

    //Error should never happen on next line, since our regex includes subgroups -
    //a match means there is at least one subgroup match as well.
    val matched = regexMatch.subgroups.find(s => s != null).getOrElse(throw new Exception("Internal error"))

    val parts = matched.split("/")
    if (parts.length < 2) {
      throw new IllegalArgumentException("No valid entity found in " + uri)
    }
    val entity = parts(0)
    val id = Try { parts(1).toLong }.getOrElse(
      throw new IllegalArgumentException(s"Could not parse entity ID in '${regexMatch.toString()}' of '$uri'"))
    val resolver = resolvers.getOrElse(entity,
      throw new IllegalArgumentException(s"No resolver found for entity: $entity"))
    val resolved = resolver(id)
    resolved
  }
}

/**
 * The default system registry of URI reference resolvers
 */
object ReferenceResolver extends ReferenceResolver {}
