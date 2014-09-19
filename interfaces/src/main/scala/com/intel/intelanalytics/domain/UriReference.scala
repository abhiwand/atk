package com.intel.intelanalytics.domain

import java.net.URI

/**
 * Things that can be referenced with a simple URI of the form scheme://entity/id.
 */
trait UriReference extends HasId {

  /** URI scheme. Default is "ia" */
  def scheme: String = "ia"

  /** The entity id */
  def id: Long

  /** The entity type */
  def entity: String

  /** The full URI */
  def uri: String = {
    val ia_uri: String = s"$scheme://$entity/$id"
    ia_uri
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
 */
object ReferenceResolver {
  var resolvers : Map[String, Long => UriReference] = Map()
  var regex = """\w+""".r

  /**
   * Registers an URI resolver that can provide objects of a certain type
   * @param entity the name of the entity type, e.g. "graph"
   * @param resolver a function that returns an object given an ID
   */
  def register(entity: String, resolver: Long => UriReference) : Unit = {
    synchronized {
      resolvers += entity -> resolver
      regex = resolvers.keys.map(k => s"""/($k)/(\d+)""").mkString("""".+(""", "|", ")").r
    }
  }

  /**
   * Returns a reference for the given URI if possible.
   *
   * @throws IllegalArgumentException if no suitable resolver can be found for the entity type in the URI
   */
  def resolve(uri: String): UriReference = {
    require(new URI(uri) != null, "Invalid URI")
    val regexMatch = regex.findFirstMatchIn(uri)
                            .getOrElse(throw new IllegalArgumentException("Could not find entity name in " + uri))
    val entity : String = ???
    val id : Long = ???
    val resolver = resolvers.getOrElse(entity,
                throw new IllegalArgumentException(s"No resolver found for entity: $entity"))
    val resolved = resolver(id)
    resolved
  }
}
