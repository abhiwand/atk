package com.intel.intelanalytics.domain

import java.net.URI

import scala.util.Try

import scala.reflect.runtime.{ universe => ru }
import ru._

//TODO - refactor to separate files

case class SingleReference[T <: UriReference](reference: T)

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

trait ReferenceResolver {
  /**
   * Checks to see if this string might be a valid reference, without actually trying to resolve it.
   */
  def isReferenceUriFormat(s: String): Boolean

  /**
   * Returns a reference for the given URI if possible.
   *
   * @throws IllegalArgumentException if no suitable resolver can be found for the entity type in the URI.
   *                                  Note this exception will be in the Try, not actually thrown immediately.
   */
  def resolve(uri: String): Try[UriReference]

 /**
   * Returns a (possibly updated) reference.
   */
  def resolve(reference: UriReference): Try[UriReference] = {
    resolve(reference.uri)
  }

}

/**
 * Storage for entities that can process different kinds of references
 */
class EntityRegistry {

  private var _entities: Map[TypeSymbol, EntityManagement] = Map.empty

  def entities: Set[EntityManagement] = _entities.values.toSet

  /**
   * Registers an URI resolver that can provide objects of a certain type
   * @param entity the name of the entity type, e.g. "graph"
   */
  def register[E <: EntityManagement: TypeTag](entity: E): Unit = {
    synchronized {
      _entities += typeTag[E].tpe.members.find(m => m.isType && m.name.decoded == "Reference").get.asType -> entity
      _resolver = null //resolver becomes invalid when new entities added, will generate a new one
      //next time someone asks for one.
    }
  }

  /**
   * Retrieves a registered entity that works with the given reference type
   * @tparam R the reference type
   * @return an Entity that can work with that reference type
   */
  def entity[R <: UriReference: TypeTag](): Option[EntityManagement] = entityForType(typeTag[R].tpe.typeSymbol.asType)

  /**
   * Retrieves a registered entity that works with the given reference type
   * @param referenceType the type of the reference
   * @return an Entity that can work with that reference type
   */
  def entityForType(referenceType: TypeSymbol): Option[EntityManagement] = _entities.get(referenceType)

  /**
   * A cached resolver that works with the entities registered so far.
   */
  private var _resolver: ReferenceResolver = null

  /**
   * Returns a ReferenceResolver that uses the entities in this registry
   */
  def resolver: ReferenceResolver = {
    if (_resolver == null) {
      synchronized {
        _resolver = new RegistryReferenceResolver(this)
      }
    }
    _resolver
  }

}

/**
 * Default entity registry for the system
 */
object EntityRegistry extends EntityRegistry {}

/**
 * Provides a way to get access to arbitrary objects in the system by using an URI.
 *
 * The methods in this class are thread safe.
 *
 * Note that this class is generally used through the companion object.
 */
class RegistryReferenceResolver(registry: EntityRegistry) extends ReferenceResolver {

  var resolvers: Map[String, Long => UriReference] = registry.entities.flatMap { entity =>
    val resolver: Long => UriReference = entity.getReference
    (Seq(entity.name) ++ entity.alternatives).flatMap { name =>
      Seq(name.name -> resolver,
        name.plural -> resolver)
    }
  }.toMap[String, Long => UriReference]

  val regex = resolvers.keys match {
    case keys if keys.isEmpty => "<invalid>".r
    case keys => keys.map(k => s"""($k/[0-9]+)""").mkString(""".+/(""", "|", ")").r
  }

  /**
   * Checks to see if this string might be a valid reference, without actually trying to resolve it.
   */
  def isReferenceUriFormat(s: String) = regex.findFirstIn(s).isDefined

  /**
   * Returns a reference for the given URI if possible.
   *
   * @throws IllegalArgumentException if no suitable resolver can be found for the entity type in the URI.
   *                                  Note this exception will be in the Try, not actually thrown immediately.
   */
  def resolve(uri: String): Try[UriReference] = Try {
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
    val id = Try {
      parts(1).toLong
    }.getOrElse(
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
object ReferenceResolver extends ReferenceResolver {
  /**
   * Returns a reference for the given URI if possible.
   *
   * @throws IllegalArgumentException if no suitable resolver can be found for the entity type in the URI.
   *         Note this exception will be in the Try, not actually thrown immediately.
   */
  override def resolve(uri: String): Try[UriReference] = EntityRegistry.resolver.resolve(uri)

  /**
   * Checks to see if this string might be a valid reference, without actually trying to resolve it.
   */
  override def isReferenceUriFormat(s: String): Boolean = EntityRegistry.resolver.isReferenceUriFormat(s)
}

case class AugmentedResolver(base: ReferenceResolver, data: Seq[UriReference with HasData]) extends ReferenceResolver {
  /**
   * Returns a reference for the given URI if possible.
   *
   * @throws IllegalArgumentException if no suitable resolver can be found for the entity type in the URI.
   *         Note this exception will be in the Try, not actually thrown immediately.
   */
  override def resolve(uri: String): Try[UriReference] = {
    base.resolve(uri).map { ref =>
      data.find(d => d.uri == ref.uri).getOrElse(ref)
    }
  }

  /**
   * Checks to see if this string might be a valid reference, without actually trying to resolve it.
   */
  override def isReferenceUriFormat(s: String): Boolean = base.isReferenceUriFormat(s)

  def ++(moreData: Seq[UriReference with HasData]) = this.copy(data = this.data ++ moreData)
}