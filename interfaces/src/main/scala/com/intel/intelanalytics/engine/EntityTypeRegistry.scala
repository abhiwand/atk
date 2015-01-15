package com.intel.intelanalytics.engine

import com.intel.intelanalytics.{ NotNothing, domain }
import com.intel.intelanalytics.domain.{ HasData, UriReference, EntityType, EntityManager }
import com.intel.intelanalytics.engine.plugin.Invocation

import scala.reflect.runtime.{ universe => ru }
import ru._
/**
 * Default entity registry for the system
 */
object EntityTypeRegistry extends EntityTypeRegistry {}

/**
 * Storage for entities that can process different kinds of references
 */
class EntityTypeRegistry {

  private var entityTypes: List[(Type, EntityManager[_])] = List.empty
  private var _entities: Map[EntityType, EntityManager[_]] = Map.empty

  def entities: Set[(EntityType, EntityManager[_])] = _entities.toSet

  /**
   * Registers an URI resolver that can provide objects of a certain type
   * @param entity the name of the entity type, e.g. "graph"
   */
  def register[E <: EntityType: TypeTag](entity: E, entityManagement: EntityManager[E]): Unit = {
    synchronized {
      entityTypes = (entityManagement.referenceTag.tpe, entityManagement) :: entityTypes
      _entities += (entity -> entityManagement)
      _resolver = null //resolver becomes invalid when new entities added, will generate a new one
      //next time someone asks for one.
    }
  }

  /**
   * Retrieves a registered entity that works with the given reference type
   * @tparam R the reference type
   * @return an Entity that can work with that reference type
   */
  def entityManager[R <: UriReference: TypeTag]()(implicit ev: NotNothing[R]): Option[EntityManager[_]] =
    entityManagerForType(typeOf[R])

  /**
   * Retrieves a registered entity that works with the given reference type
   * @return an Entity that can work with that reference type
   */
  def entityManager[E <: EntityType](entity: E): Option[EntityManager[E]] =
    _entities.get(entity).map(_.asInstanceOf[EntityManager[E]])

  /**
   * Retrieves a registered entity that works with the given reference type
   * @param requestedType the type of the reference
   * @return an Entity that can work with that reference type
   */
  def entityManagerForType(requestedType: Type): Option[EntityManager[_]] = {
    require(!(requestedType =:= typeOf[Nothing]), "No entity manager handles the Nothing type")
    require(!(requestedType =:= typeOf[UriReference]), "No entity manager handles the raw UriReference type, " +
      "please specify a subclass appropriate for the entity you want to use")
    val matches = entityTypes.find {
      case (t, e) =>
        val res = requestedType <:< t
        res
    }.map(_._2)
    matches.headOption
  }

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

  /**
   * Create an empty / uninitialized instance of the requested type if possible.
   *
   * @tparam R the requested reference type
   */
  def create[R <: UriReference: TypeTag](annotation: Option[String] = None)(implicit invocation: Invocation): R = {
    val manager: EntityManager[_] = entityManager[R]().get
    val reference = manager.create()
    resolver.resolve[R](reference).get
  }

  /**
   * Creates an (empty) instance of the given type, reserving a URI
   */
  def delete[R <: UriReference: TypeTag](reference: R)(implicit invocation: Invocation, ev: NotNothing[R]): Unit = {
    val manager: EntityManager[_] = entityManager[R]().get
    manager.delete(reference.asInstanceOf[manager.type#Reference])
  }

  /**
   * Save data of the given type, possibly creating a new object.
   */
  def saveData[T <: UriReference with HasData: TypeTag](data: T)(implicit invocation: Invocation): T = {
    val manager = entityManager[T].get
    manager.saveData(data.asInstanceOf[manager.type#Data]).asInstanceOf[T]
  }

}