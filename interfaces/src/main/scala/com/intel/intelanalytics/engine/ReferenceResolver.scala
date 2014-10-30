package com.intel.intelanalytics.engine

import com.intel.intelanalytics.domain.UriReference
import com.intel.intelanalytics.engine.plugin.Invocation

import scala.util.Try
import scala.reflect.runtime.{ universe => ru }
import ru._

/**
 * The default system registry of URI reference resolvers
 */
object ReferenceResolver extends ReferenceResolver {

  private[intelanalytics] def coerceReference[T <: UriReference: TypeTag](ref: UriReference): T = {
    Try {
      ref.asInstanceOf[T]
    }.getOrElse(throw new IllegalArgumentException(s"Could not convert $ref to an instance of $typeTag[T]"))
  }

  /**
   * Returns a reference for the given URI if possible.
   *
   * @throws IllegalArgumentException if no suitable resolver can be found for the entity type in the URI.
   *                                  Note this exception will be in the Try, not actually thrown immediately.
   */
  override def resolve[T <: UriReference: TypeTag](uri: String)(implicit invocation: Invocation,
                                                                e: T DefaultsTo UriReference): Try[T] =
    EntityRegistry.resolver.resolve(uri)

  /**
   * Checks to see if this string might be a valid reference, without actually trying to resolve it.
   */
  override def isReferenceUriFormat(s: String): Boolean = EntityRegistry.resolver.isReferenceUriFormat(s)

  /**
   * Creates an (empty) instance of the given type, reserving a URI
   */
  override def create[T <: UriReference: ru.TypeTag]()(implicit invocation: Invocation): T = EntityRegistry.resolver.create()

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
  def resolve[T <: UriReference: TypeTag](uri: String)(implicit invocation: Invocation,
                                                       e: T DefaultsTo UriReference): Try[T]

  /**
   * Returns a (possibly updated) reference.
   */
  //TODO: Make the return type a subtype of the argument type
  def resolve[T <: UriReference: TypeTag](reference: UriReference)(implicit invocation: Invocation): Try[T] = {
    resolve[T](reference.uri)
  }

  /**
   * Creates an (empty) instance of the given type, reserving a URI
   */
  def create[T <: UriReference: TypeTag]()(implicit invocation: Invocation): T

}