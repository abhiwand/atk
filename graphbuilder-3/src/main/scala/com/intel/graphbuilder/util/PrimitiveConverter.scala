package com.intel.graphbuilder.util

/**
 * Titan doesn't support primitive properties so we convert them to their Object equivalents
 *
 * e.g. scala.Int to java.lang.Integer
 *      scala.Long to java.lang.Long
 *      scala.Char to java.lang.Char
 *      etc.
 */
object PrimitiveConverter {

  // Stable values are needed for pattern matching primitive classes
  // See http://stackoverflow.com/questions/7157143/how-can-i-match-classes-in-a-scala-match-statement
  private val int = classOf[Int]
  private val long = classOf[Long]
  private val float = classOf[Float]
  private val double = classOf[Double]
  private val byte = classOf[Byte]
  private val short = classOf[Short]
  private val boolean = classOf[Boolean]
  private val char = classOf[Char]

  /**
   * Titan doesn't support primitive properties so we convert them to their Object equivalents
   * e.g. Int to java.lang.Integer.
   *
   * @param dataType convert primitives to Objects, e.g. Int to java.lang.Integer.
   * @return the dataType unchanged, unless it was a primitive
   */
  def primitivesToObjects(dataType: Class[_]): Class[_] = dataType match {
    case `int` => classOf[java.lang.Integer]
    case `long` => classOf[java.lang.Long]
    case `float` => classOf[java.lang.Float]
    case `double` => classOf[java.lang.Double]
    case `byte` => classOf[java.lang.Byte]
    case `short` => classOf[java.lang.Short]
    case `boolean` => classOf[java.lang.Boolean]
    case `char` => classOf[java.lang.Character]
    case default => default
  }

}
