package com.intel.spark.graphon.iatpregel

/**
 * Implementations of this trait can provide a publicly visible Double value called "delta". It is intended to be
 * used for when cross-iteration change (delta values) are stored with each vertex during an IATPregel computation.
 */
trait DeltaProvider {
  val delta: Double
}
