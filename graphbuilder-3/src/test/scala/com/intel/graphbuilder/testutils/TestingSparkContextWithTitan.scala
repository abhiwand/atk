package com.intel.graphbuilder.testutils


/**
 * For testing with both Spark and Titan at the same time.
 *
 * This handles the case where multiple cleanup() methods need to be called in after()
 */
trait TestingSparkContextWithTitan extends TestingSparkContext with TestingTitan {

  override def after: Unit = {
    super.cleanupSpark()
    super.cleanupTitan()
  }

}
