package com.intel.graphbuilder.testutils

// TODO: not sure if this is needed, still working out TestingSparkContext

trait TestingSparkContextWithTitan extends TestingSparkContext with TestingTitan {

  override def after: Unit = {
    super.cleanupSpark()
    super.cleanupTitan()
  }

}
