package com.intel.intelanalytics.engine.spark.frame.plugins.groupby.aggregators

import org.scalatest.Matchers
import org.scalatest.matchers.{ MatchResult, Matcher }

object MatcherUtils extends Matchers {

  /**
   * Tests if two mean counters are equal +- tolerance.
   */
  def equalWithTolerance(right: MeanCounter, tolerance: Double) = Matcher { (left: MeanCounter) =>
    MatchResult(
      left.count == right.count && left.sum === (right.sum +- tolerance),
      left + " did not equal " + right + " with tolerance " + tolerance,
      left + " equaled " + right + " with tolerance " + tolerance
    )
  }

  /**
   * Tests if two mean counters are equal +- tolerance.
   */
  def equalWithTolerance(right: VarianceCounter, tolerance: Double) = Matcher { (left: VarianceCounter) =>
    MatchResult(
      left.count == right.count && left.mean === (right.mean +- tolerance) && left.m2 === (right.m2 +- tolerance),
      left + " did not equal " + right + " with tolerance " + tolerance,
      left + " equaled " + right + " with tolerance " + tolerance
    )
  }
}