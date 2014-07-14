package com.intel.testutils

import org.scalatest.Matchers
import org.scalatest.matchers.{ MatchResult, Matcher }

object MatcherUtils extends Matchers {
  def equalWithTolerance(right: Array[Double], tol: Double) =
    Matcher { (left: Array[Double]) =>
      MatchResult(
        (left zip right) forall { case (a, b) => a === (b +- tol) },
        left + " did not equal " + right + " with tolerance " + tol,
        left + " equaled " + right + " with tolerance " + tol
      )
    }
}
