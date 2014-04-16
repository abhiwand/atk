package com.intel.graphbuilder.testutils

import org.specs2.mutable.After

/**
 * Extends After adding a concrete after() method.
 *
 * This allows subclasses to call super so you can mix in multiple After traits.
 */
trait MultipleAfter extends After {

  override def after: Any = {}
}
