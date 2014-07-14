//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

package com.intel.intelanalytics.component

trait ClassLoaderAware {

  /**
   * Execute a code block using specified class loader
   * rather than the ClassLoader of the currentThread()
   */
  def withLoader[T](loader: ClassLoader)(expr: => T): T = {
    val prior = Thread.currentThread().getContextClassLoader
    try {
      Thread.currentThread().setContextClassLoader(loader)
      expr
    }
    finally {
      Thread.currentThread().setContextClassLoader(prior)
    }
  }

  /**
   * Execute a code block using the ClassLoader of 'this'
   * rather than the ClassLoader of the currentThread()
   */
  def withMyClassLoader[T](expression: => T): T = {
    val prior = Thread.currentThread().getContextClassLoader
    try {
      val loader = this.getClass.getClassLoader
      Thread.currentThread setContextClassLoader loader
      expression
    }
    finally {
      Thread.currentThread setContextClassLoader prior
    }
  }
}
