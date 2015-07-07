/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package com.intel.taproot.analytics

import com.intel.taproot.event.EventContext
import com.intel.taproot.analytics.engine.plugin.Invocation
import com.intel.taproot.analytics.security.UserPrincipal

/**
 * Implicits for working with invocations and event logging
 */
trait EventLoggingImplicits {

  implicit def eventContext[T <: Invocation](implicit inv: T): EventContext = {
    require(inv != null, "invocation cannot be null")
    inv.eventContext
  }

  implicit def user[T](implicit inv: Invocation): UserPrincipal = inv.user

}
