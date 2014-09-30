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

package com.intel.intelanalytics.engine.spark.user

import com.intel.intelanalytics.component.ClassLoaderAware
import com.intel.intelanalytics.repository.SlickMetaStoreComponent
import com.intel.intelanalytics.security.UserPrincipal
import org.apache.commons.lang.StringUtils
import com.intel.intelanalytics.domain.User
import com.intel.event.EventLogging

/**
 * Get users from metaStore
 *
 * @param metaStore the database
 */
class UserStorage(val metaStore: SlickMetaStoreComponent#SlickMetaStore) extends EventLogging with ClassLoaderAware {

  def getUserPrincipal(apiKey: String): UserPrincipal = {
    metaStore.withSession("Getting user principal") {
      implicit session =>
        {
          if (StringUtils.isBlank(apiKey)) {
            throw new SecurityException("Api key was not provided")
          }
          val users: List[User] = metaStore.userRepo.retrieveByColumnValue("api_key", apiKey)
          users match {
            case Nil => throw new SecurityException("User not found with apiKey:" + apiKey)
            case us if us.length > 1 => throw new SecurityException("Problem accessing user credentials")
            case user => {
              val userPrincipal: UserPrincipal = new UserPrincipal(users(0), List("user")) //TODO need role definitions
              userPrincipal
            }
          }
        }
    }
  }

}
