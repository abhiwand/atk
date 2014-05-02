package com.intel.intelanalytics.security

import com.intel.intelanalytics.domain.User

/**
 * Principal representing an authenticated API user
 * @param user user DB entity representing the API user
 * @param roles roles of the authenticated user
 */
case class UserPrincipal(user: User, roles: List[String]) {

}
