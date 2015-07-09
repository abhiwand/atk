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

package com.intel.taproot.analytics.rest.v1.viewmodels

/**
 * The REST service response for a single command in the list provided by "GET ../commands".
 *
 * @param id unique id auto-generated by the database
 * @param name the name of the command to be performed. In the case of a builtin command, this name is used to
 *             find the stored implementation. For a user command, this name is purely for descriptive purposes.
 * @param url the URI for 'this' command in terms of the REST API
 */
case class GetCommands(id: Long, name: String, url: String) {
  require(id > 0, "id must be greater than zero")
  require(name != null, "name must not be null")
  require(url != null, "url must not be null")
}