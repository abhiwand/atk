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

package com.intel.intelanalytics.engine.plugin

/**
 * Describes the results returned from an execution of a Query. The actual results are stored as an RDD for retrieval
 * at a later time out of band.
 *
 * @param totalPages Total Number of pages of the retrieved data
 * @param pageSize Number of records per page
 */
case class QueryPluginResults(totalPages: Int, pageSize: Long)
