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

package org.trustedanalytics.atk.rest.v1.viewmodels

import org.joda.time.DateTime
import org.trustedanalytics.atk.domain.schema.Schema

/**
 * The REST service response for single dataFrame in "GET ../frames/id"
 *
 * @param uri unique id auto-generated by the database
 * @param name name assigned by user
 * @param schema the schema of the frame (defines columns, etc)
 * @param rowCount the number of rows in the frames
 * @param links sundry links for the frame
 * @param errorFrameUri uri of the error frame, if it exists
 * @param entityType type of entity, in this case probably "frame"
 * @param status status of the entity, like Active or Dropped...
 * @param lastReadDate the last time data from this frame was accessed
 */

case class GetFrame(uri: String,
                    name: Option[String],
                    schema: Schema,
                    rowCount: Option[Long],
                    links: List[RelLink],
                    errorFrameUri: Option[String],
                    entityType: String,
                    status: String,
                    lastReadDate: DateTime) {
  require(uri != null, "uri must not be null")
  require(name != null, "name must not be null")
  require(schema != null, "schema must not be null")
  require(rowCount.isEmpty || rowCount.get >= 0, "rowCount must not be negative")
  require(links != null, "links must not be null")
  require(errorFrameUri != null, "errorFrameId must not be null")
  require(entityType != null, "entityType must not be null")
  require(status != null, "status may not be null")
}