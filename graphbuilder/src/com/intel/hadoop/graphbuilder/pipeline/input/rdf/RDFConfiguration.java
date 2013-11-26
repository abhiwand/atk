/* Copyright (C) 2013 Intel Corporation.
*     All rights reserved.
*
 *  Licensed under the Apache License, Version 2.0 (the "License");
*  you may not use this file except in compliance with the License.
*  You may obtain a copy of the License at
*
*       http://www.apache.org/licenses/LICENSE-2.0
*
*   Unless required by applicable law or agreed to in writing, software
*   distributed under the License is distributed on an "AS IS" BASIS,
*   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*   See the License for the specific language governing permissions and
*   limitations under the License.
*
* For more about this software visit:
*      http://www.01.org/GraphBuilder
 */

package com.intel.hadoop.graphbuilder.pipeline.input.rdf;

import com.intel.hadoop.graphbuilder.util.RuntimeConfig;

/**
 * Class holding all static strings
 */
public class RDFConfiguration {

    public static final String CMD_RDF_NAMESPACE = "rdfNamespace";
    public static RuntimeConfig config = RuntimeConfig.getInstance(RDFConfiguration.class);


}
