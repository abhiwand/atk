/**
 * Copyright (C) 2013 Intel Corporation.
 *     All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * For more about this software visit:
 *     http://www.01.org/GraphBuilder
 */
package com.intel.hadoop.graphbuilder.pipeline.tokenizer.hbase;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;


public class HBaseGraphBuildingRuleTest {

    @Test
    public void testGetVidColNameFromVertexRule() throws Exception {
        assertEquals("etl-cf:user", HBaseGraphBuildingRule.getVidColNameFromVertexRule("etl-cf:user"));
        assertEquals("etl-cf:user", HBaseGraphBuildingRule.getVidColNameFromVertexRule("etl-cf:user,"));
        assertEquals("etl-cf:user", HBaseGraphBuildingRule.getVidColNameFromVertexRule("etl-cf:user=etl-cf:vertex_type"));
        assertEquals("manager", HBaseGraphBuildingRule.getVidColNameFromVertexRule("OWL.People,manager"));
        assertEquals("manager", HBaseGraphBuildingRule.getVidColNameFromVertexRule("OWL.People,manager=more,stuff,is,okay"));
    }

    @Test
    public void testGetVidColNameFromVertexRule_Error() {
        try {
            HBaseGraphBuildingRule.getVidColNameFromVertexRule("OWL.People,manager,extra-junk");
        }
        catch( IllegalArgumentException e ) {
            assertEquals("A vertex rule should NOT have more than one comma in the VID column name: OWL.People,manager,extra-junk", e.getMessage());
        }
    }

    @Test
    public void testGetRDFTagFromVertexRule() {
        assertEquals("[RDF Object]", HBaseGraphBuildingRule.getRDFTagFromVertexRule("[RDF Object],vertex_col1=vertex_prop1"));
        assertNull(HBaseGraphBuildingRule.getRDFTagFromVertexRule("vertex_col1=vertex_prop1"));
    }
}
