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
package com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema;

import org.junit.Test;

import static junit.framework.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;

public class EdgeSchemaTest {

	@Test
	public void edgeSchemaConstructorTest() throws Exception {
		final String THE_EDGE = "The Edge";
		EdgeSchema edgeSchema = new EdgeSchema(THE_EDGE);
		assertNotNull(edgeSchema.getPropertySchemata());
		assertEquals("Should have been 0", 0,
				edgeSchema.getLabel().compareTo(THE_EDGE));
	}

	@Test
	public void edgeSchemaSetGetLabelTest() {

		final String THE_EDGE = "The Edge";
		final String BONO = "Bono";

		EdgeSchema edgeSchema = new EdgeSchema(THE_EDGE);

		assertEquals("Should have been 0", 0,
				edgeSchema.getLabel().compareTo(THE_EDGE));

		edgeSchema.setLabel(BONO);
		assertEquals("Should have been 0", 0,
				edgeSchema.getLabel().compareTo(BONO));

		edgeSchema.setLabel(THE_EDGE);
		assertEquals("Should have been 0", 0,
				edgeSchema.getLabel().compareTo(THE_EDGE));
	}

}
