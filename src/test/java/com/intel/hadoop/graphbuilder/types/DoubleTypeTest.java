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
package com.intel.hadoop.graphbuilder.types;

import org.junit.Test;

import java.io.DataInput;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

public class DoubleTypeTest {
	private static final double EPSILON = 0.0001d;

	@Test
	public void testDoubleType() throws Exception {
		DoubleType d = new DoubleType();
		assertEquals("Should have been 0", 0, d.get(), EPSILON);
		double val = 5.0d;
		d = new DoubleType(val);
		assertEquals(val, d.get(), EPSILON);
		d.set(val * -1);
		assertEquals(val * -1, d.get(), EPSILON);

		val = 123.0d;
		d = new DoubleType(val);
		assertEquals("toString mismatch", String.valueOf(val), d.toString());
		assertEquals("toJSONString mismatch", String.valueOf(val),
				d.toJSONString());

		d.add(new DoubleType(1.0d));
		assertEquals(124.0d, d.get(), EPSILON);
		assertEquals("compareTo mismatch", d.compareTo(new DoubleType(124.0d)),0);

		val = 500.0d;
		DataInput dInput = mock(DataInput.class);
		when(dInput.readDouble()).thenReturn(val);
		d.readFields(dInput);
		assertEquals(val, d.get(), EPSILON);
	}
}
