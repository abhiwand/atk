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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.io.DataInput;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
public class FloatTypeTest {
	private static final float EPSILON = 0.0001f;

	@Test
	public void testFloatType() throws Exception {
		FloatType f = new FloatType();
		assertEquals(0f, f.get(), EPSILON);
		float val = 5.0f;
		f = new FloatType(val);
		assertEquals(val, f.get(), EPSILON);
		f.set(val * -1);
		assertEquals(val * -1, f.get(), EPSILON);

		val = 123.0f;
		f = new FloatType(val);
		assertEquals("toString mismatch", String.valueOf(val), f.toString());
		assertEquals("toJSONString mismatch", String.valueOf(val),
				f.toJSONString());

		f.add(new FloatType(1.0f));
		assertEquals(124.0f, f.get(), EPSILON);
		assertEquals("compareTo mismatch", f.compareTo(new FloatType(124.0f)), 0);

		val = 500.0f;
		DataInput dInput = mock(DataInput.class);
		when(dInput.readFloat()).thenReturn(val);
		f.readFields(dInput);
		assertEquals(val, f.get(), EPSILON);
	}
}
