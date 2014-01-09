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

public interface GBDataType {
	public static final int DOUBLE = 0;
	public static final int FLOAT = 1;
	public static final int INT = 2;
	public static final int ENCAPSULATED_OBJECT = 3;
	public static final int LONG = 4;
	public static final int STRING = 5;
	public static final int EMPTY = 6;

	public abstract int getType();

}
