/* Copyright (C) 2014 Intel Corporation.
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
package com.intel.hadoop.graphbuilder.test;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Collection;

/**
 * Imperfect test for checking for expected Intel Header.
 * 
 * This will do a "pretty good" job by checking for the expected phrases.
 * 
 * It could be better obviously but this was quick and easy.
 */
public class IntelHeaderTest {

	/**
	 * All of the parts of the expected header
	 */
	private static final String[] EXPECTED_GRAPHBUILDER_HEADER = {
			"Copyright (C) 2013 Intel Corporation.",
			"All rights reserved.",
			"Licensed under the Apache License, Version 2.0 (the \"License\");",
			"you may not use this file except in compliance with the License.",
			"You may obtain a copy of the License at",
			"http://www.apache.org/licenses/LICENSE-2.0",
			"Unless required by applicable law or agreed to in writing, software",
			"distributed under the License is distributed on an \"AS IS\" BASIS,",
			"WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.",
			"See the License for the specific language governing permissions and",
			"limitations under the License.",
			"For more about this software visit:",
			"http://www.01.org/GraphBuilder" };

	@Test
	public void testForExpectedIntelCopyrightHeader() throws Throwable {
		StringBuilder errorMessage = new StringBuilder();
		for (File graphBuilderJavaFile : getGraphBuilerJavaFiles()) {
			String java = FileUtils.readFileToString(graphBuilderJavaFile);
			for (String part : EXPECTED_GRAPHBUILDER_HEADER) {
				if (!java.contains(part)) {
					errorMessage.append(graphBuilderJavaFile.getAbsolutePath()
							+ " was missing " + part + "\n");
					break;
				}
			}
		}
		if (errorMessage.length() > 0) {
			Assert.fail("Java file was missing standard header:\n"
					+ errorMessage);
		}
	}

	/**
	 * All Java files in the project
	 */
	private Collection<File> getGraphBuilerJavaFiles() {
		File baseDir = new File(System.getProperty("user.dir"));
		return FileUtils.listFiles(baseDir, new String[] { "java" }, true);
	}

}
