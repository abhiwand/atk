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
 */
package com.intel.giraph.io.titan;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import java.util.Iterator;
import java.util.Map;

/**
 * Converts a Giraph configuration file to a Titan configuration file. For all
 * Titan specific properties, the conversion chomps the Giraph prefix and
 * provides to Titan's graph factory.
 * 
 */
public class GiraphToTitanGraphFactory {

	/** Class logger. */
	private static final Logger LOG = Logger
			.getLogger(GiraphToTitanGraphFactory.class);

	public static BaseConfiguration generateTitanConfiguration(
			final Configuration config, final String prefix) {

		final BaseConfiguration titanconfig = new BaseConfiguration();
		final Iterator<Map.Entry<String, String>> itty = config.iterator();
		while (itty.hasNext()) {
			final Map.Entry<String, String> entry = itty.next();
			final String key = entry.getKey();
			final String value = entry.getValue();

			titanconfig.setProperty(key, value);
			if (key.startsWith(prefix)) {
				titanconfig.setProperty(key.substring(prefix.length() + 1),
						value);
			}
		}
		return titanconfig;
	}
}
