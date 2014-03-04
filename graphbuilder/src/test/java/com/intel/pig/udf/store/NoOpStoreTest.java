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
package com.intel.pig.udf.store;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class NoOpStoreTest {

    @Test
    public void test() throws Exception {
        NoOpStore store = new NoOpStore();

        // getters should not return null
        assertNotNull(store.getOutputFormat());
        assertNotNull(store.getOutputFormat().getOutputCommitter(null));
        assertNotNull(store.getOutputFormat().getRecordWriter(null));

        // methods should not throw any exceptions
        store.prepareToWrite(null);
        store.putNext(null);
        store.setStoreLocation(null, null);
        store.getOutputFormat().checkOutputSpecs(null);
        store.getOutputFormat().getOutputCommitter(null).abortJob(null, null);
        store.getOutputFormat().getOutputCommitter(null).abortTask(null);
        store.getOutputFormat().getOutputCommitter(null).commitJob(null);
        store.getOutputFormat().getOutputCommitter(null).commitTask(null);
        store.getOutputFormat().getOutputCommitter(null).setupJob(null);
        store.getOutputFormat().getOutputCommitter(null).setupTask(null);
        store.getOutputFormat().getRecordWriter(null).write(null, null);
        store.getOutputFormat().getRecordWriter(null).close(null);

        // task commit should be true
        assertTrue(store.getOutputFormat().getOutputCommitter(null).needsTaskCommit(null));
    }
}
