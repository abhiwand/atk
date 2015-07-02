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

package com.intel.taproot.giraph.combiner;

import org.apache.giraph.combiner.MessageCombiner;
import org.apache.hadoop.io.LongWritable;

/**
 * Combiner which finds the minimum of {@link LongWritable}.
 */
public class MinimumLongCombiner extends MessageCombiner<LongWritable, LongWritable> {
    @Override
    public void combine(LongWritable vertexIndex, LongWritable originalMessage,
        LongWritable messageToCombine) {
        if (originalMessage.get() > messageToCombine.get()) {
            originalMessage.set(messageToCombine.get());
        }
    }

    @Override
    public LongWritable createInitialMessage() {
        return new LongWritable(Long.MAX_VALUE);
    }
}
