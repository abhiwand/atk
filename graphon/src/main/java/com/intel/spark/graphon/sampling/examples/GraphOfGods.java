//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

package com.intel.spark.graphon.sampling.examples;

import com.intel.graphbuilder.util.SerializableBaseConfiguration;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.example.GraphOfTheGodsFactory;

/**
 * Creates the Titan Graph of the Gods graph in Titan
 */
public class GraphOfGods {

    public static void main(String[] args) {

        SerializableBaseConfiguration titanConfig = new SerializableBaseConfiguration();
        titanConfig.setProperty("storage.backend", "hbase");
        titanConfig.setProperty("storage.hbase.table", "graphofgods");
        titanConfig.setProperty("storage.hostname", "fairlane");

        TitanGraph graph = TitanFactory.open(titanConfig);
        GraphOfTheGodsFactory.load(graph);
        graph.commit();
    }
}
