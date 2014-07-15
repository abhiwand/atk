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
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ElementHelper;

/**
 * Creates the Titan Graph of the Gods graph in Titan
 */
public class GraphOfGods {

    public static void main(String[] args) {

        SerializableBaseConfiguration titanConfig = new SerializableBaseConfiguration();
        titanConfig.setProperty("storage.backend", "hbase");
        titanConfig.setProperty("storage.tablename", "graphofgods");
        titanConfig.setProperty("storage.hostname", "fairlane");
        titanConfig.setProperty("storage.batch-loading", "true");
        titanConfig.setProperty("autotype", "none");
        titanConfig.setProperty("storage.buffer-size", "2048");
        titanConfig.setProperty("storage.attempt-wait", "300");
        titanConfig.setProperty("storage.lock-wait-time", "400");
        titanConfig.setProperty("storage.lock-retries", "15");
        titanConfig.setProperty("storage.idauthority-retries", "30");
        titanConfig.setProperty("storage.write-attempts", "10");
        titanConfig.setProperty("storage.read-attempts", "6");
        titanConfig.setProperty("ids.block-size", "300000");
        titanConfig.setProperty("ids.renew-timeout", "150000");

        TitanGraph graph = TitanFactory.open(titanConfig);

        graph.makeKey("name").dataType(String.class).indexed(Vertex.class).unique().make();
        graph.makeKey("age").dataType(Integer.class).indexed(Vertex.class).make();
        graph.makeKey("type").dataType(String.class).make();

        final TitanKey time = graph.makeKey("time").dataType(Integer.class).make();
        final TitanKey reason = graph.makeKey("reason").dataType(String.class).indexed(Edge.class).make();
        graph.makeKey("place").dataType(Geoshape.class).indexed(Edge.class).make();

        graph.makeLabel("father").manyToOne().make();
        graph.makeLabel("mother").manyToOne().make();
        graph.makeLabel("battled").sortKey(time).make();
        graph.makeLabel("lives").signature(reason).make();
        graph.makeLabel("pet").make();
        graph.makeLabel("brother").make();

        graph.commit();

        // vertices

        Vertex saturn = graph.addVertex(null);
        saturn.setProperty("name", "saturn");
        saturn.setProperty("age", 10000);
        saturn.setProperty("type", "titan");

        Vertex sky = graph.addVertex(null);
        ElementHelper.setProperties(sky, "name", "sky", "type", "location");

        Vertex sea = graph.addVertex(null);
        ElementHelper.setProperties(sea, "name", "sea", "type", "location");

        Vertex jupiter = graph.addVertex(null);
        ElementHelper.setProperties(jupiter, "name", "jupiter", "age", 5000, "type", "god");

        Vertex neptune = graph.addVertex(null);
        ElementHelper.setProperties(neptune, "name", "neptune", "age", 4500, "type", "god");

        Vertex hercules = graph.addVertex(null);
        ElementHelper.setProperties(hercules, "name", "hercules", "age", 30, "type", "demigod");

        Vertex alcmene = graph.addVertex(null);
        ElementHelper.setProperties(alcmene, "name", "alcmene", "age", 45, "type", "human");

        Vertex pluto = graph.addVertex(null);
        ElementHelper.setProperties(pluto, "name", "pluto", "age", 4000, "type", "god");

        Vertex nemean = graph.addVertex(null);
        ElementHelper.setProperties(nemean, "name", "nemean", "type", "monster");

        Vertex hydra = graph.addVertex(null);
        ElementHelper.setProperties(hydra, "name", "hydra", "type", "monster");

        Vertex cerberus = graph.addVertex(null);
        ElementHelper.setProperties(cerberus, "name", "cerberus", "type", "monster");

        Vertex tartarus = graph.addVertex(null);
        ElementHelper.setProperties(tartarus, "name", "tartarus", "type", "location");

        // edges

        jupiter.addEdge("father", saturn);
        jupiter.addEdge("lives", sky).setProperty("reason", "loves fresh breezes");
        jupiter.addEdge("brother", neptune);
        jupiter.addEdge("brother", pluto);

        neptune.addEdge("lives", sea).setProperty("reason", "loves waves");
        neptune.addEdge("brother", jupiter);
        neptune.addEdge("brother", pluto);

        hercules.addEdge("father", jupiter);
        hercules.addEdge("mother", alcmene);
        ElementHelper.setProperties(hercules.addEdge("battled", nemean), "time", 1, "place", Geoshape.point(38.1f, 23.7f));
        ElementHelper.setProperties(hercules.addEdge("battled", hydra), "time", 2, "place", Geoshape.point(37.7f, 23.9f));
        ElementHelper.setProperties(hercules.addEdge("battled", cerberus), "time", 12, "place", Geoshape.point(39f, 22f));

        pluto.addEdge("brother", jupiter);
        pluto.addEdge("brother", neptune);
        pluto.addEdge("lives", tartarus).setProperty("reason", "no fear of death");
        pluto.addEdge("pet", cerberus);

        cerberus.addEdge("lives", tartarus);

        // commit the transaction to disk
        graph.commit();
    }
}
