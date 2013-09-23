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

import static com.intel.giraph.io.titan.conf.GiraphTitanConstants.EDGE_LABEL_LIST;
import static com.intel.giraph.io.titan.conf.GiraphTitanConstants.EDGE_PROPERTY_KEY_LIST;
import static com.intel.giraph.io.titan.conf.GiraphTitanConstants.VERTEX_PROPERTY_KEY_LIST;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;

import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler;
import com.thinkaurelius.titan.graphdb.types.system.SystemKey;
import com.thinkaurelius.titan.graphdb.types.system.SystemType;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.google.common.collect.Lists;
import org.apache.log4j.Logger;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


@SuppressWarnings("unchecked")
public class GiraphVertexLoaderLongDoubleFloat {

	static final long UPPER_BOUND = 1000000000L;
	private boolean isSystemType = false;
	private long vertexId = 0;
	/** The configuration */
	private Vertex<LongWritable, DoubleWritable, FloatWritable> vertex = null;
	private String[] vertexPropertyKeyList;
	private String[] edgePropertyKeyList;
	private String[] edgeLabelList;
	private static Set<String> vertexPropertyKeyValues = null;
	private static Set<String> edgePropertyKeyValues = null;
	private static Set<String> edgeLabelValues = null;

	/** Class logger. */
	private static final Logger LOG = Logger
			.getLogger(GiraphVertexLoaderLongDoubleFloat.class);

	public GiraphVertexLoaderLongDoubleFloat(
			final ImmutableClassesGiraphConfiguration conf,
			final StaticBuffer key) {
		this(conf, IDHandler.getKeyID(key));
	}

	public GiraphVertexLoaderLongDoubleFloat(
			final ImmutableClassesGiraphConfiguration conf, final long id) {
		if (id < UPPER_BOUND && id > 0) {
			vertex = conf.createVertex();
			vertex.initialize(new LongWritable(id), new DoubleWritable(0));
			vertexId = id;
			vertexPropertyKeyList = VERTEX_PROPERTY_KEY_LIST.get(conf).split(
					",");
			edgePropertyKeyList = EDGE_PROPERTY_KEY_LIST.get(conf).split(",");
			edgeLabelList = EDGE_LABEL_LIST.get(conf).split(",");
			vertexPropertyKeyValues = new HashSet<String>(
					Arrays.asList(vertexPropertyKeyList));
			edgePropertyKeyValues = new HashSet<String>(
					Arrays.asList(edgePropertyKeyList));
			edgeLabelValues = new HashSet<String>(Arrays.asList(edgeLabelList));
		}
	}

	public Vertex getVertex() {
		return this.isSystemType ? null : this.vertex;
	}

	public RelationFactory getFactory() {
		return new RelationFactory();
	}

	public class RelationFactory implements
			com.thinkaurelius.titan.graphdb.database.RelationFactory {

		private final Map<String, Object> properties = new HashMap<String, Object>();

		private Direction direction;
		private TitanType type;
		private long relationID;
		private long otherVertexID;
		private Object value;

		@Override
		public long getVertexID() {
			return vertexId;
		}

		@Override
		public void setDirection(final Direction direction) {
			this.direction = direction;
		}

		@Override
		public void setType(final TitanType type) {
			if (type == SystemKey.TypeClass) {
				isSystemType = true;
			}
			this.type = type;
		}

		@Override
		public void setRelationID(final long relationID) {
			this.relationID = relationID;
		}

		@Override
		public void setOtherVertexID(final long vertexId) {
			this.otherVertexID = vertexId;
		}

		@Override
		public void setValue(final Object value) {
			this.value = value;
		}

		@Override
		// edge property
		public void addProperty(final TitanType type, final Object value) {
			properties.put(type.getName(), value);
		}

		public void build() {
			if (this.type instanceof SystemType)
				return;
			if (vertexId <= 0 || vertexId >= UPPER_BOUND)
				return;
			if (this.type.isPropertyKey()) {
				Preconditions.checkNotNull(value);
				// filter vertex property key name
				if (vertexPropertyKeyValues.contains(this.type.getName())) {
					final Object vertexValueObject = this.value;
					final double vertexValue = ((Number) vertexValueObject)
							.doubleValue();
					vertex.setValue(new DoubleWritable(vertexValue));
				}
			} else {
				Preconditions.checkArgument(this.type.isEdgeLabel());
				// filter Edge Label
				if (edgeLabelValues.contains(this.type.getName())) {
					if (this.direction.equals(Direction.OUT)) {
						float edgeValue = 0;
						for (final Map.Entry<String, Object> entry : this.properties
								.entrySet()) {
							if (entry.getValue() != null
									&& edgePropertyKeyValues.contains(entry
											.getKey())) {
								final Object edgeValueObject = entry.getValue();
								edgeValue = ((Number) edgeValueObject)
										.floatValue();
							}
						}
						Edge<LongWritable, FloatWritable> edge = EdgeFactory
								.create(new LongWritable(this.otherVertexID),
										new FloatWritable(edgeValue));
						vertex.addEdge(edge);

					} else if (this.direction.equals(Direction.BOTH))
						throw ExceptionFactory.bothIsNotSupported();
				}
			}
		}
	}
}
