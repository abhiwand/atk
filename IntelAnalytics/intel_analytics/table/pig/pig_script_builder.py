from intel_analytics.table.pig import pig_helpers
from intel_analytics.config import global_config
from intel_analytics.table.hbase.schema import ETLSchema

MAX_ROW_KEY = 'max_row_key'

#Below are the only data types currently supported by GB
#see com.intel.hadoop.graphbuilder.sampleapplications.GraphElementsToDB
#TODO: we should update GB to use pig data types not java data types
#we have so many types to deal with: python, pig, java.
gb_type_mapping = {'chararray': 'String',
                   'int': 'Integer',
                   'long': 'Long',
                   'float': 'Float',
                   'double': 'Double'}

class PigScriptBuilder(object):
    def get_append_tables_statement(self, etl_schema, target_table, source_tables):
        statements = []
        properties = etl_schema.get_table_properties(target_table)
        i = 0
        in_relations = []
        final_cols = []
        for source in source_tables:
            etl_schema.load_schema(source)
            pig_schema_info = pig_helpers.get_pig_schema_string(etl_schema.get_feature_names_as_CSV(), etl_schema.get_feature_types_as_CSV())
            loading_hbase_constructor_args = pig_helpers.get_hbase_storage_schema_string(
                etl_schema.get_feature_names_as_CSV())
            in_relations.append('relation_%s_in' %(str(i)))
            i = i + 1
            # forming the load statements which load data to a relation.
            # The relation is the latest appended to in_relations.
            statements.append("%s = LOAD 'hbase://%s' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('%s') as (%s);" %(in_relations[-1], source, loading_hbase_constructor_args, pig_schema_info))

            in_final_list = set(final_cols)
            in_current_table = set(etl_schema.feature_names)
            diff = in_current_table - in_final_list
            final_cols.extend(list(diff))
            etl_schema.feature_names = []
            etl_schema.feature_types = []

        if len(source_tables) == 1:
            statements.append('combined_relation = %s;' %(in_relations[-1]))
        elif len(source_tables) > 1:
            statements.append('combined_relation = UNION ONSCHEMA %s;' %(','.join(in_relations)))


        statements.append('temp = rank combined_relation;')
        statements.append('combined_relation_out = foreach temp generate $0 + %s as key, %s;' %(properties[MAX_ROW_KEY], ','.join(final_cols)))
        storing_hbase_constructor_args = pig_helpers.get_hbase_storage_schema_string(','.join(final_cols))
        statements.append("STORE combined_relation_out INTO '%s' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('%s');" %(target_table, storing_hbase_constructor_args))


        return "\n".join(statements)
    
    def vertex_str(self, vertex, public=False):
        """
        Gets the string for the vertex to use in the command call to graph_builder.
        vertex is of type GraphBuilderVertex
        """
        column_family = global_config['hbase_column_family']
        s = (column_family + vertex.key) if public is False else vertex.key
        if len(vertex.properties) > 0:
            s += '=' + ','.join(
                (map(lambda p: column_family + p, vertex.properties))
                if public is False else vertex.properties)
        return s
    
    
    def edge_str(self, edge, public=False):
        """
        Gets the string for the edge to use in the command call to graph_builder.
        edge is of type GraphBuilderEdge
        """
        column_family = global_config['hbase_column_family']
        s = ("{0}{1},{0}{2},{3}" if public is False else "{1},{2},{3}") \
            .format(column_family, edge.source, edge.target, edge.label)
        if len(edge.properties) > 0:
            s += ',' + ','.join((map(lambda p: column_family + p, edge.properties))
                                if public is False else edge.properties)
        return s
    
    def _build_hbase_table_load_statement(self, table_name, pig_alias):
        schema = ETLSchema()
        schema.load_schema(table_name)
        f_names = schema.get_feature_names_as_CSV()
        pig_schema = pig_helpers.get_pig_schema_string(f_names, schema.get_feature_types_as_CSV())
        hbase_load_args = pig_helpers.get_hbase_storage_schema_string(f_names)    
        return "%s = LOAD 'hbase://%s' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('%s') AS (%s) ;" % (pig_alias, table_name, hbase_load_args, pig_schema)
        
    def _get_gb_vertex_rule(self, registered_vertex_properties):
        """
        Creates the vertex rule for the additional registered_vertex_properties
        registered_vertex_properties is of type GraphBuilderVertexProperties
        """
        return self.vertex_str(registered_vertex_properties.vertex, True)
    
    def _get_gb_edge_rule(self, registered_edge_properties, directed):
        """
        Creates the edge rule for the additional registered_edge_properties
        registered_edge_properties is of type GraphBuilderEdgeProperties
        """
        return ('-d ' if directed else '-e ') + self.edge_str(registered_edge_properties.edge, True)
    
    def _populate_schema_table(self, table_names):
        schema_dict = {}
        for table_name in table_names:
            schema = ETLSchema()
            schema.load_schema(table_name)
            field_count = len(schema.feature_names)
            for i in range(field_count):
                schema_dict[schema.feature_names[i]] = schema.feature_types[i]
        return schema_dict
    
    def _build_store_graph_statement(self, pig_alias, gb_conf_file, vertex_list, edge_list, schema_dict, other_args):
        #create the property type definitions in property_typedefs set
        #create the edge labels and edge property names in edge_schemata set
        #see STORE_GRAPH macro in graphbuilder.pig
        property_typedefs = set()
        edge_schemata = set()
        for gb_vertex in vertex_list:
            for vertex_property in gb_vertex.properties:
                vertex_property_type = schema_dict[vertex_property]
                entry = vertex_property + ':' + gb_type_mapping[vertex_property_type]
                property_typedefs.add(entry)  
             
        for gb_edge in edge_list: 
            edge_def = ''
            #gb_edge.label may already exist in edge_schemata list
            edge_schemata.add(gb_edge.label)
            for edge_property in gb_edge.properties:
                #edge_property may already exist in edge_schemata list
                edge_schemata.add(edge_property)
                edge_property_type = schema_dict[edge_property]
                entry = edge_property + ':' + gb_type_mapping[edge_property_type]
                property_typedefs.add(entry)
        type_info = ','.join(property_typedefs)
        edge_schemata_str = ','.join(edge_schemata)        
        return "STORE_GRAPH(%s, '%s', '%s', '%s', '%s');" % (pig_alias, gb_conf_file, type_info, edge_schemata_str, other_args)
    
    def _build_load_titan_statement(self, directed, gb_conf_file, source_table_name, vertex_list, edge_list, other_args): 
        if other_args == None:
            other_args = ''
        edges = ' '.join(map(lambda e: '"' + self.edge_str(e) + '"', edge_list))
        vertex_rule = ' '.join(map(lambda v: '"' + self.vertex_str(v) + '"', vertex_list))        
        edge_rule = ('-d ' if directed else '-e ') + edges
        return "LOAD_TITAN('%s', '%s', '%s', '%s', '%s');" % (source_table_name, vertex_rule, edge_rule, gb_conf_file, other_args)
        
    def create_pig_bulk_load_script(self, gb_conf_file, source_frame, vertex_list, edge_list, registered_vertex_properties, registered_edge_properties, directed, overwrite, append, flatten):
        
        source_table_name = source_frame._table.table_name
        
        #build additional arguments passed to bulk loading macros
        other_args = ""
        if overwrite:
            other_args += " -O "
        if append:
            other_args += " -a "
        if flatten:
            other_args += " -F "
            
        #start generating pig statements
        statements = []
        statements.append("IMPORT '%s';" % global_config['graph_builder_pig_udf'])
        
        #no additional vertices/edges registered, use regular LOAD_TITAN
        #@Deprecated: LOAD_TITAN should be removed later, we want to move to a single bulk loading pig macro
        if registered_vertex_properties == None and registered_edge_properties == None:
            statements.append(self._build_load_titan_statement(directed, gb_conf_file, source_table_name, vertex_list, edge_list, other_args)) 
        else:      
            edges = ' '.join(map(lambda e: '"' + self.edge_str(e, True) + '"', edge_list))
            vertex_rule = ' '.join(map(lambda v: '"' + self.vertex_str(v, True) + '"', vertex_list))        
            edge_rule = ('-d ' if directed else '-e ') + edges
                        
            statements.append("SET default_parallel %s;" % global_config['pig_parallelism_factor'])
            statements.append("DEFINE CreatePropGraphElements com.intel.pig.udf.eval.CreatePropGraphElements('-v %s %s');" % (vertex_rule, edge_rule))
            statements.append(self._build_hbase_table_load_statement(source_table_name, "base_graph"))
            statements.append("g_0 = FOREACH base_graph GENERATE FLATTEN(CreatePropGraphElements(*));")
            final_union_alias = None
            if registered_vertex_properties:#we have some extra vertex properties to register
                statements.append(self._build_hbase_table_load_statement(registered_vertex_properties.source_frame._table.table_name, "graph_vp"))
                statements.append("DEFINE CreatePropGraphElements com.intel.pig.udf.eval.CreatePropGraphElements('-v %s');" % (self._get_gb_vertex_rule(registered_vertex_properties)))
                statements.append("g_vp = FOREACH graph_vp GENERATE FLATTEN(CreatePropGraphElements(*));")
                statements.append("unioned_vp = GRAPH_UNION(g_0, g_vp);")
                final_union_alias = 'unioned_vp'
            if registered_edge_properties:#we have some extra edge properties to register
                statements.append(self._build_hbase_table_load_statement(registered_edge_properties.source_frame._table.table_name, "graph_ep"))
                edge = registered_edge_properties.edge
                edge_rule = self._get_gb_edge_rule(registered_edge_properties, directed)
                statements.append("DEFINE CreatePropGraphElements com.intel.pig.udf.eval.CreatePropGraphElements('-v %s %s %s');" % (edge.source, edge.target, edge_rule))
                statements.append("g_ep = FOREACH graph_ep GENERATE FLATTEN(CreatePropGraphElements(*));")
                if registered_vertex_properties:
                    statements.append("unioned = GRAPH_UNION(unioned_vp, g_ep);")
                else:
                    statements.append("unioned = GRAPH_UNION(g_0, g_ep);")
                final_union_alias='unioned'
            
            assert final_union_alias != None
            
            #populate the name/type dictionary from etl_schema, which will be used 
            #when generating the STORE_GRAPH statement in the end
            table_names = [source_table_name]
            if registered_vertex_properties:
                table_names.append(registered_vertex_properties.source_frame._table.table_name)
            if registered_edge_properties:
                table_names.append(registered_edge_properties.source_frame._table.table_name)
            schema_dict = self._populate_schema_table(table_names)
            print "schema_dict " , schema_dict
                        
            #create the property type definitions and
            #edge labels and edge property names to be passed to STORE_GRAPH
            #don't forget to add the additional vertex/edge properties
            if registered_vertex_properties != None:
                vertex_list.append(registered_vertex_properties.vertex)
            if registered_edge_properties != None:
                edge_list.append(registered_edge_properties.edge)
            statements.append(self._build_store_graph_statement(final_union_alias, gb_conf_file, vertex_list, edge_list, schema_dict, other_args.strip()))

        return "\n".join(statements)
        