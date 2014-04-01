
from string import Template
from intel_analytics.table.pig import pig_helpers


class FlattenScriptBuilder(object):
    """
    Build a Pig script for flattening a column with a list of values into multiple rows

    For example,

    Input:
        1 a,b,c
        2 b
        3 c

    "Flattened" Output:
        1 a
        1 b
        1 c
        2 b
        3 c
    """
    def build_script(self, input_table, output_table, etl_schema, column_name, string_split_options):
        """
        Build a Pig script for flattening a column into multiple rows as a string with the supplied parameters
        """
        column_index = etl_schema.feature_names.index(column_name)
        pig_schema_info = pig_helpers.get_pig_schema_string(etl_schema.get_feature_names_as_CSV(),
                                                            etl_schema.get_feature_types_as_CSV())
        hbase_constructor_args = pig_helpers.get_hbase_storage_schema_string(etl_schema.get_feature_names_as_CSV())
        feature_names = etl_schema.get_feature_names_as_CSV();

        return Template(self._get_script()).substitute({
            'column_index': column_index,
            'delimiter': string_split_options.delimiter,
            'trim_start': self._none_to_empty(string_split_options.trim_start),
            'trim_end': self._none_to_empty(string_split_options.trim_end),
            'trim_whitespace': string_split_options.trim_whitespace,
            'pig_schema_info': pig_schema_info,
            'feature_names': feature_names,
            'input_table': input_table,
            'output_table': output_table,
            'hbase_constructor_args': hbase_constructor_args
        })

    def _none_to_empty(self,string):
        """
        Helper that converts None to an empty string
        """
        if string is None:
            return ''
        return string;

    def _get_script(self):
        """
        Get the Pig script as a string.
        """
        return """
DEFINE FlattenColumnToMultipleRows com.intel.pig.udf.flatten.FlattenColumnToMultipleRowsUDF('${column_index}', '${delimiter}', '${trim_start}', '${trim_end}', '${trim_whitespace}');
inpt = LOAD 'hbase://${input_table}' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('${hbase_constructor_args}') AS (${pig_schema_info});
flattened = FOREACH inpt GENERATE FLATTEN(FlattenColumnToMultipleRows(*)) AS (${pig_schema_info});
ranked = RANK flattened;
reslt = FOREACH ranked GENERATE * AS ( key, ${feature_names} );
STORE reslt INTO 'hbase://${output_table}' using org.apache.pig.backend.hadoop.hbase.HBaseStorage('${hbase_constructor_args}');
"""

