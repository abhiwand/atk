--register user's parser
REGISTER $REGISTER_STATEMENT;

--delete output
rmf $OUTPUT_FILE

-- set the number of reducers
SET default_parallel $DEGREE_OF_PARALLELISM; 

logs = LOAD '$INPUT_FILE' using PigStorage('$INPUT_DELIM') AS (line: chararray);
parsed = FOREACH logs GENERATE $CUSTOM_PARSER as $SCHEMA_DEF;

--need the below step to extract the individual fields of the tuples
parsed_val = FOREACH parsed GENERATE $FIELD_EXTRACTORS;

STORE parsed_val INTO '$OUTPUT_FILE' USING PigStorage('$OUTPUT_DELIM');