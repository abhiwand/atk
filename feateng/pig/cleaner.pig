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

null_val = FILTER parsed_val BY $CLEAN_FIELD is NULL;

all_records_group = GROUP parsed_val ALL;
null_val_group = GROUP null_val ALL;
dataset_size = FOREACH all_records_group GENERATE COUNT(parsed_val) as dataset_size;
null_val_size = FOREACH null_val_group GENERATE COUNT(null_val) as null_val_size;
-- cleaned_data contains the dataset with the null values cleaned out
cleaned_data = FILTER parsed_val BY $CLEAN_FIELD is not NULL;

STORE cleaned_data  INTO '$OUTPUT_FILE' USING PigStorage('$OUTPUT_DELIM');