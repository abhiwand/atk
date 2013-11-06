REGISTER lib/datafu-0.0.10.jar;
DEFINE VAR datafu.pig.stats.VAR();

--register user's parser
REGISTER $REGISTER_STATEMENT;

--delete output
rmf $TRANSFORMED_OUTPUT

-- set the number of reducers
SET default_parallel $DEGREE_OF_PARALLELISM; 

logs = LOAD '$INPUT_FILE' using PigStorage('$INPUT_DELIM') AS (line: chararray);

parsed = FOREACH logs GENERATE $CUSTOM_PARSER as $SCHEMA_DEF;

--need the below step to extract the individual fields of the tuples
parsed_val = FOREACH parsed GENERATE $FIELD_EXTRACTORS;

grp = GROUP parsed_val ALL;
avg_var_relation = FOREACH grp GENERATE AVG(grp.parsed_val.$NORMALIZATION_FIELD) as average, VAR(grp.parsed_val.$NORMALIZATION_FIELD) as variance;
stddev_relation = FOREACH avg_var_relation GENERATE SQRT(variance) as stddev;

standardized_dataset = FOREACH parsed GENERATE $NORMALIZED_FIELDS;

STORE standardized_dataset INTO '$TRANSFORMED_OUTPUT' USING PigStorage('$OUTPUT_DELIM');
--TODO: need to also store mean, stddev to use while testing the models as the test set should also be standardized with these same values