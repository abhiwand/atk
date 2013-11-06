REGISTER ../lib/datafu-0.0.10.jar;
DEFINE VAR datafu.pig.stats.VAR();

--register user's parser
REGISTER ../custom-parsers/shaw_udfs.py USING jython AS shaw_udfs;

--delete output
rmf /tmp/shaw_standardized

-- set the number of reducers
SET default_parallel 4; 

logs = LOAD 'sample.log' using PigStorage('\n') AS (line: chararray);

parsed = FOREACH logs GENERATE shaw_udfs.parseRecord(*) as (timestamp: chararray, event_type: chararray, method: chararray, duration: double, item_id: chararray, src_tms: chararray, dst_tms: chararray);

--need the below step to extract the individual fields of the tuples
parsed_val = FOREACH parsed GENERATE $0.timestamp, $0.event_type, $0.method, $0.duration, $0.item_id, $0.src_tms, $0.dst_tms;

cleaned_parsed_val = FILTER parsed_val BY duration is not NULL;

grp = GROUP cleaned_parsed_val ALL;

avg_var_relation = FOREACH grp GENERATE AVG(grp.cleaned_parsed_val.duration) as average_duration, VAR(grp.cleaned_parsed_val.duration) as variance;
stddev_relation = FOREACH avg_var_relation GENERATE SQRT(variance) as stddev;
standardized_duration = FOREACH parsed GENERATE $0.timestamp, $0.event_type, $0.method, ($0.duration - avg_var_relation.average_duration)/stddev_relation.stddev as normalized_duration, $0.item_id, $0.src_tms, $0.dst_tms;

STORE standardized_duration  INTO '/tmp/shaw_standardized' USING PigStorage(',');