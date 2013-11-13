REGISTER ../lib/datafu-0.0.10.jar;
DEFINE VAR datafu.pig.stats.VAR();

--register user's parser
REGISTER ../custom-parsers/shaw_udfs.py USING jython AS shaw_udfs;

--delete output
rmf /tmp/shaw_processed_csv
rmf /tmp/shaw_filtered_records

-- set the number of reducers
SET default_parallel 4; 

logs = LOAD 'sample.log' using PigStorage('\n') AS (line: chararray);
--logs = LOAD '../single_sample.log' USING PigStorage('\n') AS (line: chararray);

parsed = FOREACH logs GENERATE shaw_udfs.parseRecord(*) as (timestamp: chararray, event_type: chararray, method: chararray, duration: double, item_id: chararray, src_tms: chararray, dst_tms: chararray);
-- describe parsed;
--need the below step to extract the individual fields of the tuples
parsed_val = FOREACH parsed GENERATE $0.timestamp, $0.event_type, $0.method, $0.duration, $0.item_id, $0.src_tms, $0.dst_tms;

-- illustrate parsed_val;

grp = GROUP parsed_val ALL;

average_relation = FOREACH grp GENERATE AVG(grp.parsed_val.duration) as average_duration;
var = FOREACH grp GENERATE VAR(grp.parsed_val.duration) as variance;
stddev_relation = FOREACH var GENERATE SQRT(variance) as stddev;

standardized_duration = FOREACH parsed GENERATE $0.timestamp, $0.event_type, $0.method, ($0.duration - average_relation.average_duration)/stddev_relation.stddev as normalized_duration, $0.item_id, $0.src_tms, $0.dst_tms;

--if we need counts later on:
--mean = foreach grp {
--        sum = SUM(grp.parsed_val.duration);
--        count = COUNT(grp.parsed_val.duration);
--        generate sum/count as avg, count as count;
--};

