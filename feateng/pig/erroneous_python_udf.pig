parsed_val = LOAD 'hbase://shaw_table' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('etl-cf:timestamp etl-cf:item_id etl-cf:method etl-cf:src_tms etl-cf:event_type etl-cf:dst_tms etl-cf:duration', '-loadKey true') 
	as (key:chararray, timestamp:chararray, item_id:chararray, method:chararray, src_tms:chararray, event_type:chararray, dst_tms:chararray, duration:chararray);

REGISTER 'tests/erroneous_udfs.py' USING jython as erroneous_udfs	
squared_duration = FOREACH parsed_val GENERATE erroneous_udfs.square(duration);
illustrate squared_duration;
