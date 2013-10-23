HDFS_INPUT=/user/user/mohit/sample/fail_sample.rdf
HBASE_RAW_TABLE=test_corner_rdf
COLUMN=data:rdf
hadoop jar /home/user/mohit/hbase_codes/feature_engineering/parsers/code/target/hbase-bulkloader-1.0.jar ImportFromFile -c $COLUMN -i $HDFS_INPUT -t $HBASE_RAW_TABLE -d
echo "data loaded to hbase"
HBASE_PARSED_TABLE=parsed_test_corner_rdf
hadoop jar /home/user/mohit/hbase_codes/feature_engineering/parsers/code/target/hbase-bulkloader-1.0.jar ParseRdf -c $COLUMN -i $HBASE_RAW_TABLE -o $HBASE_PARSED_TABLE  -d
echo "data parse!!"
