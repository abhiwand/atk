HDFS_INPUT=/user/user/mohit/sample/fail_sample.json
HBASE_RAW_TABLE=test_corner_json
COLUMN=data:json
hadoop jar /home/user/mohit/hbase_codes/feature_engineering/parsers/code/target/hbase-bulkloader-1.0.jar ImportFromFile -c $COLUMN -i $HDFS_INPUT -t $HBASE_RAW_TABLE
echo "data loaded to hbase"
HBASE_PARSED_TABLE=parsed_test_corner_json
hadoop jar /home/user/mohit/hbase_codes/feature_engineering/parsers/code/target/hbase-bulkloader-1.0.jar ParseJson  -libjars /home/user/mohit/hbase_codes/feature_engineering/parsers/lib/json-simple-1.1.1.jar -c $COLUMN -i $HBASE_RAW_TABLE -o $HBASE_PARSED_TABLE -d
echo "data parse!!"
