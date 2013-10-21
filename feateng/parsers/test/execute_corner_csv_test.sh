HDFS_INPUT=/user/user/mohit/sample/fail_sample.csv
HBASE_RAW_TABLE=test_corner_csv
COLUMN=data:csv
hadoop jar /home/user/mohit/hbase_codes/feature_engineering/parsers/code/target/hbase-bulkloader-1.0.jar ImportFromFile -c $COLUMN -i $HDFS_INPUT -t $HBASE_RAW_TABLE
echo "data loaded to hbase"
HBASE_PARSED_TABLE=parsed_test_corner_csv
HEADER=foo,bar,foobar,fushbar
hadoop jar /home/user/mohit/hbase_codes/feature_engineering/parsers/code/target/hbase-bulkloader-1.0.jar ParseCsv -c $COLUMN -i $HBASE_RAW_TABLE -o $HBASE_PARSED_TABLE -h $HEADER -d
echo "data parse!!"
