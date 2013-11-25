
HADOOP=/home/kdatta1/hadoop-1.2.1/bin/hadoop
GB_JAR=/home/kdatta1/tribeca/sprint5/source_code/graphbuilder/target/graphbuilder-2.0-hadoop-job.jar
GB_CLASS=com.intel.hadoop.graphbuilder.sampleapplications.TableToRDFGraph
OUTPUT=output0

$HADOOP dfs -rmr $OUTPUT
$HADOOP dfs -rmr .Trash/*

$HADOOP jar $GB_JAR $GB_CLASS -t table0 \
  -v "features:id=features:name,features:age,features:dept" "features:manager" \
  -e "features:id,features:manager,worksUnder,features:underManager"  \
  -o $OUTPUT \
  -n OWL

echo "---------------Vertices---------------"
$HADOOP dfs -cat $OUTPUT/vdata/rdftriples-*
echo "---------------Edges---------------"
$HADOOP dfs -cat $OUTPUT/edata/rdftriples-*
