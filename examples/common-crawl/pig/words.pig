register IntelGraphAnalytics-0.0.1-SNAPSHOT-jar-with-dependencies.jar

DEFINE ArcLoader2 com.intel.graph.analytics.etl.ArcLoader2();
DEFINE ExtractDomain com.intel.graph.analytics.etl.ExtractDomain();
DEFINE ExtractLinks com.intel.graph.analytics.etl.ExtractLinks();
DEFINE ExtractWords com.intel.graph.analytics.etl.ExtractWords();

arc_records = LOAD '1285385314958_17.arc.gz' USING ArcLoader2() AS (url:chararray, timestamp:chararray, content:chararray);

word_list = FOREACH arc_records GENERATE ExtractDomain(url) AS domain, FLATTEN(ExtractWords(content)) AS word;

