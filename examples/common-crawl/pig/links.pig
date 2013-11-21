register IntelGraphAnalytics-0.0.1-SNAPSHOT-jar-with-dependencies.jar

DEFINE ArcLoader2 com.intel.graph.analytics.etl.ArcLoader2();
DEFINE ExtractDomain com.intel.graph.analytics.etl.ExtractDomain();
DEFINE ExtractLinks com.intel.graph.analytics.etl.ExtractLinks();
DEFINE ExtractWords com.intel.graph.analytics.etl.ExtractWords();

arc_records = LOAD '1285385314958_17.arc.gz' USING ArcLoader2() AS (url:chararray, timestamp:chararray, content:chararray);

url_link_list = FOREACH arc_records GENERATE url AS src_url, FLATTEN(ExtractLinks(content)) AS dest_url;
domain_link_list = FOREACH url_link_list GENERATE ExtractDomain(src_url) AS src_domain, ExtractDomain(dest_url) AS dest_domain;
domain_link_list = FILTER domain_link_list BY dest_domain IS NOT NULL AND dest_domain != '' AND src_domain != dest_domain;
