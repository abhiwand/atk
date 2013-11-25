-- REGISTER The demo jar first.
-- register target/IntelGraphAnalytics-0.0.1-SNAPSHOT-jar-with-dependencies.jar                
DEFINE ArcLoader com.intel.graph.analytics.examples.ArcLoader();
DEFINE ExtractText com.intel.graph.analytics.examples.ExtractText();
DEFINE RemoveSymbols com.intel.graph.analytics.examples.RemoveSymbols();
DEFINE RemoveNums com.intel.graph.analytics.examples.RemoveNums();


raw_data = LOAD '/user/mohitdee/arc_data/100.arc.gz' USING ArcLoader() AS (url:chararray, timestamp:chararray, content:chararray);

url_text = FOREACH raw_data GENERATE url AS url, FLATTEN(ExtractText(content)) AS text;
lower_flatten = FOREACH url_text GENERATE url, LOWER(text) as lower_clean_text: chararray;

--limited = LIMIT lower_flatten 2;

clean_symbols = FOREACH lower_flatten GENERATE url, RemoveSymbols(lower_clean_text) as cleaned_text:chararray;
clean_numerics = FOREACH clean_symbols GENERATE url, RemoveNums(cleaned_text) as cleaned_text:chararray;


splitted_text = FOREACH clean_numerics GENERATE url, FLATTEN(TOKENIZE(cleaned_text)) as tokens;
filter_small_words = FILTER  splitted_text BY SIZE(tokens) >4;

grouped = GROUP filter_small_words BY (url,tokens);      

--graph = FOREACH  grouped GENERATE group , deduped.tokens;
counts = FOREACH grouped GENERATE group as wordDoc, COUNT(filter_small_words) AS wordCount;

--final = FOREACH counts GENERATE flatten(wordDoc) , wordCount;   
