-- Pig script to create a Link-Page graph from Wikipedia dataset


REGISTER /usr/local/pig/piggybank.jar;
xml_data = LOAD 'wiki.test' using org.apache.pig.piggybank.storage.XMLLoader('page') AS (page: chararray);
DUMP xml_data;
x = foreach xml_data generate REGEX_EXTRACT(page, '<id>(.*?)</id>', 1) as (id: chararray), page;
x = foreach x generate REGEX_EXTRACT(page, '<title>(.*?)</title>', 1) as (title: chararray), id, page;
x = foreach x generate REGEX_EXTRACT(page, '<text\\s.*>(.*?)</text>', 1) as (text: chararray), id, title;
