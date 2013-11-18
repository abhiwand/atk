-- if you run this script in local mode, make sure /tmp/worldbank.csv exists on local FS

REGISTER 'pig/tests/erroneous_udfs.py' USING jython as erroneous_udfs	
REGISTER /home/user/pig-0.12.0/contrib/piggybank/java/piggybank.jar;
logs = LOAD '/tmp/worldbank.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER') AS (country:chararray,year:chararray,co2_emission:double,electric_consumption:double,energy_use:double,fertility:double,gni:double,internet_users:double,life_expectancy:double,military_expenses:double,population: double,hiv_prevelence:double);
squared_country = FOREACH logs GENERATE erroneous_udfs.square(country);
dump squared_country;
