--
-- PostgreSQL Schema Migration
--
-- This file was generated from pg_dump with modifications
-- See FlywayDB.org documentation to see how these changes are applied
--
-- !!! DO NOT MODIFY THIS SCRIPT !!!
--

-- Add columns to frames:

ALTER TABLE frame ADD COLUMN command_id LONG NULL;

ALTER TABLE frame ADD COLUMN parent LONG NULL;

ALTER TABLE frame ADD COLUMN materialized TIMESTAMP WITHOUT TIME ZONE NULL;

ALTER TABLE frame ADD COLUMN materialized_duration INTERVAL NULL;

ALTER TABLE frame ADD COLUMN storage_format TEXT NULL;

ALTER TABLE frame ADD COLUMN storage_uri TEXT NULL;

UPDATE TABLE frame SET storage_format = 'file/sequence', storage_uri = frame.Id + '/rev' + frame.revision;

ALTER TABLE frame DROP COLUMN revision;



