
/**
 * Add the column detailedProgress to command table
 * This column is used to display extra details of command progress
 */
CREATE TABLE command (
  query_id bigint PRIMARY KEY,
  name character varying(254) NOT NULL,
  arguments text,
  error text,
  progress text NOT NULL DEFAULT '[]',
  detailedProgress text NOT NULL DEFAULT '[]',
  complete boolean DEFAULT false NOT NULL,
  total_partitions bigint,
  created_on timestamp without time zone NOT NULL,
  modified_on timestamp without time zone NOT NULL,
  created_by bigint
);