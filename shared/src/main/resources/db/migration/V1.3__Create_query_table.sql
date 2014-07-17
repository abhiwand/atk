--
-- PostgreSQL Schema Migration
--
-- This file was generated from pg_dump with modifications
-- See FlywayDB.org documentation to see how these changes are applied
--
-- !!! DO NOT MODIFY THIS SCRIPT !!!
--

--
-- Name: queries; Type: TABLE; Schema: public; Owner: metastore; Tablespace:
--
CREATE TABLE queries (
  query_id bigint PRIMARY KEY,
  name character varying(254) NOT NULL,
  arguments text,
  error text,
  complete boolean DEFAULT false NOT NULL,
  total_pages bigint,
  page_size bigint,
  created_on timestamp without time zone NOT NULL,
  modified_on timestamp without time zone NOT NULL,
  created_by bigint
);

ALTER TABLE public.queries OWNER TO metastore;

--
-- Name: queries_query_id_seq; Type: SEQUENCE; Schema: public; Owner: metastore
--
CREATE SEQUENCE queries_query_id_seq
  START WITH 1
  INCREMENT BY 1
  NO MAXVALUE
  NO MINVALUE
  CACHE 1;

ALTER TABLE public.queries_query_id_seq OWNER TO metastore;

--
-- Name: queries_query_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: metastore
--

ALTER SEQUENCE queries_query_id_seq OWNED BY queries.query_id;


--
-- Name: queries_query_id_seq; Type: SEQUENCE SET; Schema: public; Owner: metastore
--

SELECT pg_catalog.setval('queries_query_id_seq', 1, false);

--
-- Name: query_id; Type: DEFAULT; Schema: public; Owner: metastore
--

ALTER TABLE ONLY queries ALTER COLUMN query_id SET DEFAULT nextval('queries_query_id_seq'::regclass);