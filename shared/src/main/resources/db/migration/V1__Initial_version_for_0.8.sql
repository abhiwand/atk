--
-- PostgreSQL Schema Migration
--
-- This file was generated from pg_dump with modifications
-- See FlywayDB.org documentation to see how these changes are applied
--

SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: command; Type: TABLE; Schema: public; Owner: metastore; Tablespace:
--

CREATE TABLE command (
    command_id integer NOT NULL,
    name character varying(254) NOT NULL,
    arguments text,
    error text,
    complete boolean DEFAULT false NOT NULL,
    result text,
    created_on timestamp without time zone NOT NULL,
    modified_on timestamp without time zone NOT NULL,
    created_by bigint
);


ALTER TABLE public.command OWNER TO metastore;

--
-- Name: command_command_id_seq; Type: SEQUENCE; Schema: public; Owner: metastore
--

CREATE SEQUENCE command_command_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;


ALTER TABLE public.command_command_id_seq OWNER TO metastore;

--
-- Name: command_command_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: metastore
--

ALTER SEQUENCE command_command_id_seq OWNED BY command.command_id;


--
-- Name: command_command_id_seq; Type: SEQUENCE SET; Schema: public; Owner: metastore
--

SELECT pg_catalog.setval('command_command_id_seq', 1, false);


--
-- Name: frame; Type: TABLE; Schema: public; Owner: metastore; Tablespace:
--

CREATE TABLE frame (
    frame_id integer NOT NULL,
    name character varying(128) NOT NULL,
    description text,
    uri text NOT NULL,
    schema text NOT NULL,
    status_id bigint DEFAULT 1 NOT NULL,
    created_on timestamp without time zone NOT NULL,
    modified_on timestamp without time zone NOT NULL,
    created_by bigint,
    modified_by bigint
);


ALTER TABLE public.frame OWNER TO metastore;

--
-- Name: frame_frame_id_seq; Type: SEQUENCE; Schema: public; Owner: metastore
--

CREATE SEQUENCE frame_frame_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;


ALTER TABLE public.frame_frame_id_seq OWNER TO metastore;

--
-- Name: frame_frame_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: metastore
--

ALTER SEQUENCE frame_frame_id_seq OWNED BY frame.frame_id;


--
-- Name: frame_frame_id_seq; Type: SEQUENCE SET; Schema: public; Owner: metastore
--

SELECT pg_catalog.setval('frame_frame_id_seq', 1, false);


--
-- Name: graph; Type: TABLE; Schema: public; Owner: metastore; Tablespace:
--

CREATE TABLE graph (
    id integer NOT NULL,
    name character varying(128) NOT NULL,
    description text,
    storage character varying(254) NOT NULL,
    status_id bigint DEFAULT 1 NOT NULL,
    created_on timestamp without time zone NOT NULL,
    modified_on timestamp without time zone NOT NULL,
    created_by bigint,
    modified_by bigint
);


ALTER TABLE public.graph OWNER TO metastore;

--
-- Name: graph_id_seq; Type: SEQUENCE; Schema: public; Owner: metastore
--

CREATE SEQUENCE graph_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;


ALTER TABLE public.graph_id_seq OWNER TO metastore;

--
-- Name: graph_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: metastore
--

ALTER SEQUENCE graph_id_seq OWNED BY graph.id;


--
-- Name: graph_id_seq; Type: SEQUENCE SET; Schema: public; Owner: metastore
--

SELECT pg_catalog.setval('graph_id_seq', 1, false);


--
-- Name: status; Type: TABLE; Schema: public; Owner: metastore; Tablespace:
--

CREATE TABLE status (
    id bigint NOT NULL,
    name character varying(128) NOT NULL,
    description text NOT NULL,
    created_on timestamp without time zone NOT NULL,
    modified_on timestamp without time zone NOT NULL
);


ALTER TABLE public.status OWNER TO metastore;

--
-- Name: users; Type: TABLE; Schema: public; Owner: metastore; Tablespace:
--

CREATE TABLE users (
    id integer NOT NULL,
    username character varying(254),
    api_key character varying(512),
    created_on timestamp without time zone NOT NULL,
    modified_on timestamp without time zone NOT NULL
);


ALTER TABLE public.users OWNER TO metastore;

--
-- Name: users_id_seq; Type: SEQUENCE; Schema: public; Owner: metastore
--

CREATE SEQUENCE users_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;


ALTER TABLE public.users_id_seq OWNER TO metastore;

--
-- Name: users_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: metastore
--

ALTER SEQUENCE users_id_seq OWNED BY users.id;


--
-- Name: users_id_seq; Type: SEQUENCE SET; Schema: public; Owner: metastore
--

SELECT pg_catalog.setval('users_id_seq', 1, false);


--
-- Name: command_id; Type: DEFAULT; Schema: public; Owner: metastore
--

ALTER TABLE ONLY command ALTER COLUMN command_id SET DEFAULT nextval('command_command_id_seq'::regclass);


--
-- Name: frame_id; Type: DEFAULT; Schema: public; Owner: metastore
--

ALTER TABLE ONLY frame ALTER COLUMN frame_id SET DEFAULT nextval('frame_frame_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: metastore
--

ALTER TABLE ONLY graph ALTER COLUMN id SET DEFAULT nextval('graph_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: metastore
--

ALTER TABLE ONLY users ALTER COLUMN id SET DEFAULT nextval('users_id_seq'::regclass);


--
-- Data for Name: status; Type: TABLE DATA; Schema: public; Owner: metastore
--

COPY status (id, name, description, created_on, modified_on) FROM stdin;
1	INIT	Initial Status: currently building or initializing	2014-06-13 08:09:02.247	2014-06-13 08:09:02.273
2	ACTIVE	Active and can be interacted with	2014-06-13 08:09:02.35	2014-06-13 08:09:02.35
3	INCOMPLETE	Partially created: failure occurred during construction.	2014-06-13 08:09:02.363	2014-06-13 08:09:02.363
4	DELETED	Deleted but can still be un-deleted, no action has yet been taken on disk	2014-06-13 08:09:02.371	2014-06-13 08:09:02.371
5	DELETE_FINAL	Underlying storage has been reclaimed, no un-delete is possible	2014-06-13 08:09:02.38	2014-06-13 08:09:02.38
\.


--
-- Name: command_pkey; Type: CONSTRAINT; Schema: public; Owner: metastore; Tablespace:
--

ALTER TABLE ONLY command
    ADD CONSTRAINT command_pkey PRIMARY KEY (command_id);


--
-- Name: frame_pkey; Type: CONSTRAINT; Schema: public; Owner: metastore; Tablespace:
--

ALTER TABLE ONLY frame
    ADD CONSTRAINT frame_pkey PRIMARY KEY (frame_id);


--
-- Name: graph_pkey; Type: CONSTRAINT; Schema: public; Owner: metastore; Tablespace:
--

ALTER TABLE ONLY graph
    ADD CONSTRAINT graph_pkey PRIMARY KEY (id);


--
-- Name: status_pkey; Type: CONSTRAINT; Schema: public; Owner: metastore; Tablespace:
--

ALTER TABLE ONLY status
    ADD CONSTRAINT status_pkey PRIMARY KEY (id);


--
-- Name: users_pkey; Type: CONSTRAINT; Schema: public; Owner: metastore; Tablespace:
--

ALTER TABLE ONLY users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);


--
-- Name: command_created_by; Type: FK CONSTRAINT; Schema: public; Owner: metastore
--

ALTER TABLE ONLY command
    ADD CONSTRAINT command_created_by FOREIGN KEY (created_by) REFERENCES users(id);


--
-- Name: frame_created_by; Type: FK CONSTRAINT; Schema: public; Owner: metastore
--

ALTER TABLE ONLY frame
    ADD CONSTRAINT frame_created_by FOREIGN KEY (created_by) REFERENCES users(id);


--
-- Name: frame_modified_by; Type: FK CONSTRAINT; Schema: public; Owner: metastore
--

ALTER TABLE ONLY frame
    ADD CONSTRAINT frame_modified_by FOREIGN KEY (modified_by) REFERENCES users(id);


--
-- Name: frame_status_id; Type: FK CONSTRAINT; Schema: public; Owner: metastore
--

ALTER TABLE ONLY frame
    ADD CONSTRAINT frame_status_id FOREIGN KEY (status_id) REFERENCES status(id);


--
-- Name: graph_created_by; Type: FK CONSTRAINT; Schema: public; Owner: metastore
--

ALTER TABLE ONLY graph
    ADD CONSTRAINT graph_created_by FOREIGN KEY (created_by) REFERENCES users(id);


--
-- Name: graph_modified_by; Type: FK CONSTRAINT; Schema: public; Owner: metastore
--

ALTER TABLE ONLY graph
    ADD CONSTRAINT graph_modified_by FOREIGN KEY (modified_by) REFERENCES users(id);


--
-- Name: graph_status_id; Type: FK CONSTRAINT; Schema: public; Owner: metastore
--

ALTER TABLE ONLY graph
    ADD CONSTRAINT graph_status_id FOREIGN KEY (status_id) REFERENCES status(id);


--
-- Name: public; Type: ACL; Schema: -; Owner: postgres
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM postgres;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- PostgreSQL database dump complete
--
