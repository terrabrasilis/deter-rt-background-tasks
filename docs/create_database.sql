-- DROP DATABASE IF EXISTS deter_rt_amazonia;

CREATE DATABASE deter_rt_amazonia
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;

COMMENT ON DATABASE deter_rt_amazonia
    IS 'Used in the validation phase. Created in 2025/06/10';


-- -------------------------------------------
-- Need PostGIS extension
-- -------------------------------------------

CREATE EXTENSION postgis;

-- Ordinary tables
-- -------------------------------------------

-- DROP TABLE IF EXISTS public.deter_rt;

CREATE TABLE IF NOT EXISTS public.deter_rt
(
    id serial,
    uuid uuid NOT NULL DEFAULT gen_random_uuid(),
    geom geometry(MultiPolygon,4674) NOT NULL,
    class_name character varying(256) NOT NULL,
    view_date date NOT NULL,
    area_km double precision NOT NULL,
    created_at date NOT NULL DEFAULT (now())::date,
    tile_id character varying(256) NOT NULL,
    auditar integer NOT NULL DEFAULT 0,
    audit_date date,
    CONSTRAINT deter_rt_pkey PRIMARY KEY (id)
);

-- DROP INDEX IF EXISTS public.deter_rt_geom_idx;

CREATE INDEX IF NOT EXISTS deter_rt_geom_idx
    ON public.deter_rt USING gist
    (geom)
    WITH (buffering=auto)
    TABLESPACE pg_default;


-- DROP TABLE IF EXISTS public.deter_otico;

CREATE TABLE IF NOT EXISTS public.deter_otico
(
    id serial,
    geom geometry(MultiPolygon,4674) NOT NULL,
    view_date date NOT NULL,
    class_name character varying(256) NOT NULL,
    CONSTRAINT deter_otico_pkey PRIMARY KEY (id)
);

-- DROP INDEX IF EXISTS public.deter_otico_geom_idx;

CREATE INDEX IF NOT EXISTS deter_otico_geom_idx
    ON public.deter_otico USING gist
    (geom)
    WITH (buffering=auto)
    TABLESPACE pg_default;

-- DROP TABLE IF EXISTS public.deter_rt_validados;

CREATE TABLE IF NOT EXISTS public.deter_rt_validados
(
    fid serial,
    id integer,
    uuid character varying NOT NULL,
    geom geometry(MultiPolygon,4674) NOT NULL,
    area_km double precision NOT NULL,
    view_date date NOT NULL,
    class_name character varying(256),
    created_at date NOT NULL,
    tile_id character varying(256) NOT NULL,
    nome_avaliador character varying,
    classe_avaliador character varying,
    data_avaliacao timestamp without time zone,
    deltat integer,
    lat double precision,
    lon double precision,
    status integer,
    -- nome_avaliador1 character varying,
    -- classe_avaliador1 character varying,
    -- datafim_avaliador1 timestamp without time zone,
    -- deltat_avaliador1 integer,
    -- nome_avaliador2 character varying,
    -- classe_avaliador2 character varying,
    -- datafim_avaliador2 timestamp without time zone,
    -- deltat_avaliador2 integer,
    -- nome_avaliador3 character varying,
    -- classe_avaliador3 character varying,
    -- datafim_avaliador3 timestamp without time zone,
    -- deltat_avaliador3 integer,
    -- nome_avaliador4 character varying,
    -- classe_avaliador4 character varying,
    -- datafim_avaliador4 timestamp without time zone,
    -- deltat_avaliador4 integer,
    CONSTRAINT deter_rt_validados_fid_pk PRIMARY KEY (fid)
);


-- Control tables
-- -------------------------------------------

-- DROP TABLE IF EXISTS public.collector_log;

CREATE TABLE IF NOT EXISTS public.collector_log
(
    id serial,
    description text,
    processed_on date NOT NULL DEFAULT (now())::date,
    success boolean NOT NULL,
    CONSTRAINT collector_log_pkey PRIMARY KEY (id)
);

-- DROP TABLE IF EXISTS public.input_data;

CREATE TABLE IF NOT EXISTS public.input_data
(
    id serial,
    file_name character varying(256) NOT NULL,
    download_date date NOT NULL DEFAULT (now())::date,
    count_itens integer NOT NULL,
    CONSTRAINT input_data_pkey PRIMARY KEY (id)
);

-- Temporary tables
-- -------------------------------------------

-- DROP SCHEMA IF EXISTS tmp ;

CREATE SCHEMA IF NOT EXISTS tmp
    AUTHORIZATION postgres;

COMMENT ON SCHEMA tmp
    IS 'Used to temporary tables from tasks';


