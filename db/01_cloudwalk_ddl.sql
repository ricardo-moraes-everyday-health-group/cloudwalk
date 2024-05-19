CREATE DATABASE gdp;

\c gdp;

CREATE TABLE public.ny_gdp_mktp_cd_data (
    "country_id" VARCHAR(2) NOT NULL,
    "country_value" VARCHAR(100),
    "countryiso3code" VARCHAR(3),
    "date" VARCHAR(4) NOT NULL,
    "value" DECIMAL,
    "unit" VARCHAR(100),
    "obs_status" VARCHAR(65535),
    "decimal_value" INT
    );

CREATE TABLE public.country (
    "id" VARCHAR(2) NOT NULL,
    "name" VARCHAR(100),
    "iso3_code" VARCHAR(3),
    CONSTRAINT "pk_country" PRIMARY KEY ("id")
    );

CREATE TABLE public.gdp (
    "country_id" VARCHAR(2) NOT NULL,
    "year" VARCHAR(4) NOT NULL,
    "value" DECIMAL,
    CONSTRAINT "pk_gdp" PRIMARY KEY ("country_id", "year")
    );

CREATE USER gdp WITH PASSWORD 'gdp';

GRANT ALL PRIVILEGES ON DATABASE gdp TO gdp;

ALTER TABLE public.ny_gdp_mktp_cd_data OWNER TO gdp;
ALTER TABLE public.country OWNER TO gdp;
ALTER TABLE public.gdp OWNER TO gdp;

CREATE EXTENSION IF NOT EXISTS tablefunc;