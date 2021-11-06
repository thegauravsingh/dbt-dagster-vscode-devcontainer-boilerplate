CREATE DATABASE  dbtdb;
CREATE USER dbtuser WITH ENCRYPTED PASSWORD 'pssd';
GRANT ALL PRIVILEGES ON DATABASE dbtdb TO dbtuser;
ALTER DATABASE dbtdb OWNER TO dbtuser;

CREATE SCHEMA dbt AUTHORIZATION dbtuser;