-- Postgresql 11
--
 
DROP SCHEMA IF EXISTS nc_cdf CASCADE;
CREATE SCHEMA nc_cdf;

DROP TABLE IF EXISTS nc_cdf.CountItemStatus;
CREATE TABLE nc_cdf.CountItemStatus (
id SERIAL PRIMARY KEY,
text varchar(13)
)
;

COPY nc_cdf.CountItemStatus (text) FROM '/container_root_dir/SQL/enumerations/CountItemStatus.txt';

DROP TABLE IF EXISTS nc_cdf.ReportingUnitType;
CREATE TABLE nc_cdf.ReportingUnitType (
id SERIAL PRIMARY KEY,
text varchar(20)
)
;

COPY nc_cdf.ReportingUnitType (text) FROM '/container_root_dir/SQL/enumerations/ReportingUnitType.txt';

DROP TABLE IF EXISTS nc_cdf.ReportingUnit;
CREATE TABLE nc_cdf.ReportingUnit (
id SERIAL PRIMARY KEY,
reportingunittype_id REFERENCES reportingunittype(id),
countitemstatus_id REFERENCES countitemstatus(id)
)
;

DROP TABLE IF EXISTS nc_cdf.Party;
CREATE TABLE nc_cdf.Party (
id SERIAL PRIMARY KEY,
abbreviation varchar(6),
name varchar(50)
)
;

-- BTRIM removes both leading and trailing spaces from a string
INSERT INTO nc_cdf.Party (abbreviation) SELECT DISTINCT BTRIM(choice_party) FROM nc.results_pct WHERE choice_party IS NOT NULL AND BTRIM(choice_party) <> '';

DROP TABLE IF EXISTS nc_cdf.Contest;
CREATE TABLE nc_cdf.Contest (
id SERIAL PRIMARY KEY,
)
;
