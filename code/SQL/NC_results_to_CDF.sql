-- Postgresql 11
--
 
DROP SCHEMA IF EXISTS nc_cdf CASCADE;
CREATE SCHEMA nc_cdf;

DROP TABLE IF EXISTS nc_cdf.CountItemStatus;
CREATE TABLE nc_cdf.CountItemStatus (
id SERIAL,
text varchar(13),
CONSTRAINT countitemstatus_pk PRIMARY KEY(id)
)
;

COPY nc_cdf.CountItemStatus (text) FROM '/container_root_dir/SQL/enumerations/CountItemStatus.txt';

DROP TABLE IF EXISTS nc_cdf.ReportingUnitType;
CREATE TABLE nc_cdf.ReportingUnitType (
id SERIAL,
text varchar(20),
CONSTRAINT ReportingUnitType_pk PRIMARY KEY(id)
)
;

COPY nc_cdf.ReportingUnitType (text) FROM '/container_root_dir/SQL/enumerations/ReportingUnitType.txt';

DROP TABLE IF EXISTS nc_cdf.ReportingUnit;
CREATE TABLE nc_cdf.ReportingUnit (
id SERIAL,
CONSTRAINT ReportingUnit_pk PRIMARY KEY (id)
)
;

DROP TABLE IF EXISTS nc_cdf.Party;
CREATE TABLE nc_cdf.Party (
id SERIAL,
abbreviation varchar(6),
name varchar(50),
CONSTRAINT Party_pk PRIMARY KEY (id)
)
;

-- BTRIM removes both leading and trailing spaces from a string
INSERT INTO nc_cdf.Party (abbreviation) SELECT DISTINCT BTRIM(choice_party) FROM nc.results_pct WHERE choice_party IS NOT NULL AND BTRIM(choice_party) <> '';

DROP TABLE IF EXISTS nc_cdf.Contest;
CREATE TABLE nc_cdf.Contest (
id SERIAL,
CONSTRAINT Contest_pk PRIMARY KEY (id)
)
;
