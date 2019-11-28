-- Postgresql 11
--
 
DROP SCHEMA IF EXISTS nc_cdf CASCADE;
CREATE SCHEMA nc_cdf;

CREATE SEQUENCE nc_cdf.id_seq;   -- helps create unique id value across all tables in schema nc_cdf

DROP TABLE IF EXISTS nc_cdf.IdentifierType;
CREATE TABLE nc_cdf.IdentifierType (
id BIGINT DEFAULT nextval('nc_cdf.id_seq') PRIMARY KEY,
text varchar(30)
)
;

COPY nc_cdf.IdentifierType (text) FROM '/container_root_dir/SQL/enumerations/IdentifierType.txt';

DROP TABLE IF EXISTS nc_cdf.GPUnit;
CREATE TABLE nc_cdf.GPUnit (
id BIGINT DEFAULT nextval('nc_cdf.id_seq') PRIMARY KEY,
name varchar(50)
)
;

DROP TABLE IF EXISTS nc_cdf.ExternalIdentifier;
CREATE TABLE nc_cdf.ExternalIdentifier (
id BIGINT DEFAULT nextval('nc_cdf.id_seq') PRIMARY KEY,
foreign_id INTEGER, -- how to reference the primary key of the foreign table? We don't know which table it is! ***
value varchar(50),
identifiertype_id INTEGER REFERENCES nc_cdf.identifiertype(id)
)
;

-- insert generic data for North Carolina
INSERT INTO nc_cdf.GPUnit (name) VALUES ('North Carolina');
WITH ids AS (SELECT g.id AS nc, i.id AS fips FROM nc_cdf.GPUnit g, nc_cdf.IdentifierType i)
    INSERT INTO nc_cdf.ExternalIdentifier(foreign_id,value,identifiertype_id) VALUES ((SELECT nc FROM ids LIMIT 1),'3700000000',(SELECT fips FROM ids LIMIT 1));


DROP TABLE IF EXISTS nc_cdf.CountItemStatus;
CREATE TABLE nc_cdf.CountItemStatus (
id BIGINT DEFAULT nextval('nc_cdf.id_seq') PRIMARY KEY,
text varchar(13)
)
;

COPY nc_cdf.CountItemStatus (text) FROM '/container_root_dir/SQL/enumerations/CountItemStatus.txt';

DROP TABLE IF EXISTS nc_cdf.ReportingUnitType;
CREATE TABLE nc_cdf.ReportingUnitType (
id BIGINT DEFAULT nextval('nc_cdf.id_seq') PRIMARY KEY,
text varchar(30)
)
;

COPY nc_cdf.ReportingUnitType (text) FROM '/container_root_dir/SQL/enumerations/ReportingUnitType.txt';

DROP TABLE IF EXISTS nc_cdf.ReportingUnit;
CREATE TABLE nc_cdf.ReportingUnit (
id BIGINT DEFAULT nextval('nc_cdf.id_seq') PRIMARY KEY,
reportingunittype_id INTEGER REFERENCES nc_cdf.reportingunittype(id),
countitemstatus_id INTEGER REFERENCES nc_cdf.countitemstatus(id)
)
;



DROP TABLE IF EXISTS nc_cdf.Party;
CREATE TABLE nc_cdf.Party (
id BIGINT DEFAULT nextval('nc_cdf.id_seq') PRIMARY KEY,
abbreviation varchar(6),
name varchar(50)
)
;

-- BTRIM removes both leading and trailing spaces from a string
INSERT INTO nc_cdf.Party (abbreviation) SELECT DISTINCT BTRIM(choice_party) FROM nc.results_pct WHERE choice_party IS NOT NULL AND BTRIM(choice_party) <> '';

DROP TABLE IF EXISTS nc_cdf.ElectionType;
CREATE TABLE nc_cdf.ElectionType (
id BIGINT DEFAULT nextval('nc_cdf.id_seq') PRIMARY KEY,
text varchar(30)
)
;

COPY nc_cdf.ElectionType (text) FROM '/container_root_dir/SQL/enumerations/ElectionType.txt';

DROP TABLE IF EXISTS nc_cdf.Election;
CREATE TABLE nc_cdf.Election (
id BIGINT DEFAULT nextval('nc_cdf.id_seq') PRIMARY KEY,
name varchar(50),
enddate DATE,
startdate DATE,
electiontype_id INTEGER REFERENCES nc_cdf.electiontype(id),
OtherType varchar(20)
)
;

-- --

DROP TABLE IF EXISTS nc_cdf.Contest;
CREATE TABLE nc_cdf.Contest (
id SERIAL PRIMARY KEY
)
;
