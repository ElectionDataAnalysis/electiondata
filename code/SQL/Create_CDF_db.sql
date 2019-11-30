DROP SCHEMA IF EXISTS cdf CASCADE;
CREATE SCHEMA cdf;

CREATE SEQUENCE cdf.id_seq   -- helps create unique id value across all tables in schema cdf
;

DROP TABLE IF EXISTS cdf.IdentifierType;
CREATE TABLE cdf.IdentifierType (
id BIGINT DEFAULT nextval('cdf.id_seq') PRIMARY KEY,
text varchar(30)
);
COPY cdf.IdentifierType (text) FROM '/container_root_dir/SQL/enumerations/IdentifierType.txt';

DROP TABLE IF EXISTS cdf.GPUnit;
CREATE TABLE cdf.GPUnit (
id BIGINT DEFAULT nextval('cdf.id_seq') PRIMARY KEY,
name varchar(50)
);

DROP TABLE IF EXISTS cdf.ExternalIdentifier;
CREATE TABLE cdf.ExternalIdentifier (
id BIGINT DEFAULT nextval('cdf.id_seq') PRIMARY KEY,
foreign_id INTEGER, 
value varchar(50),
identifiertype_id INTEGER REFERENCES cdf.identifiertype(id),
othertype varchar(30)
);


DROP TABLE IF EXISTS cdf.CountItemStatus;
CREATE TABLE cdf.CountItemStatus (
id BIGINT DEFAULT nextval('cdf.id_seq') PRIMARY KEY,
text varchar(13)
);
COPY cdf.CountItemStatus (text) FROM '/container_root_dir/SQL/enumerations/CountItemStatus.txt';

DROP TABLE IF EXISTS cdf.ReportingUnitType;
CREATE TABLE cdf.ReportingUnitType (
id BIGINT DEFAULT nextval('cdf.id_seq') PRIMARY KEY,
text varchar(30)
);
COPY cdf.ReportingUnitType (text) FROM '/container_root_dir/SQL/enumerations/ReportingUnitType.txt';

DROP TABLE IF EXISTS cdf.ReportingUnit;
CREATE TABLE cdf.ReportingUnit (
id BIGINT DEFAULT nextval('cdf.id_seq') PRIMARY KEY,
reportingunittype_id INTEGER REFERENCES cdf.reportingunittype(id),
othertype varchar(30),
countitemstatus_id INTEGER REFERENCES cdf.countitemstatus(id)
);


DROP TABLE IF EXISTS cdf.Party;
CREATE TABLE cdf.Party (
id BIGINT DEFAULT nextval('cdf.id_seq') PRIMARY KEY,
abbreviation varchar(6),
name varchar(50)
)
;


DROP TABLE IF EXISTS cdf.ElectionType;
CREATE TABLE cdf.ElectionType (
id BIGINT DEFAULT nextval('cdf.id_seq') PRIMARY KEY,
text varchar(30)
);
COPY cdf.ElectionType (text) FROM '/container_root_dir/SQL/enumerations/ElectionType.txt';

DROP TABLE IF EXISTS cdf.Election;
CREATE TABLE cdf.Election (
id BIGINT DEFAULT nextval('cdf.id_seq') PRIMARY KEY,
name varchar(50),
enddate DATE,
startdate DATE,
electiontype_id INTEGER REFERENCES cdf.electiontype(id),
othertype varchar(30)
)
;

DROP TABLE IF EXISTS cdf.Contest;
CREATE TABLE cdf.Contest (
id SERIAL PRIMARY KEY
)
;
