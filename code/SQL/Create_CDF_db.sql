DROP SCHEMA IF EXISTS cdf CASCADE;
CREATE SCHEMA cdf;

CREATE SEQUENCE cdf.id_seq   -- helps create unique id value across all tables in schema cdf
;

DROP TABLE IF EXISTS cdf.IdentifierType;
CREATE TABLE cdf.IdentifierType (
id BIGINT DEFAULT nextval('cdf.id_seq') PRIMARY KEY,
text varchar(30) UNIQUE
);
COPY cdf.IdentifierType (text) FROM '/container_root_dir/SQL/enumerations/IdentifierType.txt';

DROP TABLE IF EXISTS cdf.GPUnit;
CREATE TABLE cdf.GPUnit (
id BIGINT DEFAULT nextval('cdf.id_seq') PRIMARY KEY,
name varchar(50) UNIQUE
);

DROP TABLE IF EXISTS cdf.ExternalIdentifier;
CREATE TABLE cdf.ExternalIdentifier (
id BIGINT DEFAULT nextval('cdf.id_seq') PRIMARY KEY,
foreign_id INTEGER, 
value varchar(50),
identifiertype_id INTEGER REFERENCES cdf.identifiertype(id),
othertype varchar(30),
UNIQUE (foreign_id,identifiertype_id,othertype)
);


DROP TABLE IF EXISTS cdf.CountItemStatus;
CREATE TABLE cdf.CountItemStatus (
id BIGINT DEFAULT nextval('cdf.id_seq') PRIMARY KEY,
text varchar(13) UNIQUE
);
COPY cdf.CountItemStatus (text) FROM '/container_root_dir/SQL/enumerations/CountItemStatus.txt';

DROP TABLE IF EXISTS cdf.ReportingUnitType;
CREATE TABLE cdf.ReportingUnitType (
id BIGINT DEFAULT nextval('cdf.id_seq') PRIMARY KEY,
text varchar(30) UNIQUE
);
COPY cdf.ReportingUnitType (text) FROM '/container_root_dir/SQL/enumerations/ReportingUnitType.txt';

DROP TABLE IF EXISTS cdf.ReportingUnit;
CREATE TABLE cdf.ReportingUnit (
id BIGINT DEFAULT nextval('cdf.id_seq') PRIMARY KEY,
reportingunittype_id INTEGER REFERENCES cdf.reportingunittype(id),
othertype varchar(30),
gpunit_id INTEGER REFERENCES cdf.gpunit(id)
);

DROP TABLE IF EXISTS cdf.Party;
CREATE TABLE cdf.Party (
id BIGINT DEFAULT nextval('cdf.id_seq') PRIMARY KEY,
abbreviation varchar(6),
name varchar(50) UNIQUE
)
;

DROP TABLE IF EXISTS cdf.ElectionType;
CREATE TABLE cdf.ElectionType (
id BIGINT DEFAULT nextval('cdf.id_seq') PRIMARY KEY,
text varchar(30) UNIQUE
);
COPY cdf.ElectionType (text) FROM '/container_root_dir/SQL/enumerations/ElectionType.txt';

DROP TABLE IF EXISTS cdf.Election;
CREATE TABLE cdf.Election (
id BIGINT DEFAULT nextval('cdf.id_seq') PRIMARY KEY,
name varchar(50) UNIQUE,
enddate DATE,
startdate DATE,
electiontype_id INTEGER REFERENCES cdf.electiontype(id),
othertype varchar(30)
)
;

DROP TABLE IF EXISTS cdf.CountItemType;
CREATE TABLE cdf.ElectionType (
id BIGINT DEFAULT nextval('cdf.id_seq') PRIMARY KEY,
text varchar(30) UNIQUE
);
COPY cdf.CountItemType (text) FROM '/container_root_dir/SQL/enumerations/CountItemType.txt';

DROP TABLE IF EXISTS cdf.Contest;
CREATE TABLE cdf.Contest (
id BIGINT DEFAULT nextval('cdf.id_seq') PRIMARY KEY,
name varchar(200) UNIQUE,
election_id INTEGER REFERENCES election(id)
)
;

DROP TABLE IF EXISTS cdf.BallotMeasureSelection;
CREATE TABLE cdf.BallotMeasureSelection (
id BIGINT DEFAULT nextval('cdf.id_seq') PRIMARY KEY,
selection varchar(10)
)
;

DROP TABLE IF EXISTS cdf.Candidate;
CREATE TABLE cdf.Candidate (
id BIGINT DEFAULT nextval('cdf.id_seq') PRIMARY KEY,
ballotname varchar(100),
election_id INTEGER REFERENCES election(id)
party_id INTEGER REFERENCES party(id)
)
;

DROP TABLE IF EXISTS cdf.CandidateSelection;
CREATE TABLE cdf.CandidateSelection (
id BIGINT DEFAULT nextval('cdf.id_seq') PRIMARY KEY,
candidate_id INTEGER REFERENCES candidate(id)
)
;

DROP TABLE IF EXISTS cdf.Contest_Selection_Join;
CREATE TABLE cdf.Contest_Selection_Join (
id BIGINT DEFAULT nextval('cdf.id_seq') PRIMARY KEY,
contest_id INTEGER REFERENCES contest(id),
candidateselection_id INTEGER REFERENCES candidateselection(id),
ballotmeasureselection_id INTEGER REFERENCES ballotmeasureselection(id),
CONSTRAINT contest_selection_type CHECK ( candidateselection_id IS NULL OR ballotmeasureselection_id IS NULL)
)
;

DROP TABLE IF EXISTS cdf.VoteCount;
CREATE TABLE cdf.VoteCount (
id BIGINT DEFAULT nextval('cdf.id_seq') PRIMARY KEY,
countitemstatus_id INTEGER REFERENCES cdf.countitemstatus(id),
countitemtype_id INTEGER REFERENCES cdf.countitemtype(id),
othertype varchar(30)
)
;
comment on column cdf.countitemstatus_id is 'CDF lists CountItemStatus as a field in ReportingUnit, not in VoteCount *** seems like an error';

