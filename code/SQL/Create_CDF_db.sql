-- PostgreSQL 12
-- *** need to add "NOT NULL" requirements per CDF

DROP SCHEMA IF EXISTS cdf CASCADE;
CREATE SCHEMA cdf;

CREATE SEQUENCE cdf.id_seq   -- helps create unique id value across all tables in schema cdf 
;

DROP TABLE IF EXISTS cdf.IdentifierType;
CREATE TABLE cdf.IdentifierType (
id BIGINT DEFAULT nextval('cdf.id_seq') PRIMARY KEY,
txt text  UNIQUE NOT NULL
);
COPY cdf.IdentifierType (txt) FROM '/container_root_dir/SQL/enumerations/IdentifierType.txt';

DROP TABLE IF EXISTS cdf.GpUnit;
CREATE TABLE cdf.GpUnit (
id BIGINT DEFAULT nextval('cdf.id_seq') PRIMARY KEY,
name  text  UNIQUE
);

DROP TABLE IF EXISTS cdf.ExternalIdentifier;
CREATE TABLE cdf.ExternalIdentifier (
id BIGINT DEFAULT nextval('cdf.id_seq') PRIMARY KEY,
foreign_id INTEGER, 
value  text NOT NULL,
identifiertype_id INTEGER NOT NULL REFERENCES cdf.identifiertype(id),
othertype  text ,
UNIQUE (foreign_id,identifiertype_id,othertype)
);
-- check that if identifier type is "other" then othertype string is not empty or null ***


DROP TABLE IF EXISTS cdf.CountItemStatus;
CREATE TABLE cdf.CountItemStatus (
id BIGINT DEFAULT nextval('cdf.id_seq') PRIMARY KEY,
txt text  UNIQUE NOT NULL
);
COPY cdf.CountItemStatus (txt) FROM '/container_root_dir/SQL/enumerations/CountItemStatus.txt';

DROP TABLE IF EXISTS cdf.ReportingUnitType;
CREATE TABLE cdf.ReportingUnitType (
id BIGINT DEFAULT nextval('cdf.id_seq') PRIMARY KEY,
txt text  UNIQUE NOT NULL
);
COPY cdf.ReportingUnitType (txt) FROM '/container_root_dir/SQL/enumerations/ReportingUnitType.txt';

DROP TABLE IF EXISTS cdf.ReportingUnit;
CREATE TABLE cdf.ReportingUnit (
id BIGINT DEFAULT nextval('cdf.id_seq') PRIMARY KEY,
reportingunittype_id INTEGER NOT NULL REFERENCES cdf.reportingunittype(id),
othertype  text ,
gpunit_id INTEGER NOT NULL REFERENCES cdf.gpunit(id)
);

DROP TABLE IF EXISTS cdf.Party;
CREATE TABLE cdf.Party (
id BIGINT DEFAULT nextval('cdf.id_seq') PRIMARY KEY,
abbreviation  text ,
name  text  UNIQUE
)
;

DROP TABLE IF EXISTS cdf.ElectionType;
CREATE TABLE cdf.ElectionType (
id BIGINT DEFAULT nextval('cdf.id_seq') PRIMARY KEY,
txt text  UNIQUE
);
COPY cdf.ElectionType (txt) FROM '/container_root_dir/SQL/enumerations/ElectionType.txt';

DROP TABLE IF EXISTS cdf.Election;
CREATE TABLE cdf.Election (
id BIGINT DEFAULT nextval('cdf.id_seq') PRIMARY KEY,
name  text  UNIQUE,
enddate DATE,
startdate DATE,
electiontype_id INTEGER REFERENCES cdf.electiontype(id),
othertype  text 
)
;

DROP TABLE IF EXISTS cdf.CountItemType;
CREATE TABLE cdf.CountItemType (
id BIGINT DEFAULT nextval('cdf.id_seq') PRIMARY KEY,
txt text  UNIQUE
);

COPY cdf.CountItemType (txt) FROM '/container_root_dir/SQL/enumerations/CountItemType.txt';

DROP TABLE IF EXISTS cdf.Office;
CREATE TABLE cdf.Office (
id BIGINT DEFAULT nextval('cdf.id_seq') PRIMARY KEY,
name  text  UNIQUE,
description  text 
)
;

DROP TABLE IF EXISTS cdf.CandidateContest;  -- CDF requires a name ***
CREATE TABLE cdf.CandidateContest (
id BIGINT DEFAULT nextval('cdf.id_seq') PRIMARY KEY,
votesallowed INTEGER NOT NULL,
numberelected INTEGER,
numberrunoff INTEGER,
office_id INTEGER REFERENCES cdf.office(id),
primaryparty_id INTEGER REFERENCES cdf.party(id),
electiondistrict_id INTEGER NOT NULL REFERENCES cdf.reportingunit(id),
name  text    -- *** how to fill this?
)
;

DROP TABLE IF EXISTS cdf.BallotMeasureContest;  -- CDF requires a name ***
CREATE TABLE cdf.BallotMeasureContest (
id BIGINT DEFAULT nextval('cdf.id_seq') PRIMARY KEY,
electiondistrict_id INTEGER NOT NULL REFERENCES cdf.reportingunit(id),
name  text    -- *** how to fill this?
)
;


DROP TABLE IF EXISTS cdf.BallotMeasureSelection;    -- note: we don't enumerate the choices, though some kind of universal yes/no is tempting ***
CREATE TABLE cdf.BallotMeasureSelection (
id BIGINT DEFAULT nextval('cdf.id_seq') PRIMARY KEY,
selection  text 
)
;

DROP TABLE IF EXISTS cdf.Candidate;
CREATE TABLE cdf.Candidate (
id BIGINT DEFAULT nextval('cdf.id_seq') PRIMARY KEY,
ballotname  text ,
election_id INTEGER REFERENCES cdf.election(id),
party_id INTEGER REFERENCES cdf.party(id)
)
;

DROP TABLE IF EXISTS cdf.CandidateSelection;
CREATE TABLE cdf.CandidateSelection (
id BIGINT DEFAULT nextval('cdf.id_seq') PRIMARY KEY,
candidate_id INTEGER REFERENCES cdf.candidate(id)
)
;

DROP TABLE IF EXISTS cdf.VoteCount;
CREATE TABLE cdf.VoteCount (
id BIGINT DEFAULT nextval('cdf.id_seq') PRIMARY KEY,
countitemstatus_id INTEGER REFERENCES cdf.countitemstatus(id),
countitemtype_id INTEGER REFERENCES cdf.countitemtype(id),
othertype  text 
)
;
comment on column cdf.VoteCount.countitemstatus_id is 'CDF lists CountItemStatus as a field in ReportingUnit, not in VoteCount *** seems like an error';


DROP TABLE IF EXISTS cdf.BallotMeasureContest_Selection_Join;
CREATE TABLE cdf.BallotMeasureContest_Selection_Join (
id BIGINT DEFAULT nextval('cdf.id_seq') PRIMARY KEY,
ballotmeasurecontest_id INTEGER REFERENCES cdf.ballotmeasurecontest(id),
ballotmeasureselection_id INTEGER REFERENCES cdf.ballotmeasureselection(id)
)
;

DROP TABLE IF EXISTS cdf.CandidateContest_Selection_Join;
CREATE TABLE cdf.CandidateContest_Selection_Join (
id BIGINT DEFAULT nextval('cdf.id_seq') PRIMARY KEY,
candidatecontest_id INTEGER REFERENCES cdf.candidatecontest(id),
candidateselection_id INTEGER REFERENCES cdf.candidateselection(id)
)
;

