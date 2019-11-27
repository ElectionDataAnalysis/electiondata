-- Postgresql 11
--

DROP TABLE IF EXISTS CountItemStatus;
CREATE TABLE CountItemStatus (
id SERIAL,
text varchar(13),
CONSTRAINT countitemstatus_pk PRIMARY KEY(id)
)
;

COPY CountItemStatus (text) FROM '/container_root_dir/SQL/enumerations/CountItemStatus.txt';

DROP TABLE IF EXISTS ReportingUnitType;
CREATE TABLE ReportingUnitType (
id SERIAL,
text varchar(20),
CONSTRAINT ReportingUnitType_pk PRIMARY KEY(id)
)
;

COPY ReportingUnitType (text) FROM '/container_root_dir/SQL/enumerations/ReportingUnitType.txt';

DROP TABLE IF EXISTS ReportingUnit;
CREATE TABLE ReportingUnit (

)
;

DROP TABLE IF EXISTS Party;
CREATE TABLE Party (
id SERIAL,
abbreviation varchar(6),
name varchar(50),
CONSTRAINT Party_pk PRIMARY KEY (id)
)
;

-- BTRIM removes both leading and trailing spaces from a string
INSERT INTO Party (abbreviation) SELECT DISTINCT BTRIM(choice_party) FROM results_pct WHERE choice_party IS NOT NULL AND BTRIM(results_pct.choice_party) <> '';
