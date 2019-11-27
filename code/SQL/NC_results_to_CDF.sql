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




DROP TABLE IF EXISTS ReportingUnit;
CREATE TABLE ReportingUnit (

)
;


