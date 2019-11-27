-- Postgresql 11
--

DROP TABLE IF EXISTS CountItemStatus;
CREATE TABLE CountItemStatus (
id NUMERIC NOT NULL,
text varchar(13),
CONSTRAINT countitemstatus_pk PRIMARY KEY(id)
)
;

INSERT INTO CountItemStatus (id,text)
VALUES
(1,'completed'),
(2,'in-process'),
(3,'not-processed'),
(4,'unknown')
;



DROP TABLE IF EXISTS ReportingUnit;
CREATE TABLE ReportingUnit (

)
;


