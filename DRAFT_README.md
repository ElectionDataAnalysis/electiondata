# election_anomaly
_Documentation under construction_

## User Experience




### Unique key over all tables

### Tables
There are several kinds of tables in the database:
* Tables whose name starts with `_` are 'metadata tables. These are not essential to the cdf, but hold relevant data (such as the source of the data in the database). 
  * NB: there can be metadata enumeration tables, such as `_datafile_separator`
* Tables with exactly two columns `Id` and `Txt` (or, in the case of BallotMeasureSelection, `Id` and `Selection`) are 'enumeration tables', holding enumeration lists specified in the CDF.
* Tables with at least one column that is neither `Txt`, nor `Selection` nor a foreign id (ending in `_Id`). These are 'CDF element tables'. They correspond to the elements of the CDF (boxes in the CDF diagram).
* Tables whose names end with `Join` are 'join tables', holding relationships between elements of the CDF (lines in the CDF diagram). 

### Adding elements
Not all elements of the NIST CDF need to be in the database. You may wish to alter the database design to include another element of the CDF. E.g., as of this writing `OfficeGroup` is ignored by the database created by this repository even though it might be useful to certain kinds of analysis. Here's how:
* Inside the folder `src/election_anomaly/CDF_schema_def_info/CDFElements` add a subfolder for your element, whose contents should be analogous to content of existing element folders.

* Create any necessary joins (dictated by relationships between elements in the CDF) inside the folder `src/election_anomaly/CDF_schema_def_info/Joins`
  * NB: the 'foreign keys' in the join files may refer to more than one other table. E.g., the `Contest_Id` in `ElectionContestJoin` may refer to `BallotMeasureContest.Id` or to `CandidateContest.Id`. In this case the `refers_to` field in `foreign_keys.txt` holds a semicolon-separated list. See, e.g., `src/election_anomaly/CDF_schema_def_info/Joins/ElectionContestJoin/foreign_keys.txt`.

[//]: # "TODO give more detail?"

## About the Filesystem

### Context directory
Each file (except for remark.txt and ExternalIdentifiers.txt and Candidate.txt) has a Name column, which must not have duplicate entries. Entries in this column correspond to the Name field in the corresponding database table

PrimaryParty column in CandidateContest.txt is for primary election contests. If a contest is not a primary election contest, this should be null. 

[//]: # "TODO per CDF, there should be a CandidateContestOfficeJoin table"
