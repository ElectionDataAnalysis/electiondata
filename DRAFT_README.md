# election_anomaly
_Documentation under construction_

## User Experience

### Design Decisions
There are some options for segregating data:
* Separate DB for each state 
* One DB for all results

The Common Data Format does not have an obviously natural way to handle the case of multiple updates to results files during a single canvass in a single jurisdiction. Some options are:
* Add a timestamp field to the ReportingUnit table. Note that this is outside the CDF (as of March 2020) and could break things. 
* Treat each update as a separate Election (possibly co-opting the StartDate or EndDate field to become a timestamp, or putting a timestamp in the Election.Name)

### Create New Database
* User provides:
  * name of database
* System: 
  * creates database as specified in CDF_schema_def_info

### Create Munger

### Load data from a datafile into an existing database
* User uploads datafile, providing:
  * name of database
  * choice to analyze Candidate Contests only, Ballot Measure Contests only, or both
* User picks munger, providing:
  * name of munger
  * atomic reporting unit type represented in datafile
* System:

## About the database
There are several kinds of tables in the database:
* Tables whose name starts with `_` are 'metadata tables. These are not essential to the cdf, but hold relevant data (such as the source of the data in the database). 
* Tables with exactly two columns `Id` and `Txt` are 'enumeration tables', holding enumeration lists specified in the CDF.
* Tables with at least one column that is neither `Txt` nor a foreign id (ending in `_Id`). These are `CDF element tables`. They correspond to the elements of the CDF (boxes in the CDF diagram).
* Tables whose names end with `Join` are 'join tables', holding relationships between elements of the CDF (lines in the CDF diagram).