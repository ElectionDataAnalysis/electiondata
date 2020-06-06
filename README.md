# election_anomaly
_Documentation under construction_

# Funding
Funding provided October 2019 - April 2021 by the National Science Foundation
 * Award #1936809, "EAGER: Data Science for Election Verification" 
 * Award #2027089, "RAPID: Election Result Anomaly Detection for 2020"

# License
See [LICENSE.md](./LICENSE.md)

# Overview
This repository hopes to provide reliable tools for consolidation and analysis of raw election results from the most reliable sources -- the election agencies themselves. 
 * Consolidation: can take as input election results files from a wide variety of sources and loads the data into the [common data format](https://github.com/usnistgov/ElectionResultsReporting) from the National Institute of Standards and Technology (NIST)
 * Export: creates tab-separated flat export files of results sets rolled up to any desired intermediate geography (e.g., by county, or by congressional district)
 * Analysis: provides a variety of analysis tools
 
[//]: # "TODO keep this up to date"
 * Visualization: provides a variety of visualization tools.
 
[//]: # "TODO keep this up to date"

# How to run the app
## Loading data
run `src/election_anomaly/main_routines/050_load_datafile.py`

## Pulling election result rollups
run `src/election_anomaly/main_routines/100_pull_top_counts_by_vote_type.py`
or `src/election_anomaly/main_routines/101_pull_top_counts.py`
[//]: # "TODO keep this up to date"


## Environment
### Database
The system runs out of the box with a postgresql database; to use other varieties of SQL, you will need to modify the routines in the `dbr_routines` module. 

Store your login credentials stored in a file. Contents of that file should be:
```
[postgresql]
host=<url for your postgresql server>
port=<port for your postgresql server>
dbname=<name of your election data database>
user=<your user name>
password=<your password>
```
  
### Context information by jurisdiction
Because each original raw results file comes from a particular election agency, and each election agency has a fixed jurisdiction, it is natural to organize information by jurisdiction. 

Each jurisdiction should have its own subdirectory of the directory `src/jurisdictions`.


## Mungers
Election result data comes in a variety of file formats. Even when the basic format is the same, file columns may have different interpretations. The code is built to ease -- as much as possible -- the chore of processing and interpreting each format. Following the [Jargon File](http://catb.org/jargon/html/M/munge.html), which gives one meaning of "munge" as "modify data in some way the speaker doesn't need to go into right now or cannot describe succinctly," we call each set of basic information about interpreting an election result file a "munger". The munger template is in the directory `src/templates/munger_templates`.

The directory `src/mungers` holds the munger directories. Each munger directory needs the following component files:
 * `format.txt` holds information about the file format
   * `count_columns` is a comma-separated list of integers identifying the columns that contain vote counts. Our convention is to count from the left of the file, with leftmost column as 0.
   * `header_row_count` is an integer, the number of header rows in the file
   * `field_name_row` is the single header row containing field names for columns that do not hold counts. (Columns containing vote counts may have field value information in more than one header row, e.g., one header row for contest and a second header row for candidate). Our convention is to count from the top of the file, with the top row as 0.
   * `file_type` is one of a certain list of types recognized by the system. As of 6/4/2020, the list is:
     * `txt` for tab-separated text
     * `csv` for comma-separated text
     * `xls` or `xlsx` for Excel files (and any other files readable by the `read_excel` function in the `pandas` package.
   * `encoding` is the file encoding, e.g., `iso-8859-1`.
   
  * `cdf_tables.txt` One line for each main class in the Common Data Format. Specifies how to read the values for that table from the source file. Columns are:
    * `name` Name of the Common Data Format class (e.g., 'ReportingUnit')
    * `raw_identifier_formula` Formula for creating the raw identifier from the results file. 
    * `source` Identifies the placement in the file of the relevant information. The system recognizes these possibilities:
      * `row` for classes calculated from values in same row as a given vote count value. In this case the `raw_identifier_formula` can reference entries within the row via the relevant column name in angle brackets (e.g., <COUNTY>)
      * `column` for classes calculated from values in the same column as a given vote count value. In this case the `raw_identifier_formula can reference entries within the column via the relevant row number in angle brackets (e.g., <0>)
      * `other` for classes that are the same for the whole results file. In this case the `raw_identifier_formula` should be blank.
    
### row-sourced formula example
Consider this snippet from a Philadelphia, Pennsylvania voting results file:
```
WARD,DIVISION,VOTE TYPE,CATEGORY,SELECTION,PARTY,VOTE COUNT
01,01,A,JUDGE OF THE SUPERIOR COURT,AMANDA GREEN-HAWKINS,DEMOCRATIC,2
01,01,M,JUDGE OF THE SUPERIOR COURT,AMANDA GREEN-HAWKINS,DEMOCRATIC,146
01,01,P,JUDGE OF THE SUPERIOR COURT,AMANDA GREEN-HAWKINS,DEMOCRATIC,0
```
The formula `Ward <WARD>;Division <DIVISION>` would yield `Ward 01;Division 01`.

# Code components

### About the `CDF_schema_def_info` folder:
 - Contains folder `enumerations` with the various enumerations from the Common Data Format
 - Contains file `tables.txt` with the python dictionary determining the tables in the postgres common data format schema.


### Strings used as names and dictionary keys
Each element (each election, candidate, reporting unit, etc.) has a name -- a character string used in the `name` field in the corresponding database table, and also used in the files in the `context`  folder as keys in python dictionaries containing more info about the element. 

For ReportingUnits, the naming convention is to list as much of the composing information as possible in the name of the element, using `;` as a separator. E.g., 
 * `North Carolina` -- the state of NC
 * `North Carolina;Alamance County` -- Alamance County, which is contained in North Carolina
 * `North Carolina;Alamance County;Precinct 12W` -- Precinct 12W in Alamance County

# Conventions
Reporting units that are not physical geographical precincts (such as an administrative precinct containing absentee ballot counts from one county) are classified as Reporting Type `precinct` if they should be part of any roll-up involving all precincts in a jurisdiction (e.g., if the absentee ballot counts are not included in any other Reporting Units of Type `precinct` in the county).

Nesting of Reporting Units is coded by semicolons in the name, e.g., `Pennsylvania;Philadelphia;Ward 8;Division 16` is contained in `Pennsylvania;Philadelphia;Ward 8`, which is contained in `Pennsylvania;Philadelphia`, which is contained in `Pennsylvania`. The semicolons are used by the code to roll up results from smaller Reporting Units into larger Reporting Units.

# Data Import Process
## New state
## New munger
A new datafile may have a new column (e.g., for a new vote type), in which case a new munger is needed
## New datafile
Even if the munger is essentially unchanged, each new datafile may have new ReportingUnits or Parties. ***

A new datafile may have new names for existing elements (such as Reporting Units or Offices) ***

More rarely, a new datafile may have new Offices and CandidateContests.

