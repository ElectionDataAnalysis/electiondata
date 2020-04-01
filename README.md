# election_anomaly
_Documentation under construction_

# Funding
Funding provided October 2019 - September 2020 by the National Science Foundation, Award #1936809, "EAGER: Data Science for Election Verification" 

# License
See [LICENSE.md](./LICENSE.md)

#General note
 The word 'state' includes also the District of Columbia and the five major US territories: Puerto Rico, American Samoa, the US Virgin Islands, the Northern Mariana Islands and Guam.

## Nota bene
 * leading and trailing whitespace is stripped from values in each datafile
 * munger & munger v. datafile review is still rough. When in doubt, start over
 * in `cdf_tables.txt` use same formula for `Office` and `CandidateContest` 
## How to run the app
***

## Environment
### Database
You will need access to a postgresql database, with your login credentials stored in a file. Contents of that file should be:

```
[postgresql]
host=<url for your postgresql server>
port=<port for your postgresql server>
dbname=<name of your election data database>
user=<your user name>
password=<your password>
```


### .gitignore
Folders you will need in your local repo:
`src/jurisdictions` holds your jurisdiction-specific data. 
 * Each jurisdiction needs its own directory, e.g., `src/jurisdictions/NC` for North Carolina. 

## State-specific information
Each state directory has three required subfolders.

  
###`context` for information about the state that cannot be read from the contents of the data and metadata files. 
This information may be common to many datafiles; it may be related to information in the datafile but may require some contextual knowledge outside of any particular datafile. For example, the fact that the election on 2018-11-06 in North Carolina was a `general` election is contextual knowledge. Each file in the `context` directory should have a single header row.

    * `BallotMeasureSelection.txt` Header is `Selection`; one row for each possible selection, e.g., `Yes` or `No` or `For` or `Against`. 
    * `datafile.txt` describes all datafiles for the state. Columns are:
      * `name`	
      * `encoding`	
      * `separator` ('comma' or 'tab')
      * `source_url`
      * `file_date` date given for the file at the source
      * `download_date`	
      * `note`	

    * `remark.txt` String containing any notable information about the state and its data
    * `Election.txt` Tab-separated list of elections. Columns are:
      * `Name`
      * `ElectionType`
      * `ShortName`    Name that will be used for directories, etc.
      * `ReportingUnit` Parent Reporting Units (e.g., 'North Carolina' must precede children (e.g., 'North Carolina;Alamance County'))
      * `StartDate`
      * `EndDate`
    * `Office.txt` Tab-separated list of office names. Note that when datafiles are processed, lines relevant to offices **not** listed here will not be loaded into the common data format schema. Note that party nominees for office contests are treated as offices; i.e., 'US Senate primary; Republican Party' is an Office. Columns are:
      * `Name`
      * `ElectionDistrict`
      * `ElectionDistrictType` e.g., 'state-house' or 'congressional', following conventions in `CDF_schema_def_info/enumerations/ReportingUnitType.txt`
    * `ReportingUnit.txt` Tab-separated list of reporting units (usually geographical precincts, counties, etc., but could also be individual machines, adminstrative precincts, etc.). Columns are:
      * `Name`
      * `ReportingUnitType`
     * `Party.txt` List of political parties, one per line. One column:
       * `Name`

### About mungers
Files from different sources require different processing assumptions. We call each set of assumptions a "munger"

The folder `src/mungers` holds a directory for each munger. Each munger directory needs the following component files:
 * `ExternalIdentifiers.txt` [*** explain]
 * `atomic_reporting_unit_type.txt` contains one line, holding the type of the basic ("atomic") reporting unit type of the datafile. If the datafile rows correspond to precincts, then 'precinct'. If each row represents information for an entire county, then 'county'. Note that in either case there may be "administrative precincts" to handle, e.g., absentee ballots. Because the program sums over all appropriate elements of the given 'atomic' type, it is important that each vote is counted in one and only one atomic reporting unit.
 * `ballot_measure_count_column_selections.txt`
    Necessary only for mungers with ballot measure yes and no votes in separate columns. Columns are:
    * `fieldname`
    *  `selection` (Must be "Yes" or "No")
 * `raw_columns.txt` List of columns in source file. Columns are:
    * `Name` name of the column in the source file
    * `Datatype` datatype of the column in the source file
 * `count_columns.txt` List of columns in source file corresponding to vote counts
    * `RawName` Name of the count column in the source file
    * `CountItemType` Type of count, according to the Common Data Format (e.g., 'absentee' or 'election-day'). If the CountItemType is determined by a value or values in the row of the raw file (say, from a "Vote Type" column), this field should contain a formula for creating the raw identifier of the CountItemType, and there should be corresponding rows of the raw_identifiers.txt table.
 * `cdf_tables.txt` One line for each main table in the Common Data Format. Specifies how to read the values for that table from the source file.
    * `CDF_Element` Name of the Common Data Format table/element (e.g., 'ReportingUnit')
    * `ExternalIdentifier` Formula for creating the raw identifier from the source file. 
    * `InternalFieldName` Usually 'Name', this is the column in the Common Data Format table that names the item.
    * `Enumerations` Formulas for specifying any enumerated values
    * `OtherFields` Formulas for specifying any other fields. `ids_d` is used in the code to refer to internal Common Data Format primary keys.


### About raw identifiers
(TODO)
Need Office and  CandidateContest separately for each munger. 

### About formulas
When creating a munger, you will need to create formulas for creating raw identifiers from rows. Use angle brackets <> to enclose field values. E.g. consider this snippet from a Philadelphia voting results file:
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