# results_analysis
_Documentation under construction_

#Funding
Funding provided October 2019 - September 2020 by the National Science Foundation, Award #1936809, "EAGER: Data Science for Election Verification" 

#License
See [LICENSE.md](./LICENSE.md)

#General note
 The word 'state' includes also the District of Columbia and the five major US territories: Puerto Rico, American Samoa, the US Virgin Islands, the Northern Mariana Islands and Guam.

## How to run the app
The app is controlled by the Python3 module src.election_anomaly

## Environment
### Database
You will need access to a postgresql database. Your login credentilals should be in `src/local_data/database.ini`. Contents of that file should be:

```[postgresql]
host=<url for your postgresql server>
port=<port for your postgresql server>
database=<name of your election data database>
user=<your user name>
password=<your password>
```

Your database should have a schema `misspellings` and table `corrections` to hold corrections of misspellings. You can create this with postgresql code:
```angular2
CREATE SCHEMA misspellings
CREATE TABLE misspellings.corrections (id SERIAL PRIMARY KEY, bad TEXT UNIQUE NOT NULL, good TEXT NOT NULL)
```

### .gitignore
Folders you will need in your local repo:
`src/local_data` holds your state-by-state data. 
 * Each state needs its own directory, e.g., `src/local_data/NC` for North Carolina. 
 * There should also be a directory `src/local_data/tmp` here to hold temporary files created during processing 

Each state directory has three subfolders:
  * `data` for datafiles from the state
  * `meta` for metadata files from the state
  * `context` for information about the state that cannot be read from the contents of the data and metadata files. This information may be common to many datafiles; it may be related to information in the datafile but may require some contextual knowledge outside of any particular datafile. For example, the fact that the election on 2018-11-06 in North Carolina was a `general` election is contextual knowledge.

    * `name.txt` the name of the state, e.g., 'North Carolina'
    * `schema_name.txt` the name of the schema to hold the state's raw data
    * `BallotMeasureSelection.txt` Header is `Selection`; one row for each possible selection, e.g., `Yes` or `No` or `For` or `Against`. 
    * `remark.txt` String containing any notable information about the state and its data
    * `datafile.txt` Tab-separated list of datafiles, with attributes:
      * `name`
      * `encoding`
      * `source_url`
      * `file_date`
      * `download_date`
      * `note`
      * `correction_query_list` a list of any corrections needed in response to metadata errors
    * `Election.txt` Tab-separated list of elections. Columns are:
      * `Name`
      * `ElectionType`
      * `ReportingUnit` Parent Reporting Units (e.g., 'North Carolina' must precede children (e.g., 'North Carolina;Alamance County'))
      * `StartDate`
      * `EndDate`
    * `metafile.txt` Tab-separated list of metafiles. Columns are:
      * `name`
      * `encoding`
      * `source_url`
      * `file_date`
      * `download_date`
      * `note`
      * `column_block_parser_string` Python regex to identify the block of lines in the metafile holding the relevant column definitions
      * `line_parser_string` Python regex to identify the column name, data type and column description within one line of the metafile
      * `type_map` Python dictionary whose keys are datatype names from the file and whose values are datatype names for postgresql
    * `Office.txt` Tab-separated list of office names. Note that when datafiles are processed, lines relevant to offices **not** listed here will not be loaded into the common data format schema. Columns are:
      * `Name`
      * `ElectionDistrict`
      * `ElectionDistrictType` e.g., 'state-house' or 'congressional', following conventions in `CDF_schema_def_info/enumerations/ReportingUnitType.txt`
    * `ReportingUnit.txt` Tab-separated list of reporting units (usually geographical precincts, counties, etc., but could also be individual machines, adminstrative precincts, etc.). Columns are:
      * `Name`
      * `ReportingUnitType`
     * `Party.txt` List of political parties, one per line. One column:
       * `Name`

      
### About ExternalIdentifiers
(TODO)

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