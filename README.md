# results_analysis
_Documentation under construction_

General note: the word 'state' includes also the District of Columbia and the five major US territories: Puerto Rico, American Samoa, the US Virgin Islands, the Northern Mariana Islands and Guam.
## Set-up
### Database
You will need access to a postgresql database. Your login credentilals should be in `src/election_anomaly/local_data/database.ini`. Contents of that file should be:

```[postgresql]
host=<url for your postgresql server>
port=<port for your postgresql server>
database=<name of your election data database>
user=<your user name>
password=<your password>
```

### .gitignore
Folders you will need in your local repo:
`src/election_anomaly/local_data` holds your state-by-state data. Each state needs its own directory, e.g., `src/election_anomaly/local_data/NC` for North Carolina. Each state directory has three subfolders:
  * `data` for datafiles from the state
  * `meta` for metadata files from the state
  * `context` for information about the state that cannot be read from the contents of the data and metadata files:
    * `name.txt` the name of the state, e.g., 'North Carolina'
    * `BallotMeasureSelection.txt` Python set, e.g., {'For','Against','Yes','No'}
    * `remark.txt` String containing any notable information about the state and its data
    * `datafile.txt` Python dictionary of datafiles, with attributes:
      * `'encoding'`
      * `'source_url'`
      * `'file_date'`
      * `'download_date'`
      * `'note'`
      * `'correction_query_list'` a list of any corrections needed in response to metadata errors
    * `Election.txt` Python dictionary of elections, with attributes:
      * `'ElectionType'`
      * `'ReportingUnit'`
      * `'StartDate'`
      * `'EndDate'`
      * `'ExternalIdentifiers'`
    * `metafile.txt` Python dictionary of metafiles, with attributes:
      * `'encoding'`
      * `'source_url'`
      * `'file_date'`
      * `'download_date'`
      * `'note'`
      * `'column_block_parser_string'` Python regex to identify the block of lines in the metafile holding the relevant column definitions
      * `'line_parser_string` Python regex to identify the column name, data type and column description within one line of the metafile
      * `'type_map'` Python dictionary whose keys are datatype names from the file and whose values are datatype names for postgresql
    * `Office.txt` Python dictionary of office names with attributes. Note that when datafiles are processed, lines relevant to offices **not** listed here will not be loaded into the common data format schema.
      * `'ElectionDistrict'`
      * `'ElectionDistrictType'` e.g., 'state-house' or 'congressional', following conventions in `CDF_schema_def_info/enumerations/ReportingUnitType.txt`
      * `'ExternalIdentifiers'`
    * `ReportingUnit.txt` Python dictionary of reporting units (usually geographical precincts, counties, etc., but could also be individual machines, adminstrative precincts, etc.), with attributes:
      * `'ReportingUnitType'`
      * `'ExternalIdentifiers'`
     * `Party.txt` Python dictionary of political parties, with attributes:
      * `'ExternalIdentifiers'`
      
### About ExternalIdentifiers
(TODO)
## How to run the app

# Code components

## Docker files

### `src/election_anomaly/requirements.txt`
Specifies necessary python packages that are not part of the standard python distribution




### `src/election_anomaly/local_data` folder
Contains one subfolder for each state, each containing three folders:
* `data` containing data files 
* `meta` containing metadata files for the `data` files
* `context` containing necessary state-specific information from sources other than the data files

Also contains a subfolder `tmp` to hold temporary cleaned files for upload to db

### About the `context` folder
This folder contains contextual information about a particular state. This information may be common to many datafiles; it may be related to information in the datafile but may require some contextual knowledge outside of any particular datafile. For example, the fact that the election on 2018-11-06 in North Carolina was a `general` election is contextual knowledge.

- DB elements that must be loaded from the `context` file (or corresponding attribute of state or datafile) before individual records in datafile are addressed: `election`, `reportingunit` , `party`, `office` 
- DB elements that must be loaded from each record in the datafile: `reportingunit` (type of reporting unit will need to be respected, e.g., `county` or `geoprecinct` or `other`)

### About the `CDF_schema_def_info` folder:
 - Contains folder `enumerations` with the various enumerations from the Common Data Format
 - Contains file `tables.txt` with the python dictionary determining the tables in the postgres common data format schema.

## Naming conventions

### Strings used as names and dictionary keys
Each element (each election, candidate, reporting unit, etc.) has a name -- a character string used in the `name` field in the corresponding database table, and also used in the files in the `context`  folder as keys in python dictionaries containing more info about the element.
