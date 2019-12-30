# results_analysis
Documentation under construction

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
  * `context` for information about the state that cannot be read from the contents of the data and metadata files
    * `BallotMeasureSelection.txt` 


## How to run the app

`

## docs folder
The `docs` folder has documentation for local set-up of programs and packages that are likely to be useful for dev.

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
