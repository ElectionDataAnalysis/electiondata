# About the Code 

## Documentation in Progress! Proceed with caution!

## Code components
### About the `CDF_schema_def_info` directory:
The information in this directory determines the structure of the database created by the system to store election results information. Subdirectories and their contents are:
 * `elements` subdirectory contains a subdirectory for each main tables in the database. Most of these correspond to classes in the Common Data Format; other tables (e.g., `_datafile`) start with an underscore. 
 * `enumerations` subdirectory contains a file for each relevant enumerated list from by the Common Data Format. We treat `BallotMeasureSelection` as an enumerated list.
 * `joins` subdirectory contains a subdirectory for each join table in the database.

