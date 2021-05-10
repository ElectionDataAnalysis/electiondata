# About the Code 

## Documentation in Progress! Proceed with caution!

## Code components
### About the `CDF_schema_def_info` directory:
The information in this directory determines the structure of the database created by the system to store election results information. Subdirectories and their contents are:
 * `elements` subdirectory contains a subdirectory for each main tables in the database. Most of these correspond to classes in the Common Data Format; other tables (e.g., `_datafile`) start with an underscore. 
 * `enumerations` subdirectory contains a file for each relevant enumerated list from by the Common Data Format. We treat `BallotMeasureSelection` as an enumerated list.
 * `joins` subdirectory contains a subdirectory for each join table in the database.
 
### Some hard-coded items
 Some lists are hard-coded in one place, so could be changed.
  * Recognized jurisdictions `states_and_such` in `database/__init__.py`. Anything not on this list will not appear in the output of `database.display_jurisdictions()`, even if there is corresponding data in the database.
  * Recognized contest types `contest_types_model` in `database/__init__.py`.
  * Recognized encodings `recognized_encodings` in `user_interface/__init__.py`
  * Ballot measure selections `bmselections` in `database/create_cdf_db/__init__.py`
  
### Conventions
 * Some jurisdiction names (e.g., "District of Columbia") contain spaces, while it is inconvenient to use spaces in directory and file names. So we distinguish between a jurisdiction's "true name" and its "system name", which replaces all spaces by hyphens (e.g., "District-of-Columbia").
  
  

