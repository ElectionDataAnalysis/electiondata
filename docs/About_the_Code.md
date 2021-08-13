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
  * Recognized encodings `recognized_encodings` in `userinterface/__init__.py`
  * Ballot measure selections `bmselections` in `database/create_cdf_db/__init__.py`
  
### Conventions
 * Some jurisdiction names (e.g., "District of Columbia") contain spaces, while it is inconvenient to use spaces in directory and file names. So we distinguish between a jurisdiction's "true name" and its "system name", which replaces all spaces by hyphens (e.g., "District-of-Columbia").


### Testing
There are `pytest` routines to test the dataloading, analyzer and jurisdiction-prepping functions. The data required to test the latter two is in the [`000_data_for_pytest`](../tests/000_data_for_pytest) folder. These depend on a file `tests/run_time.ini`. Note that the run_time.ini file is *not* part of the repository, as it contains database connection information. You will need to create it yourself. Or you can point to a different parameter file with the custom option `--param_file` for  pytest.

The [dataloader test](../tests/dataloading_tests/test_dataloading.py) depends on information outside the [tests folder] (../tests):
 - election results files in the results directory specified in by the `results_dir` parameter in `dataloading_tests/run_time.ini`. 
    - if the results directory does not exist, the test will create it and pull files from [`https://github.com/ElectionDataAnalysis/TestingData.git`](https://github.com/ElectionDataAnalysis/TestingData.git). You can specify a different url with the custom pytest option `--test_data_url`
 - reference files in the [`reference_results` folder](../src/reference_results). By convention, these are tabs-separated and named for the jurisdiction, e.g., `Virgina.tsv` or `American-Samoa.tsv`. Note the hyphens. If there are no reference results for a given election-jurisdiction pair, the test will fail. The reference files must have columns `Jurisdiction,Election,Contest,ReportingUnit,VoteType,Count`. 

Note that the `analyzer_tests` and `dataloader_tests` directories each have a `conftest.py` file. This may cause a problem if you try to run them simultaneously via `pytest` from the `test` directory. Running them separately works:
```
tests % pytest analyzer_tests
tests % pytest dataloader_tests
```




  
  

