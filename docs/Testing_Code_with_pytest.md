# Results files for dataloading tests
The dataloading tests rely on having some raw results data to load. And the results data should be various enough to test the various components of the data-loading code. In other words, effective testing requires a reasonable variety of input files. The repository does not contain sufficient results data for testing. A test set is available in a separate repository, [***TODO](***TODO). If [test_dataloading_by_ej.py](../tests/dataloading_tests/test_dataloading_by_ej.py) does not find results data, it will default to downloading the files from that repository.

# Sample Testing Session

## Directory and File Structure
 Call the tests from a working directory with the following structure and files:
```
.
+-- input_directory
|   +-- Alabama
|   |   + <Alabama results file>
|   |   + <maybe another Alabama results file>
|   +-- Alaska
|   |   + <Alaska results file>
|   +-- American-Samoa
|   |   + <American Samoa results file>
|   +-- <etc>
|
+-- reports_and_plots_directory
+-- run_time.ini
```
where the contents of run_time.ini are:
```
[electiondata]
results_dir=input_directory
archive_dir=archive_directory
repository_content_root=<path/to/src>
reports_and_plots_dir=reports_and_plots_directory

[postgresql]
host=localhost
port=5432
dbname=pytest
user=postgres
password=
```
 