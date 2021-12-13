# Results files for dataloading tests
The dataloading tests rely on having some raw results data to load. And the results data should be various enough to test the various components of the data-loading code. In other words, effective testing requires a reasonable variety of input files. The repository does not contain sufficient results data for testing. A test set is available in a separate repository, [***TODO](***TODO). If [test_dataloading_by_ej.py](../tests/dataloading_tests/test_dataloading_by_ej.py) does not find results data, it will default to downloading the files from that repository.

# Sample Testing Session

## Directory and File Structure
 Call the tests from a working directory with the following structure and files:
```
.
+-- input_results
|   +-- Alabama
|   |   + <Alabama results file>
|   |   + <maybe another Alabama results file>
|   +-- Alaska
|   |   + <Alaska results file>
|   +-- American-Samoa
|   |   + <American Samoa results file>
|   +-- <etc>
|
+-- reports_and_plots
+-- run_time.ini
```
The file `run_time.ini` can be the same as in the [Sample Dataloading Session](Sample_Session.md). 

## Note on dataloading tests
The tests in [test_dataloading_by_ej.py](../tests/dataloading_tests/test_dataloading_by_ej.py) will attempt to load all raw results files in `input_results` that are specified by some file in the [`ini_file_for_results` directory](../src/ini_files_for_results). You can check which jurisdictions had files loaded:
 * if the test is successful, look at the `compare_*` directories in the `reports_and_plots` directory.
 * if the test fails, look in the output from the test.

## Running the tests
You will need pytest to be installed on your system (see [pytest installation instructions](https://docs.pytest.org/en/6.2.x/getting-started.html) if necessary). Commands are run from the shell
 * dataloading routines: `pytest ~/PycharmProjects/electiondata/tests/dataloading_tests`
 * jurisdiction prep routines: `pytest ~/PycharmProjects/electiondata/tests/jurisdiction_prepper_tests/`
 * analysis routines: `pytest ~/PycharmProjects/electiondata/tests/analyzer_tests/  `