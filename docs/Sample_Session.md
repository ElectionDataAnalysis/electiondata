# Sample Session

This document walks the reader through a simple example, from setting up project directories, through loading data and performing analyses. We assume that the package has been installed in an environment with all the necessary components (as described in [Installation.md](Installation.md). As an example, we will load the xml results file from Georgia in the repository at [tests/000_data_for_pytest/2020-General/Georgia/GA_detail_20201120_1237.xml](../tests/000_data_for_pytest/2020-General/Georgia/GA_detail_20201120_1237.xml).

## Directory and File Structure
The package offers a fair amount of flexibility in the directory structures used. For this sample session, we assume the user will call the program from a particular working directory with the following structure and files:

* `working_directory`
  * `jurisdiction_prep.ini`
  * `run_time.ini`
  * `results_directory`
    * `Georgia`
      * `GA_detail_20201120_1237.xml`
  * `archive_directory`
  * `reports_and_plots_directory`  
    
Note that during the processing package uses information from the repository. In other words, the repository contains not only the code necessary to for the system to function, but also files called by the system as it functions. The user will need to know the path to the repository content root `src` -- either the absolute path, or the path relative to the working_directory. Below we will call this path `<path/to/src>`.

## Contents of `run_time.ini`
```
[electiondata]
results_dir=results_directory
archive_dir=archive_directory
repository_content_root=<path/to/src>
reports_and_plots_dir=reports_and_plots_directory

[postgresql]
host=localhost
port=5432
dbname=electiondata_Georgia_test
user=postgres
password=
```
You may wish to check that these postgresql credentials will work on your system via the command `psql -h localhost -p 5432 -U postgres postgres`. If this command fails, or if it prompts you for a password, you will need to find out how to connect to your local postgresql instance before proceeding.  


