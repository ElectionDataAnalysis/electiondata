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
    
Note that during processing the package uses information from the repository. In other words, the repository contains not only the code necessary to compile the package, but also files called by the package as it functions. So the user will need to know the absolute path to the repository content root `src`. Below we will call this path `<path/to/src>`.

### Contents of `run_time.ini`
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
You may wish to check that these postgresql credentials will work on your system via the command `psql -h localhost -p 5432 -U postgres postgres`. If this command fails, or if it prompts you for a password, you will need to find the correct connection parameters specific to your postgresql instance.  (Note that the `dbname` parameter is arbitrary, and determines only the name of the postgresql database created by the package.)

### Contents of `jurisdiction_prep.ini` (optional)
If you choose to create your Georgia jurisdiction files from scratch (rather than using the ones provided in the repository) you will need to specify various information about Georgia in `jurisdiction_prep.ini`:
```
[electiondata]
name=Georgia
reporting_unit_type=state
abbreviated_name=GA
count_of_state_house_districts=180
count_of_state_senate_districts=56
count_of_us_house_districts=14
```

### Contents of `GA_detail_20201120_1237.xml`
Copy the file of the same name in the repository: [tests/000_data_for_pytest/2020-General/Georgia/GA_detail_20201120_1237.xml](../tests/000_data_for_pytest/2020-General/Georgia/GA_detail_20201120_1237.xml)

## Create an initialization file for the results file (optional)
If you wish to practice creating an initialization file for results, follow the steps below. Otherwise skip to the next section.

Because your `results_directory` folder has a subfolder `Georgia`, the dataloading routines will look to the folder [src/ini_files_for_results/Georgia](src/ini_files_for_results/Georgia) for information about any files in `working_directory/results_directory/Georgia`. 

## Create a munger file (optional)
You can use the munger file 

## Create Georgia jurisdiction files (optional)
If you wish to practice creating jurisdiction files, follow the steps below. Otherwise, skip to the next section.

Because the `results_directory` folder has a subfolder `Georgia`, the dataloading routines will look for information specific to Georgia in the repository folder [src/jurisdictions/Georgia](src/jurisdictions/Georgia). 

1. Delete the folder [src/jurisdictions/Georgia](src/jurisdictions/Georgia).
2. Navigate to the folder `working_directory`.
3. From within `python3.9`:
```
>>> import electiondata as ed
>>> jp = ed.JurisdictionPrepper()
>>> jp.new_juris_files()

Starting new_juris_files
Directory created: <path/to/src>/jurisdictions/Georgia
Starter dictionary created in current directory (not in jurisdiction directory):
GA_starter_dictionary.txt
```
The starter dictionary is placed in the working folder because in our experience it has sometimes been helpful to avoid overwriting an existing `dictionary.txt` file.
4. (Optional) Take a look at the newly created folder [src/jurisdictions/Georgia](src/jurisdictions/Georgia) in the repository and the newly created file `GA_starter_dictionary.txt` in your working directory. 
5. 

