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
Copy the file of the same name in the repository: 000_template.mungerGA_detail_20201120_1237.xml](../tests/000_data_for_pytest/2020-General/Georgia/GA_detail_20201120_1237.xml)

## Create a munger file (optional)
If you wish to practice creating  munger file, follow the steps below. Otherwise skip to the next section.

Your munger file should live in the folder [src/mungers](../src/mungers), and its name needs to have the extension `.munger`. 

1. (Optional) Delete the munger file [ga_xml.munger](../src/mungers/ga_xml.munger) from your local copy of the repository. This step is not strictly necessary, but will help ensure that you don't accidentally use the existing munger.
1. Make a copy of [src/mungers/000_template.munger](../src/mungers/000_template.munger) named `my_Georgia_test.munger`, inside the same folder.  
2. Fill in required parameter values to specify the organization of the results file `000_template.munger`
  * `file_type=xml` indicates that the file is in xml format.
  * `count_location=ElectionResult/Contest/Choice/VoteType/County.votes` indicates where the vote counts are to be found within the xml nesting structure.
  *  Specify the location of the info defining each vote count. Each location must start with one of the nodes in `count_location`
    * Because the file contains a variety of contests, candidates, parties, vote types (a.k.a. CountItemTypes) and geographies (a.k.a. ReportingUnits), there is no need to use the `constant_over_file` parameter.
    * In the `[munge formulas]` section, specify where the other information is found. While the ReportingUnit, CandidateContest and CountItemType can be read simply from quoted strings in the file, (`ReportingUnit` is in `County.name`, `CandidateContest` is in `Contest.text` and `CountItemType` is in `VoteType.name`), the `Candidate` and `Party` must both be read out of `Choice.text` with python's regular expression ("regex") syntax. In the package syntax, the location of the string in the file is enclosed in angle brackets `<>`, and if a regular expression is needed, the location and the regular expression are given as a pair within braces `{}`.
     
```
[format]
file_type=xml
count_location=ElectionResult/Contest/Choice/VoteType/County.votes

[munge formulas]
ReportingUnit=<County.name>
Party={<Choice.text>,^.* \((.*)\)$}
CandidateContest=<Contest.text>
Candidate={<Choice.text>,^(.*) \(.*\)$}
CountItemType=<VoteType.name>
```

## Create an initialization file for the results file (optional)
If you wish to practice creating an initialization file for results, follow the steps below. Otherwise skip to the next section.

Because your `results_directory` folder has a subfolder `Georgia`, the dataloading routines will look to the folder [src/ini_files_for_results/Georgia](../src/ini_files_for_results/Georgia) for information about any results files in `working_directory/results_directory/Georgia`. 

1. Delete [ga20g_20201120_1237.ini](../src/ini_files_for_results/Georgia/ga20g_20201120_1237.ini) from the repository.
2. Copy [src/ini_files_for_results/single_election_jurisdiction_template.ini](../src/ini_files_for_results/single_election_jurisdiction_template.ini) to a file with extension `.ini` in the folder [src/ini_files_for_results/Georgia](../src/ini_files_for_results/Georgia).
3. Define the parameters in the `[election_results]` section. 
  * If you did not create a new munger file, use `munger_list=ga_xml` instead of `munger_list=my_Georgia_test`
  * Use the actual download date instead of `2021-11-09`
```
[election_results]
results_file=Georgia/GA_detail_20201120_1237.xml
munger_list=my_Georgia_test
jurisdiction=Georgia
election=2020 General
results_short_name=any_alphanumeric_string
results_download_date=2021-11-09
results_source=electiondata repository
results_note=
is_preliminary=False
```

## Create Georgia jurisdiction files (optional)
If you wish to practice creating jurisdiction files, follow the steps below. Otherwise, skip to the next section.

Because the `results_directory` folder has a subfolder `Georgia`, the dataloading routines will look for information specific to Georgia in the repository folder [src/jurisdictions/Georgia](../src/jurisdictions/Georgia). 

1. Delete the folder [src/jurisdictions/Georgia](../src/jurisdictions/Georgia).
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

## Clean up
You may want to restore the repository to its original state 
TODO how?