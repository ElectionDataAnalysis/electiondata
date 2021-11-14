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
If you wish to practice creating an initialization file for results, follow the steps below. Otherwise skip to the next section; in this case the system will use the initialization file [ga20g_20201120_1237.ini](../src/ini_files_for_results/Georgia/ga20g_20201120_1237.ini).

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
5. Follow the instructions in the "Create or Improve a Jurisdiction" section of the [User_Guide](User_Guide.md). As you work through them, the following examples may be helpful.
  * Sample county lines in `Reporting.txt`:
```
Georgia;Appling County	county
Georgia;Atkinson County	county
Georgia;Bacon County	county
```
  * Sample county lines in `GA_starter_dictionary.txt` (or `dictionary.txt`):
```
ReportingUnit	Georgia;Appling County	Appling
ReportingUnit	Georgia;Atkinson County	Atkinson
ReportingUnit	Georgia;Bacon County	Bacon
```

4. Try loading your results!
```
>>> ed.load_or_reload_all()
```
Errors and warnings are par for the course, so don't be discouraged if the result is something like this:
```
Jurisdiction Georgia did not load. See .error file.
Jurisdiction errors written to reports_and_plots_directory/load_or_reload_all_1113_0757/_jurisdiction_Georgia.errors
>>> 
```
Take a look at the file(s) indicated, which should give you a good idea of what needs to be fixed. (If it doesn't please report the unhelpful message(s) and your question as an [issue on GitHub](https://github.com/ElectionDataAnalysis/electiondata/issues)). Repeat this step -- loading and looking at errors -- as often as necessary! (In rare cases, you may want to start over with a new database, either by erasing the existing `electiondata_Georgia_test` from your postgres instance, or changing the value of `dbname` in the `run_time.ini` file.)

### Sample errors and warnings while building jurisdiction files (optional)
#### Contests
If no contests were recognized, or no candidates were recognized, the system reports an error:
```
Jurisdiction errors (Georgia):
Could not add totals for 2020 General because no data was found
No contest-selection pairs recognized via munger my_Georgia_test.munger
```
Focus one contest, and one candidate in that contest. Look in the `.errors` and `.warnings` files. If the name of the contest or the candidate appears, the file will tell you what went wrong. If the name of the contest or the candidate does not appear in the `.errors` or `.munger` file, then there is an issue with the munger named in the results initialization file.

Contests that were parsed from the file but not recognized in the dictionary will be listed in a `.warnings` file, e.g.:
```
CandidateContests (found with munger my_Georgia_test) not found in dictionary.txt :
US Senate (Perdue)
US Senate (Loeffler) - Special
Statewide Referendum A
State Senate District 39 - Special Democratic Primary
State Senate Dist 9/Senador Estatal Dist 9
```
You may very well choose to omit certain contests (such as Statewide Referendum A and the Special Democratic Primary). For the other contests, you will need to make an entry in `dictionary.txt` -- and maybe `CandidateContest.txt` and `Office.txt` as well.
    * 'State Senate Dist 9/Senador Estatal Dist 9': if you see this after following the instructions in this sample session, the corresponding Office and CandidateContest 'GA Senate District 9' should already exist, so simply add another line to the `dictionary.txt` file: `CandidateContest	GA Senate District 9	State Senate Dist 9/Senador Estatal Dist 9`. (It may save time to add this version of all the GA Senate districts at this point.)
    * 'US Senate (Perdue)' and 'US Senate (Loeffler) - Special': Be sure to disambiguate these, either by creating two separate Offices and corresponding CandidateContests for the two US Senate positions, or by creating two separate CandidateContests corresponding to the single office 'US Senate GA'. We choose the latter, adding a row to  `CandidateContest.txt` (`US Senate GA (partial term)	1	US Senate GA	`) and two rows to `dictionary.txt`:
```
CandidateContest	US Senate GA (partial term)	US Senate (Loeffler) - Special
CandidateContest	GA Attorney General	GA Attorney General
```

#### Candidates
Candidates not found in the dictionary will be listed in a `.warnings` file. Copy and paste these names into the `Candidate.txt` file as a single BallotName column:
```
BallotName
Zulma Lopez
Zachary Perry
Yasmin Neal
```
and add corresponding rows to `dictionary.txt`:
```
Candidate	Zulma Lopez	Zulma Lopez
Candidate	Zachary Perry	Zachary Perry
Candidate	Yasmin Neal	Yasmin Neal
```
You are free to choose another convention for the candidate names in the database, as long as you specify the mapping in the dictionary. E.g., you could instead have
```
BallotName
Lopez, Zulma
Perry, Zachary
Neal, Yasmin
```
if your dictionary has
```
Candidate	Lopez, Zulma	Zulma Lopez
Candidate	Perry, Zachary	Zachary Perry
Candidate	Neal, Yasmin	Yasmin Neal
```

#### Parties
Parties not recognized will also be listed in the `.warnings` file, e.g.:
```Partys (found with munger my_Georgia_test.munger) not found in dictionary.txt :
Rep
Lib
Ind
Grn
Dem
```
All these should be mapped in `dictionary.txt`
```
Party	Republican Party	Rep
Party	Libertarian Party	Lib
Party	Independent Party	Ind
Party	Green Party	Grn
Party	Democratic Party	Dem
```
to your chosen internal database names which should be in the name column of `Party.txt`:
```
Name
Democratic Party
Republican Party
Libertarian Party
Green Party
Independent Party
```
Note: Some of the routines in the `analyze` submodule assume that every party name ends with ' Party'.

## Loading Data

## Export results

## Draw graphs

## Clean up
You may want to restore the repository to its original state 
TODO how?