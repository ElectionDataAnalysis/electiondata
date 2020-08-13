# How to Use the System

## Installation
From the root folder of your repository run `python3 setup.py install` (or if `python` is an alias for `python3` on your system, `python setup.py install`).

## Parameter Files
In the directory from which you will run the system -- which can be outside your local repository-- create the main parameter files you'll need:
* `analyze.par` for Analyzer class if you want it
* `jurisdiction_prep.par` for the JurisdictionPrepper() class
* `multi.par` for loading data. 
  
There are templates in `templates/parameter_file_templates`. Make sure that `analyze.par` and `multi.par` are consistent:
   * `db_paramfile` the same in both
   * `db_name` the same in both
   
In the directory indicated in `multi.par`, create a `.par` file for each results file you want to use. The results files and the `.par` files must both be in that directory. Follow the `templates/parameter_file_templates/results.par` template for the individual `.par` files.
 
## Choose a Munger
Ensure that the munger files are appropriate for your results file(s). 
1. If the munger doesn't already exist, pick a name for your munger and create a folder with that name in the `mungers` directory to hold `format.txt` and `cdf_elements.txt`.
2. Put the appropriate parameters in `format.txt`, following the template in `templates/munger_templates`. For example:

```item	value
header_row_count	0
field_name_row	None
field_names_if_no_field_name_row	County Code,County Name,Election Number,Election Date,Election Name,Unique Precinct Identifier,Precinct Polling Location,Total Registered Voters,Total Registered Republicans,Total Registered Democrats,Total Registered All Other Parties,Contest Name,District,Contest Code,Candidate etc,Candidate Party,Candidate Florida Voter Registration System ID Number,DOE Assigned Number,Vote Total
count_columns	18
file_type	txt
encoding	iso-8859-1
thousands_separator	None
```

3. Put formulas for reading information from the results file into `cdf_elements.txt`. You can reference field names from your file by putting them in angle brackets. E.g., 

```name	raw_identifier_formula	source
ReportingUnit	<County Name>;Precinct <Unique Precinct Identifier>	row
Party	<Candidate Party>	row
CandidateContest	<Contest Name> District <District>	row
Candidate	<Candidate etc>	row
BallotMeasureContest	<Contest Name>	row
BallotMeasureSelection	<DOE Assigned Number>	row
CountItemType	total	row
```
NB: for constants (like the CountItemType 'total' in this example), use 'row' for the source.

## Create or Improve a Jurisdiction
It's easiest to use the JurisdictionPrepper() object to create or update jurisdiction files. 

1. From the directory containing `jurisdiction_prep.par`, open a python interpreter. Import the package and initialize a JurisdictionPrepper(), e.g.:
```
>>> import election_anomaly as ea
>>> jp = ea.JurisdictionPrepper()
```
3. Call new_juris_files(), which will create the necessary files in the jurisdiction directory, as well as a starter dictionary file (`XX_starter_dictionary.txt`) in the current directory.
```
>>> err = jp.new_juris_files()
```
The program will create the necessary files in a folder (at the location `jurisdiction_path` specified in `jurisdiction_prep.par`), and a 'starter dictionary.txt' in your current directory. If something does not work as expected, check the value of `jp.new_juris_files()`, which may contain some helpful information. If the system found no errors, this value will be an empty python dictionary.
```
>>> err
{}
```
3. Add all counties to the `ReportingUnit.txt` file. You must obey the semicolon convention so that the system will know that the counties are subunits of the jurisdiction. For example:
```
Name	ReportingUnitType
Florida;Alachua County	county
Florida;Baker County	county
Florida;Bay County	county
Florida;Bradford County	county
Florida;Brevard County	county
Florida;Broward County	county
```
Currently counties must be added by hand. (NB: in some states, the word 'county' is not used. For instance, Louisiana's major subdivisions are called 'parish'.)
4. Make any necessary changes to  `Office.txt`.
 * Ensure the jurisdiction-wide offices are correct, with the jurisdiction listed as the `ElectionDistrict`. The offices added by `new_juris_files()` are quite generic. For instance, your jurisdiction may have a 'Chief Financial Officer' rather than an 'Treasurer'. Use the jurisdiction's official titles, from an official government source. Jurisdiction-level offices should be prefaced with the two-letter postal abbreviation. For example:
 ```
Name	ElectionDistrict
US President (FL)	Florida
FL Governor	Florida
US Senate FL	Florida
FL Attorney General	Florida
FL Chief Financial Officer	Florida
FL Commissioner of Agriculture	Florida
```

Note that some judicial elections are retention elections, which are handled as BallotMeasureContests, not CandidateContests.
 * Add any other Offices of interest, with their ElectionDistricts. Any ElectionDistricts that are not already in `ReportingUnit.txt` must be added to that file. NB: If you want to add Offices in bulk from a results file, you can wait and do it more easily following instructions below.

5. Make any necessary changes to `CandidateContest.txt`.
 * For each change made in the previous step to `Office.txt`, make the corresponding change to `CandidateContest.txt`. Add the general election CandidateContest for each added Office, leaving the PrimaryParty field blank. (Primaries will be handled below.) For example:
 ```
US President (FL)	1	US President (FL)	
FL Governor	1	FL Governor		
US Senate FL	1	US Senate FL	
FL Attorney General	1	FL Attorney General	
FL Chief Financial Officer	1	FL Chief Financial Officer	
FL Commissioner of Agriculture	1	FL Commissioner of Agriculture	
```

5. Make any necessary changes to the more straightforward elements. It's often easier to add these in bulk later directly from the results files (see below) -- unless you want to use internal names that differ from the names in the results file.
  * `Party.txt`. You may be able to find a list of officially recognized parties on the Board of Election's website.
  * `Candidate.txt`. 
  * `BallotMeasure.txt`. If the ElectionDistrict is not the whole jurisdiction, you may need to add these by hand. A BallotMeasure is any yes/no question on the ballot, including judicial retention. Each BallotMeasure must have an ElectionDistrict and an Election matching an entry in the `ReportingUnit.txt` or `Election.txt` file.
  * `Election.txt`.

5. Revise `XX_starter_dictionary.txt` so that it has entries for any of the items created in the steps above (except that there is no need to add Elections to the dictionary, as they are never munged from the contents of the results file). The 'cdf_internal_name' column should match the names in the jurisdiction files. The 'raw_identifier_value' column should hold the corresponding names that will be created from the results file via the munger. 
    * It is helpful to edit the starter dictionary in an application where you can use formulas, or to manipulate the file with regular expression replacement. If you are not fluent in manipulating text some other way, you may want to use Excel and its various text manipulation formulas (such as =CONCAT()). However, beware of Excel's tendency to revise formats on the sly. You may want to check `.txt` and `.csv` files manipulated by Excel in a plain text editor if you run into problems. (If you've been curious to learn regex replacement, now's a good time!)
    * For each Office and CandidateContest, look in your results file to see what convention that file uses. For example, using data from official Florida election results files:
```
cdf_element	cdf_internal_name	raw_identifier_value
CandidateContest	US President (FL)	President of the United States
CandidateContest	US House FL District 1	Representative in Congress District 1
CandidateContest	US House FL District 2	Representative in Congress District 2
Office	US President (FL)	President of the United States
Office	US House FL District 1	Representative in Congress District 1
Office	US House FL District 2	Representative in Congress District 2
```

Make sure to change the raw_identifier_value for both the Offices and the CandidateContests. You may or may not need to change the corresponding ReportingUnits -- if they don't appear explicitly in any of your results files, their raw_identifier_values are irrelevant.
    * NB: it is perfectly OK to have more than one raw_identifier_value for a single element. This can be necessary if, say, different counties use different names for a single contest.
    
12. Add entries to the starter dictionary for CountItemType and BallotMeasureSelection. 
    * Internal database names for the BallotMeasure Selections are 'Yes' and 'No'. There are no alternatives.
    * Some common standard internal database names for CountItemTypes are 'absentee', 'early', 'election-day'provisional' and 'total'. You can look at the CountItemType table in the database to see the full list, and you can use any other name you like.
```
cdf_element	cdf_internal_name	raw_identifier_value
Election	General Election 2018-11-06	11/6/18
BallotMeasureSelection	No	No
BallotMeasureSelection	No	Against
BallotMeasureSelection	Yes	Yes
BallotMeasureSelection	Yes	For
CountItemType	election-day	Election Day
CountItemType	early	One Stop
CountItemType	absentee-mail	Absentee by Mail
CountItemType	provisional	Provisional
CountItemType	total	Total Votes
CountItemType	total	total
```

12. Add any existing content from `dictionary.txt` to the starter dictionary and dedupe. If the jurisdiction is brand new there won't be any existing contest. 

13. Move `XX_starter_dictionary.txt` from the current directory and to the jurisdiction's directory, and rename it to `dictionary.txt` . 

14. Run the JurisdictionPrepper method `add_sub_county_rus_from_multi_results_file(<directory>,<error>)` to add any reporting units in the results files in <directory>. 
```
>>> err = jp.add_sub_county_rus_from_multi_results_file('/Users/singer3/Documents/Temp/000_to-be-loaded',err)
>>> err
{}
```
These will be added as precincts, unless another reporting unit type is specified with the optional argument `sub_ru_type`, e.g.:
```
>>> err = jp.add_sub_county_rus_from_multi_results_file('/Users/singer3/Documents/Temp/000_to-be-loaded',err,sub_ru_type='congressional')
>>> err
{}
```

6. If you want to add elements (other than ReportingUnits) in bulk from all results files in a directory (with `.par` files in that same directory), use  `add_elements_from_multi_results_file(<list of elements>,<directory>, <error>)`. For example:
```
>>> err = jp.add_elements_from_multi_results_file(['Candidate','CandidateContest'],'/Users/singer3/Documents/Temp/000_to-be-loaded',err)
>>> err
{}
```
Corresponding entries will be made in `dictionary.txt`, using the munged name for both the `cdf_internal_name` and the `raw_identifier_value`. Note:
    * In every file enhanced this way, look for possible variant names (e.g., 'Fred S. Martin' and 'Fred Martin' for the same candidate in two different counties. If you find variations, pick an internal database name and put a line for each raw_identfier_value variation into `dictionary.txt`.
    * Candidate: sometimes there are non-candidate lines in the file. Take a look at `Candidate.txt` to see if there are lines (such as undervotes) that you may not want included in your final results. Also look for BallotMeasureSelections you might not have noticed before and add them to `dictionary.txt`.
    * CandidateContest: Look at the new `CandidateContest.txt` file. Many may be contests you do *not* want to add -- the contests you already have (such as congressional contests) that will have been added with the raw identifier name. Some may be BallotMeasureContests that do not belong in `CandidateContest.txt`. For any new CandidateContest you do want to keep you will need to add the corresponding line to `Office.txt`. 
    * Open `dictionary.txt` and remove any lines corresponding to items removed in the bullet points above.

13. Add any useful info about the jurisdiction (such as the sources for the data) to `remark.txt`.

14. Finally, if you will be munging primary elections, use the `add_primaries_to_candidate_contest()` and `jp.add_primaries_to_dict()` methods
```
>>> jp.add_primaries_to_candidate_contest()
>>> jp.add_primaries_to_dict()

```

### The JurisdictionPrepper class details
There are routines in the `JurisdictionPrepper()` class to help prepare a jurisdiction.
 * `JurisdictionPrepper()` reads parameters from the file (`new_jurisdiction.par`) to create the directories and basic necessary files. 
 * `new_juris_files()` builds a directory for the jurisdiction, including starter files with the standard contests. It calls some methods that may be independently useful:
   * `add_standard_contests()` creates records in `CandidateContest.txt` corresponding to contests that appear in many or most jurisdictions, including all federal offices as well as state house and senate offices. 
   * `add_primaries_to_candidate_contest()` creates a record in `CandidateContest.txt` for every CandidateContest-Party pair that can be created from `CandidateContest.txt` entries with no assigned PrimaryParty and `Party.txt` entries. (Note: records for non-existent primary contests will not break anything.) 
   * `starter_dictionary()` creates a `starter_dictionary.txt` file in the current directory. Lines in this starter dictionary will *not* have the correct `raw_identifier_value` entries. Assigning the correct raw identifier values must be done by hand before proceeding.
 * `add_primaries_to_dict()` creates an entry in `dictionary.txt` for every CandidateContest-Party pair that can be created from the CandidateContests and Parties already in `dictioary.txt`. (Note: entries in `dictionary.txt` that never occur in your results file won't break anything.)
 * Adding precincts automatically:
     *`add_sub_county_rus_from_results_file(error)` is useful when:
         * county names can be munged from the rows
         * precinct (or other sub-county reporting unit) names can be munged from the rows
         * all counties are already in `dictionary.txt`
   
       can be read from _rows_ of the datafile. The method adds a record for each precinct to `ReportingUnit.txt` and `dictionary.txt`, with internal db name obeying the semicolon convention. For instance, if:
         * `ReportingUnit\tFlorida;Alachua County\tAlachua` is in `dictionary.txt
         * County name `Alachua` and precinct name `Precinct 1` can both be munged from the same row of the results file
     
         then:
         * `Florida;Alachua County;Precinct 1\tprecinct` will be added to `ReportingUnit.txt`
         * `ReportingUnit\tFlorida;Alachua County;Precinct 1\tAlachua;Precinct 1` will be added to `dictionary.txt`
     * `add_sub_county_rus_from_multi_results_file(directory,error)` does the same for every results file/munger in the directory named in a `.par` file in the directory.
 * adding other elements automatically:
     * `add_elements_from_results_file(result_file,munger,element`) pulls raw identifiers for all instances of the element from the datafile and inserts corresponding rows in `<element>.txt` and `dictionary.txt`. These rows may have to be edited by hand to make sure the internal database names match any conventions (e.g., for ReportingUnits or CandidateContests, but maybe not for Candidates or BallotMeasureContests.)
     * `add_elements_from_multi_results_file(directory, error)` does the same for every file/munger in the directory named in a `.par` file in the directory
 
## Load Data
Some routines in the Analyzer class are useful even in the data-loading process, so  create an analyzer before you start loading data.
```
>>> an = ea.Analyzer()
>>> 
```

The MultiDataLoader class allows batch uploading of all data in a given directory. That directory should contain the files to be uploaded, as well as a `.par` file for each file to be uploaded. See `templates/parameter_file_templates/results.par`. You can use `make_par_files()` to create parameter files for multiple files when they share values of the following parameters:
 * directory in which the files can be found
 * munger
 * jurisdiction
 * election
 * download_date
 * source
 * note
 * auxiliary data directory (if any)
The `load_all()` method will read each `.par` file and make the corresponding upload.
From a directory containing a `multi.par` parameter file, run
```
import election_anomaly as ea
mdl = ea.MultiDataLoader()
err = mdl.load_all()
```

If something doesn't work, check the value returned by the `load_all()` method. In this example there was an extraneous file `dictionary copy.txt` in the jurisdiction directory.
```
>>> mdl = ea.MultiDataLoader()
>>> err = mdl.load_all()
>>> err
{'BAK_PctResults20181106.par': (None, {'extraneous_files_in_juris_directory': ['dictionary copy.txt']}, {'juris_created': False}, None, None), 'ALA_PctResults20181106.par': (None, {'extraneous_files_in_juris_directory': ['dictionary copy.txt']}, {'juris_created': False}, None, None)}
```

Some results files may need to be munged with multiple mungers, e.g., if they have combined absentee results by county with election-day results by precinct. If the `.par` file for that results file has `munger_name` set to a comma-separated list of mungers, then all those mungers will be run on that one file.

If every file in your directory will use the same munger(s) -- e.g., if the jurisdiction offers results in a directory of one-county-at-a-time files, such AZ or FL -- then you may want to use `make_par_files()`, whose arguments are:
 * the directory holding the results files,
 * the munger name (for multiple mungers, pass a string that is a comma-separated list of munger names)
 * jurisdiction (can be, e.g., 'Florida' as long as every file has Florida results)
 * election (has to be just one election),
 * download_date
 * source
 * note (optional)
 * aux_data_dir (optional -- use it if your files have all have the same auxiliary data files, which might never happen in practice)

## Pull Data
The Analyzer class uses parameters in the file `analyze.par`, which should be in the directory from which you are running the program.

There are two options: pulling total vote counts, or pulling vote counts by vote type. To pull totals, use `top_counts()` in the Analyzer class.
```
>>> an.top_counts('Pennsylvania;Philadelphia','ward')
Results exported to /Users/user/Documents/rollups/2018 General/Pennsylvania;Philadelphia/by_ward/TYPEall_STATUSunknown.txt
```

To pull totals by vote type, use `top_counts_by_vote_type()`
```
>>> an.top_counts_by_vote_type('Pennsylvania;Philadelphia','ward')
Results exported to /Users/singer3/Documents/rollups/2018 General/Pennsylvania;Philadelphia/by_ward/TYPEmixed_STATUSunknown.txt
>>> 
```

Results are exported to the `rollup_directory` specified in `run_time.par`.

Note that both arguments -- the name of the top reporting unit ('Pennsylvania;Philadelphia') and the reporting unit type for the breakdown of results ('ward') must be the internal database names. To see the list of options, use `display_options()`:
```
>>> an.display_options('reporting_unit_type')
['ballot-batch', 'ballot-style-area', 'borough', 'city', 'city-council', 'combined-precinct', 'congressional', 'country', 'county', 'county-council', 'drop-box', 'judicial', 'municipality', 'polling-place', 'precinct', 'school', 'special', 'split-precinct', 'state', 'state-house', 'state-senate', 'town', 'township', 'utility', 'village', 'vote-center', 'ward', 'water', 'other']
```
Lists of reporting units will be quite long, in which case searching by substring can be useful.
```
>>> counties = an.display_options('county')
>>> [x for x in counties if 'Phila' in x]
['Pennsylvania;Philadelphia']
>>> 
```
Here we have used the capability of `display_options()` to take as an argument either a general database category ('reporting unit') or a type ('county'). 

```

