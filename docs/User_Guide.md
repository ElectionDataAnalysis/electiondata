# How to Use the System
## Environment
You will need `python3.9`. If you use the alias `python`, make sure it points to `python3.9`.

The system runs out of the box with a postgresql database; to use other varieties of SQL, you will need to modify the routines in the `database` module. 

## Installation
From the root folder of your repository run `python3 setup.py install` (or if `python` is an alias for `python3` on your system, `python setup.py install`).

## Setting up

### Main parameter file
You will need a main parameter file to specify paths and database connection information specific to your local computing environment. This file is necessary for the three main classes:
 * `JurisdictionPrepper` for preparing jurisdiction files
 * `DataLoader` for loading data
 *  `Analyzer` for exporting and analyzing results
See the [template file](../src/parameter_file_templates/run_time.ini.template) for required parameters. Avoid percent signs and line breaks in the parameter values.
   
### Other recommended files
To avoid the overhead of deriving the major subdivision type for each jurisdiction from the database, make sure that your repository has a [000_major_subjurisdiction_types.txt](../src/jurisdictions/000_for_all_jurisdictions/000_major_subjurisdiction_types.txt) in the [jurisdictions directory](../src/jurisdictions/). This file allows the user to specify other major subdivisions. For example, it may make sense to consider towns as the major subdivisions in Connecticut rather than counties. Or a user may wish to use congressional districts as the major subdivision -- though such a user should not assume that the nesting relationships (say, of precincts within congressional districts) have been coded in the [`ReportingUnit.txt` file](../src/jurisdictions/Connecticut/ReportingUnit.txt) or the database.

## Determining a Munger
Election result data comes in a variety of file formats. Even when the basic format is the same, file columns may have different interpretations. The code is built to ease -- as much as possible -- the chore of processing and interpreting each format. Following the [Jargon File](http://catb.org/jargon/html/M/munge.html), which gives one meaning of "munge" as "modify data in some way the speaker doesn't need to go into right now or cannot describe succinctly," we call each set of basic information about interpreting an election result file a "munger". 

If the munger for the format of your results file doesn't already exist:
 * pick a name for your munger 
 * create a file with that name and extension `.munger` in the `mungers` directory (e.g., `me_excel.munger`) with sections and parameters described below. You may find it helpful to work with the template from `src/mungers/000_template.munger`. 
 
 The file with munger parameters has one or more sections, each with a header:
  * (required) `[format]` for the main parameters
  * (required) `[munge formulas]`
  * (required if formulas use foreign keys) one section for each foreign key appearing in munge formulas. Foreign keys are preceded by " from  ", e.g., in the formula
  * (optional) `[ignore]`
 
### \[format\]
 There are two required format parameters: `file_type` and `count_location`. 
 The `file_type` parameter controls which function from the python `pandas` module reads the file contents. Related optional and required parameters must be given under the `[format]` header.
  * 'flat_text': Any tab-, comma-, or other-separated table in a plain tabular text file.
    * (required) a field delimiter `flat_text_delimiter` to be specified (usually `flat_text_delimiter=,` for csv or `flat_text_delimiter=tab` for .txt)
      
  * 'excel'
    * (optional) a list `sheets_to_read_names` (and/or `sheets_to_read_numbers`) of spreadsheets to read, 
    * (optional) a list `sheets_to_skip_names` of names of spreadsheets to skip
    * Default is to read all sheets
    * (optional) `merged_cells` If there are merged cells in the meaningful header rows, set `merged_cells=yes`. 
  * for both 'flat_text' and 'excel':
    * (required if `count_location=by_name`) specify location of field names for count columns. with integer `count_field_name_row` (NB: top row not skipped is 0, next row is 1, etc.)
    * (required):
        * Either `all_rows=data` or designate row containing column names for the candidate, reporting unit, etc. with the `noncount_header_row` parameter. (NB: top row not skipped is 0, next row is 1, etc.)

   Available if appropriate for any file type, under the `[format]` header:
   * (required if any munging information needs to be read from the `<results>.ini` file) `constant_over_file`, a comma-separated list of elements to be read, e.g., `constant_over_file=CandidateContest,CountItemType`.
   * (optional) `thousands_separator`. In the US the separator is almost always ',' if it is used. Watch out for Excel files which may show a comma when you look at them in Excel -- there may not be a comma in the underlying data.
   * (optional) `encoding` (If not specified or recognized, a default encoding will be used. Recognized encodings are limited [python's list of recognized encodings and aliases](https://docs.python.org/3/library/codecs.html#standard-encodings).)

   Available for flat_text and excel file types:
   * (optional - use only for multi-block) `rows_to_skip` An integer giving the number of rows to skip at the top before processing blocks.
   * (optional) `all_rows` If the file has no column headers but only data rows with counts, set this parameter to 'data'
   * (optional) `multi_block` if there are multiple blocks of data per page, each with its own headers, set this parameter to 'yes'. For multi-block sheets, munge parameters refer to the blocks (and must be the same for all blocks).
   * (optional) `max_blocks` if `multi_block=yes`, `max_blocks` is an integer telling the system how many blocks at most to read off of each sheet.
 
### \[munge formulas\]
Put each formula for parsing information from the results file into the `[munge formulas]` section. Constant items can be give either:
 * as comma separated list in constant_over_sheet parameter in .munger file, with values in the .ini file
 * as a constant formula in the `[munge formulas]` section, in which case a corresponding entry must be made in the jurisdiction's `dictionary.txt`.

For many results files, it is enough to create concatenation formulas, referencing field names from your file by putting them in angle brackets (<>. The available fields are:
  * `<count_header_0>`, `<count_header_1>`, etc. to denote information from the headers of the columns containing the counts.
  * `<row_0>`, `<row_1>`, etc., to read information constant over a sheet or block from one of the header rows. The system recognizes the leftmost non-blank cell as the content to be read.
  * `<sheetname>` to denote the name of an Excel spreadsheet within a (possibly multi-sheet) workbook.
  * any field name from the file itself, e.g., `<SELECTION>`
  * for tree-structured files like json and xml, see below for field labeling conventions.
  
Some characters are reserved to indicate parsing and interpretation in the munge formulas. Angle brackets (`<>`), braces (`{}`), commas (`,`) and the word `from` should not be used in any other way.

 Consider this snippet from a comma-separated flat-text Philadelphia, Pennsylvania voting results file:
```
WARD,DIVISION,VOTE TYPE,CATEGORY,SELECTION,PARTY,VOTE COUNT
01,01,A,JUDGE OF THE SUPERIOR COURT,AMANDA GREEN-HAWKINS,DEMOCRATIC,2
01,01,M,JUDGE OF THE SUPERIOR COURT,AMANDA GREEN-HAWKINS,DEMOCRATIC,146
01,01,P,JUDGE OF THE SUPERIOR COURT,AMANDA GREEN-HAWKINS,DEMOCRATIC,0
```
The formula `ReportingUnit=Ward <WARD>;Division <DIVISION>` would yield 'Ward 01;Division 01'.

#### the `count_location` parameter for xml and json files
In tree-structured file types like json and xml files, the `count_location` parameter is a path to the count.

Consider this snippet from a Georgia voting results xml file:
 ```
 <ElectionResult>
    <Contest>
        <Choice key="25" text="Raphael Warnock (Dem)" totalVotes="2279559">
            <VoteType name="Election Day Votes" votes="485573">
                <County name="Appling" votes="391" />
                <County name="Atkinson" votes="262" />
                <County name="Bacon" votes="162" />
```

Because the number of votes is an attribute, we set
```count_location=ElectionResult/Contest/Choice/VoteType/County.votes```
The munge formula `<VoteType.name>` would yield 'Election Day Votes'. It is important that the first element in any munge formula (in this example, `VoteType`) be in the `count_location` path. 


Consider this snippet from a `json` file from Virginia (with line breaks added for clarity):
 ```
{
"ElectionName":"2020 November General",
"ElectionDate":"2020-11-03T00:00:00",
"CreateDate":"2021-01-21T15:03:04.979827-05:00",
"RaceName":"Member House of Representatives (03)",
"NumberOfSeats":1,
"Localities":[
    {
    "Locality":{"LocalityName":"CHESAPEAKE CITY","LocalityCode":"550"},
    "PrecinctsReporting":31,"PrecinctsParticipating":31,
    "LastModified":"2020-11-09T18:12:13.92",
    "Candidates":[
        {
        "BallotName":"Robert C. \"Bobby\" Scott","BallotOrder":1,
        "Votes":35042,"Percentage":"63.70%",
        "PoliticalParty":"Democratic"
        },
 ...
```
Each pair of braces ({}) and its contents is a json "object", while each pair of brackets ([]) and its contents is a json "array". The `count_locations` path indicates which arrays one must step into to find the count, as well as the label for the count itself. For the Virginia example above: 
`count_location=Localities/Candidate/Votes`. Munge formulas referencing the top level are simply the label for the information (e.g., `CandidateContest=<RaceName>`). Munge formulas referencing information within an array must start with that array name, and provide the path within that array to the desired value (e.g., `ReportingUnit=<Localities.Locality.LocalityName>`).

Note that `count_location` is a `/`-separated path, possibly with a period (`.`) at the end for xml files where the count is in an attribute. Munge formulas use only periods.


#### flat text 
Consider this snippet from a tab-separated North Carolina voting results file:
```
County	Election Date	Precinct	Contest Group ID	Contest Type	Contest Name	Choice	Choice Party	Vote For	Election Day	One Stop	Absentee by Mail	Provisional	Total Votes	Real Precinct
ALAMANCE	11/06/2018	064	1228	S	NC COURT OF APPEALS JUDGE SEAT 3	Michael Monaco, Sr.	LIB	1	59	65	2	1	127	Y
ALAMANCE	11/06/2018	03N	1228	S	NC COURT OF APPEALS JUDGE SEAT 3	Michael Monaco, Sr.	LIB	1	59	38	1	0	98	Y
ALAMANCE	11/06/2018	03S	1228	S	NC COURT OF APPEALS JUDGE SEAT 3	Michael Monaco, Sr.	LIB	1	106	108	0	3	217	Y
```
Here the CountItemType value ('Election Day','One Stop' a.k.a. early voting, 'Absentee by Mail','Provisional' must be read from the column headers, i.e., the information in row 0 of the file. For the first data row, the formula `CountItemType=<count_header_0>` would yield CountItemType 'Election Day' for the VoteCount of 59, 'One Stop' for the vote count of 65, etc.

#### excel and multi_block=yes
To be read automatically, information that is constant over a sheet (or block) must be read either from the sheet name (using `<sheet_name>`) or from the left-most, non-blank entry in a row of the sheet using `<row_j>`, where `j` is the row number. Row numbers start with `0` after skipping the number of rows given in `rows_to_skip`.

NB: the system assumes that blocks are separated by blank lines. This means that files with blank lines internal to the blocks must be revised before processing.


#### Regular Expressions
Sometimes it is necessary to use regular expressions to extract information from fields in the results file.  For example, in a primary both the Party and the Candidate may be in the same string (e.g., "Robin Perez (DEM)"). Braces ({}) indicate that regex analysis is needed. Inside the curly brackets there are two parts, separated by a comma. The first part is the field name to pull a string from the file. The second is a python regex formula whose first group (enclosed by parentheses) marks the desired substring.
```Candidate	{<count_header_1>,^(.*) \([a-zA-Z]{3}\)$}	row```

The system will report (in the `.warnings` files) any strings that did not match the regex. 

### \[\<foreign key\> lookup\]

If any of the munge formulas depend on information from other files, munger must specify lookup information. For each foreign key, there must be a separate section with corresponding header (foreign key, plus " lookup", e.g. `[CANDIDATE NAME lookup]` if the results file has a `CANDIDATE NAME` field. This section needs:
  * `lookup_id` is the single field name holding the key to the lookup table. (If there are no headers in the lookup source file, use, e.g., `column_0`)
  * `source_file` the path to the source file, relative to the results directory given in `run_time.ini`
  * all the usual format parameters except `count_location`

For example, here is a munger for Texas 2020 General election results using lookups.
```
[format]
file_type=excel
count_location=by_name:TOTAL VOTES PER OFFICE PER COUNTY

encoding=utf_8
thousands_separator=,
count_column_numbers=4
noncount_header_row=0
count_field_name_row=0

[munge formulas]
ReportingUnit=<COUNTY NAME>
Party=<PARTY from CANDIDATE NAME>
CandidateContest=<OFFICE NAME>
Candidate=<CANDIDATE NAME>
CountItemType=total

[CANDIDATE NAME lookup]
source_file=Texas/Party_by_Candidate_20g.xlsx
lookup_id=CANDIDATE NAME
file_type=excel
noncount_header_row=0
```
NB: if there are multiple rows in the lookup file with the same values for the lookup id columns, the system will arbitrarily use the first row and ignore the others.

### \[ignore\]
 Unrecognized Contests, Candidates and Parties are collected as "none or unknown". Some states (e.g., Wisconsin 2018 General) report total votes over a contest next to individual candidates' votes. The system may read, e.g., "Total Votes Cast" as an unrecognized party name. In this case include the lines:
  ```
[ignore]
Party=Total Votes Cast
```
and similarly, if necessary, for any Contest or Selection. If there is more than one Candidate (e.g.) to be ignored, use a comma-separated list: `Candidate=Total Votes Cast,Registered Voters`

### multi_block example
[to do]
 
 You may find it helpful to follow the example of the mungers in the repository.




## Create or Improve a Jurisdiction
Because each original raw results file comes from a particular election agency, and each election agency has a fixed jurisdiction, we organize information by jurisdiction. The  [`000_for_all_jurisdictions` folder](../src/jurisdictions/000_for_all_jurisdictions) holds information pertinent to all jurisdictions: the list of elections in [`Election.txt](../src/jurisdictions/000_for_all_jurisdictions/Election.txt)

It's easiest to use the JurisdictionPrepper() object to create or update jurisdiction files.

 (0) Create a `jurisdiction_prep.ini` file, following the example in `src/parameter_file_templates/jurisdiction_prep.ini.template`. You will need to specify the number of congressional, state house and state senate districts.

 (1) From the directory containing `jurisdiction_prep.ini`, open a python interpreter. Import the package and initialize a JurisdictionPrepper(), e.g.:
```
>>> import electiondata as ea
>>> jp = ea.JurisdictionPrepper()
```
 (2) Call new_juris_files(), which will create the necessary files in the jurisdiction directory, as well as a starter dictionary file (`XX_starter_dictionary.txt`) in the current directory.
```
>>> jp.new_juris_files()
```
The routine `new_juris_files` creates the necessary files in a folder (at the location `jurisdiction_path` specified in `jurisdiction_prep.ini`). Several of these files are seeded with information that can be deduced from the other information in `jurisdiction_prep.ini`.
 
In addition, `new_juris_files` creates a starter dictionary `XX_starter_dictionary.txt` in your current directory. Eventually the `dictionary.txt` file in your jurisdiction directory will need to contain all the mappings necessary for the system to match the data read from the results file ("raw_identifiers") with the internal database names specified in the other `.txt` files in the jurisdiction directory. The starter dictionary maps the internal database names to themselves, which is usually not helpful. In the steps below, you will correct (or add) lines to `dictionary.txt` following the conventions in the file. The system does not try to guess how internal database names are related to names in the files. 

NB: it is perfectly OK to have more than one raw_identifier_value for a single element. This can be necessary if, say, different counties use different names for a single contest. What can cause problems are lines with the same cdf_element and same raw_identifier_value, but different cdf_internal_names.

 (3) Add all counties to the `ReportingUnit.txt` file and `XX_starter_dictionary.txt`. You must obey the semicolon convention so that the system will know that the counties are subunits of the jurisdiction. For example:
```
Name	ReportingUnitType
Texas;Angelina County	county
Texas;Gregg County	county
Texas;Harrison County	county
```
Counties must be added by hand. 

NB: in some jurisdictions, the major subdivision type is not 'county. For instance, Louisiana's major subdivisions are called 'parish'. In the `elections.analyze` module, several routines roll up results to the major subdivision -- usually counties. By default, the ReportingUnitType of the major subdivision is read from the file [major_subjurisdiction_types.txt](../src/jurisdictions/000_for_all_jurisdictions/major_subjurisdiction_types.txt) if possible; if that file is missing, or does not provide a subdivision type for the particular jurisdiction in question, the system will try to deduce the major subdivision type from the database. A different file of subdivision types can be specified with the optional `major_subdivision_file` parameter in `Analyzer()` or `DataLoader()`

The system assumes that internal database names of ReportingUnits carry information about the nesting of the basic ReportingUnits (e.g., counties, towns, wards, etc., but not congressional districts) via semicolons. For example: `
 * `Pennsylvania;Philadelphia;Ward 8;Division 6` is a precinct in 
 * `Pennsylvania;Philadelphia;Ward 8`, which is a ward in
 * `Pennsylvania;Philadelphia`, which is a county in
 * `Pennsylvania`, which is a state.
 
Other nesting relationships (e.g., `Pennsylvania;Philadelphia;Ward 8;Division 6` is in `Pennsylvania;PA Senate District 1`) are not yet recorded in the system (as of 4/2/2021).

To find the raw_identifiers for the dictionary, look in your results files to see how counties are written. For example, if your results file looks like this (example from Texas):
```
ELECTION DATE-NAME	OFFICE NAME	CANDIDATE NAME	COUNTY NAME	TOTAL VOTES PER OFFICE PER COUNTY
03/03/2020 - 2020 MARCH 3RD REPUBLICAN PRIMARY	U. S. REPRESENTATIVE DISTRICT 1	JOHNATHAN KYLE DAVIDSON	ANGELINA	1,660
03/03/2020 - 2020 MARCH 3RD REPUBLICAN PRIMARY	U. S. REPRESENTATIVE DISTRICT 1	LOUIE GOHMERT	ANGELINA	10,968
03/03/2020 - 2020 MARCH 3RD REPUBLICAN PRIMARY	U. S. REPRESENTATIVE DISTRICT 1	JOHNATHAN KYLE DAVIDSON	GREGG	914
03/03/2020 - 2020 MARCH 3RD REPUBLICAN PRIMARY	U. S. REPRESENTATIVE DISTRICT 1	LOUIE GOHMERT	GREGG	9,944
03/03/2020 - 2020 MARCH 3RD REPUBLICAN PRIMARY	U. S. REPRESENTATIVE DISTRICT 1	JOHNATHAN KYLE DAVIDSON	HARRISON	774
03/03/2020 - 2020 MARCH 3RD REPUBLICAN PRIMARY	U. S. REPRESENTATIVE DISTRICT 1	LOUIE GOHMERT	HARRISON	7,449
```
you would want lines in your dictionary file like this:
```
cdf_element	cdf_internal_name	raw_identifier_value
ReportingUnit	Texas;Angelina County	ANGELINA
ReportingUnit	Texas;Gregg County	GREGG
ReportingUnit	Texas;Harrison County	HARRISON
```
Note that the entries in the `cdf_internal_name` column exactly match the entries in the `Name` column in `ReportingUnit.txt`.

 (4) As necessary, revise `CandidateContest.txt` (along with `Office.txt` and `XX_starter_dictionary.txt`). 
 * The offices and candidate-contests added by `new_juris_files()` are quite generic. For instance, your jurisdiction may have a 'Chief Financial Officer' rather than an 'Treasurer'. Use the jurisdiction's official titles, from an official government source. Add any missing offices. Our convention is to preface state, district or territory offices with the two-letter postal abbreviation. For example (in `Office.txt`):
```
Name	ElectionDistrict
US President (FL)	Florida
FL Governor	Florida
US Senate FL	Florida
FL Attorney General	Florida
FL Chief Financial Officer	Florida
FL Commissioner of Agriculture	Florida
```
If you are interested in local contests offices (such as County Commissioner), you will need to add them. If the ElectionDistrict for any added contest is not already in `ReportingUnit.txt`, you will need to add it. Note that judicial retention elections are yes/no, so they should be handled as BallotMeasureContests, not CandidateContests. NB: If you want to add Offices in bulk from a results file, you can wait and do it more easily following instructions below.

For each new or revised Office, add or revise entries in `CandidateContest.txt`. Leave the PrimaryParty column empty. Do not add primaries at this point -- they can be added in bulk below.  For example (in `CandidateContest.txt`):
 ```
US President (FL)	1	US President (FL)	
FL Governor	1	FL Governor		
US Senate FL	1	US Senate FL	
FL Attorney General	1	FL Attorney General	
FL Chief Financial Officer	1	FL Chief Financial Officer	
FL Commissioner of Agriculture	1	FL Commissioner of Agriculture	
```

Finally, look in your results files to see what naming conventions are used for candidate contests. Add lines to the starter dictionary. For example, using data from official Florida election results files:
```
cdf_element	cdf_internal_name	raw_identifier_value
CandidateContest	US President (FL)	President of the United States
CandidateContest	US House FL District 1	Representative in Congress District 1
CandidateContest	US House FL District 2	Representative in Congress District 2
```

 (5) Make any necessary additions or changes to the more straightforward elements. It's often easier to add these in bulk later directly from the results files (see below) -- unless you want to use internal names that differ from the names in the results file.
  * `Party.txt`. You may be able to find a list of officially recognized parties on the Board of Election's website.
  * `BallotMeasure.txt`. If the ElectionDistrict is not the whole jurisdiction, you may need to add these by hand. A BallotMeasure is any yes/no question on the ballot, including judicial retention. Each BallotMeasure must have an ElectionDistrict and an Election matching an entry in the `ReportingUnit.txt` or `Election.txt` file.

 (6) Revise `XX_starter_dictionary.txt` so that it has entries for any of the items created in the steps above (except that there is no need to add Elections to the dictionary, as they are never munged from the contents of the results file). The 'cdf_internal_name' column should match the names in the jurisdiction files. The 'raw_identifier_value' column should hold the corresponding names that will be created from the results file via the munger. 
    * It is helpful to edit the starter dictionary in an application where you can use formulas, or to manipulate the file with regular expression replacement. If you are not fluent in manipulating text some other way, you may want to use Excel and its various text manipulation formulas (such as =CONCAT()). However, beware of Excel's tendency to revise formats on the sly. You may want to check `.txt` and `.csv` files manipulated by Excel in a plain text editor if you run into problems. (If you've been curious to learn regex replacement, now's a good time!)
 
 (7) Revise the 'CountItemType' entries in the starter dictionary to match any words or phrases used in the results files. E.g., for North Carolina
```
cdf_element	cdf_internal_name	raw_identifier_value
CountItemType	election-day	Election Day
CountItemType	early	One Stop
CountItemType	absentee-mail	Absentee by Mail
CountItemType	provisional	Provisional
CountItemType	total	Total Votes
CountItemType	total	total
```

 (8) Add any existing content from `dictionary.txt` to the starter dictionary. If the jurisdiction is brand new there won't be any existing content. 

 (9) Move `XX_starter_dictionary.txt` from the current directory and to the jurisdiction's directory, and rename it to `dictionary.txt` . 

 (10) If your results file is precinct based instead of county based, and you would like to use the results file to add precincts to your `ReportingUnit.txt` file, run `add_sub_county_rus`, e.g.: 
```
>>> jp.add_sub_county_rus('my_results.ini')
```
These will be added as precincts, unless another reporting unit type is specified with the optional argument `sub_ru_type`, e.g.:
```
>>> jp.add_sub_county_rus_from_multi_results_file('my_results.ini',sub_ru_type='town')
```
If the jurisdiction's major subdivision is not county but something else (e.g., state house district, as in Alaska, or parish as in Louisiana), use the optional argument `county_type`:
```
>>> jp.add_sub_county_rus_from_multi_results_file('my_results.ini',county_type='state-house')
```


 (11) Look at the newly added items in `ReportingUnit.txt` and `dictionary.txt`, and remove or revise as appropriate.

### Miscellaneous Notes
  * Candidate
      * Look for possible variant names (e.g., 'Fred S. Martin' and 'Fred Martin' for the same candidate in two different counties. If you find variations, pick an internal database name and put a line for each raw_identifier_value variation into `dictionary.txt`.
      * Look for non-candidate lines in the file. Take a look at `Candidate.txt` to see if there are lines (such as undervotes) that you may not want included in your final results. 
      * Our convention for internal names for candidates with quoted nicknames is to use single quotes. Make sure there are no double-quotes in the Name column in `Candidate.txt` and none in the cdf_internal_name column of `dictionary.txt`. E.g., use `Rosa Maria 'Rosy' Palomino`, not `Rosa Maria "Rosy" Palomino`. Note that if your results file has `Rosa Maria "Rosy" Palomino`, the system will read the double-quotes as single-quotes, so the dictionary.txt line `Candidate	Rosa Maria 'Rosy' Palomino	Rosa Maria "Rosy" Palomino` will munge both `Rosa Maria 'Rosy' Palomino` and `Rosa Maria "Rosy" Palomino` correctly.
     * Our convention for internal names for multiple-candidate tickets (e.g., 'Biden/Harris' is to use the full name of the top candidate, e.g., 'Joseph R. Biden'). There should be a line in `dictionary.txt` for each variation used in the results files, e.g.:
```
cdf_element	cdf_internal_name	raw_identifier_value
Candidate	Joseph R. Biden	Biden / Harris
Candidate	Joseph R. Biden	Joseph R. Biden
```

  * CandidateContest: For any new CandidateContest you do want to keep you will need to add the corresponding line to `Office.txt` (and the ElectionDistrict to `ReportingUnit.txt` if it is not already there). 
  * Primary Elections: if you will be munging primary elections, we recommend making each primary a separate election (e.g., "2021 Democratic Primary", "2021 Republican Primary")

 
## Load Data
Each results file to be loaded must be designated in a `*.ini` file inside its jurisdiction's corresponding subfolder of `ini_files_for_results` in the repository. The `*.ini` files currrently in this repository correspond to [official raw data files for the US 2020 General Election](https://doi.org/10.7910/DVN/0GKBGT). These should load directly with the munger and jurisdiction files from the `electiondata` repository. (Note, however, that due to Excel corruption issues, Vermont and Wisconsin files may fail to load; Connecticut, Maryland and Pennsylvania will load but may fail some of the tests because of inconsistencies within their official agencies' materials.)
 
The DataLoader class allows batch uploading of all data in the directory indicated by the `results_dir` parameter in the main parameter file. The subdirectories of this file should be named for the jurisdictions (with hyphens replacing spaces, as in 'US-Virgin-Islands'. The `DataLoader.load_all()` method will upload every result file that appears, as long as its path (relative to the `results_dir`) is the `results_file` parameter for some `*.ini` file in `ini_files_for_results`. 

```
import electiondata as ea
dl = ea.DataLoader()
dl.load_all()
```

Some results files may need to be munged with multiple mungers, e.g., if they have combined absentee results by county with election-day results by precinct. If the `.ini` file for that results file has `munger_list` set to a comma-separated list of mungers, then all those mungers will be run on that one file.

### Error reporting
All errors will be reported to the a subdirectory named by the database and timestamp within the directory specified by the `reports_and_plots_dir` parameter in the main parameter file.

Fatal errors will be noted in a file `*.errors`. 

Even when the upload has worked, there may be warnings about lines not loaded. The system will ignore lines that cannot be munged. For example, the only contests whose results are uploaded will be those in the `CandidateContest.txt` or `BallotMeasureContest.txt` files that are correctly described in `dictionary.txt`.

If there are no errors, the results files will be moved to a subdirectory of the directory specified by the `archive_dir` parameter in the main parameter file. 

## Exporting Data

### Initiating the Analyzer
The Analyzer class takes two optional parameters:
 * `param_file`: path to the parameter file for defining the Analyzer directories, database connection, etc. if not specified, the default is the `run_time.ini` file in the directory from which the call to Analyzer() is made
 * `dbname`: name of database to use. If not specified, the default is the database specified in the `param_file`

To get an instance of an analyzer, you can call the Analyzer class directly:
```
import electiondata as ea
analyzer = ea.Analyzer(param_file=param_file, dbname=dbname)
```
or, since every instance of the DataLoader class creates its own analyzer:
```
import electiondata as ea
dl = ea.DataLoader(param_file=param_file, dbname=dbname)
analyzer = dl.analyzer
```

### Exporting tabular data
The Analyzer class has a number of functions that allow you to aggregate the data for analysis purposes. For example, running the `.top_counts()` function exports files into your rollup_dataframe directory which with counts summed up at a particular reporting unit level. This function expects 4 arguments: the election, the jurisdiction, the reporting unit level at which the aggregation will occur, and a boolean variable indicating whether you would like the data aggregated by vote count type. For example, to export all 2020 General results in your database to a tab-separated file `tabular_results.tsv`:
```
analyzer.export_election_to_tsv("tabular_results.tsv", "2020 General")
```
To export results for a single jurisdiction, use, e.g.:
```
analyzer.export_election_to_tsv("tabular_results.tsv", "2020 General", "South Carolina")
```

This code will produce all South Carolina data from the 2018 general election, grouped by contest, county, and vote type (total, early, absentee, etc).

### NIST Common Data Format
This package also provides functionality to export the data to xml according to the [NIST election results reporting schema (Version 2)](https://github.com/usnistgov/ElectionResultsReporting/raw/version2/NIST_V2_election_results_reporting.xsd). This is as simple as identifying an election and jurisdiction of interest:
```
import electiondata as ea
analyzer = ea.Analyzer()
election_report = analyzer.export_nist_v2("2020 General", "Georgia")
```
The output is a string, the contents of the xml file.

There is also an export in the NIST V1 json format:
```
analyzer = ea.Analyzer()
analyzer.export_nist_v1_json("2020 General","Georgia")
```
The output is a string, the contents of the json file.
Both of these can take an optional `major_subdivision` parameter to control the level to which results are rolled up. The default is to roll up to the subdivision type indicated in the [`000_major_subjurisdiction_type.txt file](../jurisdictions/000_major_subjurisdiction_types.txt).


## Unload and reload data with `reload_juris_election()`
To unload existing data for a given jurisdiction and a given election you can use the routine 
```ea.reload_juris_election(jurisdiction, election, report_dir)```
This function takes optional arguments:
 * rollup (defaults to  false), if true, rolls up results within the to the major subdivision
 * dbname, name of database if given; otherwise database name taken from parameter file
 * param_file, path to parameter file for dataloading if given; otherwise parameter file
            path assumed to be 'run_time.ini'
 * move_files (defaults to True), if true, move all files to archive directory if loading (& testing, if done)
            are successful
 * run_tests (default to True), if true, run tests on data to be loaded, and load only if the tests are passed.

Results of the test will be reported in a the directory specified by the `reports_and_plots_dir` parameter in the parameter file.

## Loading a file with multiple elections or jurisdictions
Sometimes it is useful to load a single file with results from several elections or jurisdictions. For example, a secondary source may have combined results information into one file. The method `DataLoader.load_multielection_from_ini()` method allows this kind of upload. This method requires an initialization file, with all the usual required parameters except `election` and `jurisdiction`, and with the additional parameter `secondary_source`. The value of `secondary-source` should be the name of a subfolder of `src/secondary_sources` in the repository, containing files listing the elections and jurisdictions. E.g., 
```
[election_results]
results_file=MEDSL/county_2018.csv
munger_list=medsl_2018
secondary_source=MIT-Election-Data-Science-Lab
results_short_name=medsl_2018_county
results_download_date=2021-07-10
results_source=https://github.com/MEDSL/2018-elections-official/blob/master/county_2018.csv
results_note=county-level
```
The method `DataLoader.load_multielection_from_ini()` takes two optional parameters: 
 * `overwrite_existing` (default `False`): if True, will delete from database any existing results for each election-jurisdiction pair represented in the results file
 * `load_jurisdictions` (default `False`): if True, will load or update database with the jurisdiction information (from `src/jurisdictions`) for each jurisdiction represented in the results file


## Miscellaneous helpful hints
### Required Conventions
For ReportingUnits, the naming convention is to list as much of the composing information as possible in the name of the element, using `;` as a separator. E.g., 
 * `North Carolina` -- the state of NC
 * `North Carolina;Alamance County` -- Alamance County, which is contained in North Carolina
 * `North Carolina;Alamance County;Precinct 12W` -- Precinct 12W in Alamance County
The semicolons are used by the code to roll up results from smaller Reporting Units into larger Reporting Units.

### Optional Conventions
The jurisdiction files in this repository follow certain conventions. Many of these are optional; using different conventions in another copy of the system will not break anything. Internal database names names are standardized as much as possible, regardless of state, following these models:

    * Office
        * `US Senate CO`
        * `US House FL District 5`
        * `PA State Senate District 1`
        * `NC State House District 22`
        * `PA Berks County Commissioner`
        * `DC City Council District 2`
        * `OR Portland City Commissioner Seat 3`
       
    * Party
        * `Constitution Party` (regardless of state or other jurisdiction)
        
### Beware of:
 - hyphens in formal names of jurisdictions or elections -- this may break the testing (since pytest options with spaces are problematic, all hypens are replaced by spaces in [test_results.py](../tests/dataloading_tests/test_results.py)))
 - Different names for same contest in different counties (if munging from a batch of county-level files)
 - Different names for candidates, especially candidates with name suffixes or middle/maiden names
 - Different "party" names for candidates without a party affiliation 
 - Any item with an internal comma (e.g., 'John Sawyer, III')
 - A county that uses all caps (e.g., Seminole County FL)
 - % signs in parameter files, particularly as web addresses for results_source (e.g.,https://elections.wi.gov/sites/elections.wi.gov/files/2020-11/UNOFFICIAL%20WI%20Election%20Results%202020%20by%20County%2011-5-2020.xlsx) -- system cannot read ini files with % signs
 - Line breaks in parameter files may interfere with software parsing the following lines
 - Files that total over all candidates (e.g., Nebraska 2020 General). Make sure not to include the totals in the counts as a nominal "candidate".
 - Excel files that show a thousands separator when you view them (2,931) but don't have a thousands separator under the hood (2931). If all your count are zero, try adding or removing the 'thousands-separator' parameter in `format.config`.
 - the parser for multi-block flat files assumes that any line without counts separates blocks. Beware of stray count-less lines (e.g., can be created by utilities pulling tables from pdfs).

The `database` submodule has a routine to remove all counts from a particular results file, given a connection to the database, a cursor on that connection and the _datafile.Id of the results file:
```
remove_vote_counts(connection, cursor, id)
```

Replace any double-quotes in Candidate.txt and dictionary.txt with single quotes. I.e., `Rosa Maria 'Rosy' Palomino`, not `Rosa Maria "Rosy" Palomino`.

Some jurisdictions combine other counts with election results in ways that can be inconvenient. E.g., from a results file for the Alaska 2016 General Election:
```
"01-446 Aurora" ,"US PRESIDENT" ,"Registered Voters" ,"NP" ,"Total" ,2486 ,
"01-446 Aurora" ,"US PRESIDENT" ,"Times Counted" ,"NP" ,"Total" ,874 ,
"01-446 Aurora" ,"US PRESIDENT" ,"Castle, Darrell L." ,"CON" ,"Total" ,5 ,
"01-446 Aurora" ,"US PRESIDENT" ,"Clinton, Hillary" ,"DEM" ,"Total" ,295 ,
```
We don't want to add the "Registered Voters" number to the contest total, but it is in the same column as the candidates.
You can specify that such rows should be dropped by using the cdf_internal_name "row should be dropped" in the dictionary, e.g.:
```
cdf_element	cdf_internal_name	raw_identifier_value
Candidate	row should be dropped	Registered Voters
Candidate	row should be dropped	Times Counted
Candidate	Castle, Darrell L	Castle, Darrell L

```

If your sheets or files have a variable number of count columns (e.g., if columns are labeled by candidates), err on the side of including extra columns in count_column_numbers. Columns without data will be ignored. Be careful, however, not to include in your count columns any columns containing strings needed for munging.

If your excel file has merged cells across lines, it may not be clear which line holds the information. Save a sheet as tab-separated text to see which line holds which information from merged cells.

If not all rows are data, and some string fields to be munged have blank headers (e.g., often the counties are in the first column without a cell above reading "County"). In this case use '<column_i>' in the munge formulas to denote the i-th column (numbering from 0, as usual). For example, if counties are in the leftmost column and the header is blank, use '<column_0>' for the name of the county. See for example `wy_gen.munger`.

If there are hidden columns in an Excel file, you may need to omit the hidden columns from various counts.

### NIST Common Data Format imports
To import results from a file that is valid NIST V2 xml -- that can be formally validated against the [NIST election results reporting schema (Version 2)](https://github.com/usnistgov/ElectionResultsReporting/raw/version2/NIST_V2_election_results_reporting.xsd) -- use the file_type 'nist_v2_xml'

Some xml files (e.g., Ohio 2020 General) use the older Version 1 common data format. Our convention is that if the munger name contains "nist" and the file_type is xml, then the system will look for a namespace declaration.

### Difference-in-Difference calculations
The system provides a way to calculate difference-in-difference statistics. For any particular election, `Analyzer.diff_in_diff_dem_vs_rep` produces a dataframe of values for any county with results by vote type, with Democratic or Republican candidates, and any comparable pair of contests both on some ballots in the county. Contests are considered "comparable" if their districts are of the same geographical district type -- e.g., both statewide, or both state-house, etc. The method also returns a list of jurisdictions for which vote counts were zero or missing.
```
dbname = "test_0314_1836"
election = "2020 General"
an = eda.Analyzer(dbname=dbname)
diff_in_diff_dem_vs_rep, missing = an.diff_in_diff_dem_vs_rep(election)
```

Specifically, for a fixed county and party, for a fixed pair of vote types and for a fixed pair of contests, we calculate the difference-in-difference value to be
```abs(abs(pct[0][0] - pct[1][0]) - abs(pct[0][1] - pct[1][1]))```
where `pct[i][j]` denotes the percentage of the total vote share earned by the party's candidate in contest `i` on ballots in the county of vote type `j`. The vote share is of the votes for all candidates, not just Democratic or Republican. However, we omit contests that don't have both Republican and Democratic candidates

For more information and context about difference-in-difference calculations for election results, see Michael C. Herron's article [Mail-In Absentee Ballot Anomalies in North Carolina's 9th Congressional District](http://doi.org/10.1089/elj.2019.0544). Note that he uses signed difference-in-difference, while we take the absolute value.
