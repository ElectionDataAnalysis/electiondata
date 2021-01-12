# How to Use the System
## Environment
You will need `python3`. If you use the alias `python`, make sure it points to `python3`.

The system runs out of the box with a postgresql database; to use other varieties of SQL, you will need to modify the routines in the `database` module. 

## Installation
From the root folder of your repository run `python3 setup.py install` (or if `python` is an alias for `python3` on your system, `python setup.py install`).

## Setting up
In the directory from which you will run the system -- which can be outside your local repository-- create the main parameter files you will need to specify paths and database connection information specific to your local computing environment:
* `run_time.ini` for loading data (DataLoader() class) and for pulling and analyzing results (the Analyzer()) class)
  
See the template file (`src/parameter_file_templates/run_time.ini.template`). 
   
## Determining a Munger
Ensure that the munger files are appropriate for your results file(s). If the munger doesn't already exist:
 * pick a name for your munger 
 * create a file with that name and extension `.munger` in the `mungers` directory (e.g., `me_excel.munger`) with sections and parameters described below. You may find it helpful to work with the template from `src/parameter_file_templates/template.munger`. 
 
 The file with munger parameters, which must have the extension `.munger`, has one or more sections, each with a header:
  * (required) `[format]` for the main parameters
  * (may be required) one section each string locations `in_field_values` and `in_count_headers` if these are listed in the `munge_strings` parameter (defined below). 
  * (may be required) one section for each element in the `lookups` list. E.g., if `lookups=Candidate,Party` then there must be  `[Candidate lookup]` and `[Party lookup]` sections. 
  * (optional) `[ignore]` Unrecognized Contests, Candidates and Parties are collected as "none or unknown". Some states (e.g., Wisconsin 2018 General) report total votes over a contest next to individual candidates' votes. The system may read, e.g., "Total Votes Cast" as an unrecognized party name. In this case include the lines:
  ```
[ignore]
Party=Total Votes Cast
```
and similarly, if necessary, for any Contest or Selection. If there is more than one "Party" to be ignored, use a comma-separated list: `Candidate=Total Votes Cast,Registered Voters`
 
 There are three main required parameters: `file_type`, `count_locations` and `munge_strings`. Depending on the values of these, there are other required and optional fields. 
 `file_type`: controls which pandas function reads the file contents. Related optional and required parameters must be given under the `[file_type]` header.
  * 'excel'
    * (optional) a list `sheets_to_read_names` (and/or `sheets_to_read_numbers`) of spreadsheets to read, 
    * (optional) a list `sheets_to_skip_names` of names of spreadsheets to skip
    * Default is to read all sheets
  * 'json-nested'
  * 'xml'
    * (required) must have `munge_strings=in_field_values` or `munge_strings=in_field_values,constant_over_file`
    * (optional) if there are tags in the vote-count hierarchy that do not themselves contain data we want, list them in the `nesting_tags` parameter, e.g., `nesting_tags=contests,choices,jurisdictions,voteTypes`
  * 'flat_text' Any tab-, comma-, or other-separated table in a plain tabular text file.
    * (required) a field delimiter `flat_text_delimiter` to be specified (usually `flat_text_delimiter=,` for csv or `flat_text_delimiter=tab` for .txt)
  * [[ will be obsolete: `concatenated-blocks` Clarity format derived from xml]]
  
  `count_locations`: controls how the system looks for counts. Related optional and required parameters must be given under the `[count_locations]` header.
  * 'by_field_names' (NB: as of 12/2020, for this case system can handle only files with only one field-name row for the count fields. If there are multiple header rows for the count columns, use the 'by_column_number' option.)
    * (required) list `count_fields_by_name` of names of fields containing counts. 
    * (required for 'excel' and 'flat_text' file_types) specify location of field names for count columns. with integer `count_field_name_row` (NB: top row not skipped is 0, next row is 1, etc.)
  * 'by_column_number'
    * (required) list `count_column_numbers` of column numbers which may contain counts. There is no problem if this list contains spurious columns.
    
  `munge_strings`: controls how the system looks for the character strings used to munge the non-count information (Candidate, Party, etc.). There may be multiple, so the value is a list. Related optional and required parameters must be given under the `[munge_strings]` header.
  * 'in_field_values'
    * (required) either:
      * if all_rows=data (i.e., no field names) list `string_field_column_numbers` of integers designating columns (leftmost column is 0, next is 1, etc.)
      * if some of the field values are foreign keys, must give lookup information. For each foreign key, need a separate section with corresponding header (field name plus " lookup", e.g. `[county_id lookup]`. This section needs:
        * `source_file` the path to the source file, relative to the results directory given in `run_time.ini`
        * all the usual basic format parameters except `count_locations` -- but not the usual formulas
        * `lookup_id` is the single field name holding the key to the lookup table. (If there are no headers in the lookup source file, use, e.g., `column_0`)
        * for each element whose formula looks something up from this table, a formula for the foreign key replacement.
       
    * (required for 'excel' and 'flat_text' file_types where not all rows are data) specify location of field names for string columns. Need integer `string_field_name_row` (NB: top row not skipped is 0, next row is 1, etc.)
  * 'in_count_headers' this is used, e.g., when each candidate has a separate column in a tabular file. In this case there may be a single header row with relevant info, or there may be several rows (e.g., Contest in one row, Candidate in another row)
    * (required) list `count_header_row_numbers` of integers for rows containing necessary character strings. (NB: top row not skipped is 0, next row is 1, etc.)
  * 'constant_over_file'
    * (required) list of items constant over file `constant_over_file=CountItemType,CandidateContest`
    * (required) the `*.ini` file for the results file must specify the values
  * 'constant_over_sheet'
    * The system can extract strings from sheet names in Excel (designated as <sheet_name>) and/or from entire rows of a spreadsheet (designated as <row_0>, e.g.)
  
   Available if appropriate for any file type, under the `[format]` header:
   * (optional) `thousands_separator`. In the US the separator is almost always ',' if it is used. Watch out for Excel files which may show a comma when you look at them in Excel -- there may not be a comma in the underlying data.
   * (optional) `encoding` (If not specified or recognized, a default encoding will be used. Recognized encodings are limited [python's list of recognized encodings and aliases](https://docs.python.org/3/library/codecs.html#standard-encodings).)

   Available for flat_text and excel file types:
   * (optional) `rows_to_skip` An integer giving the number of rows to skip at the top to get to the table of counts. This parameter will affect all integers designating rows -- e.g., '<header_0>' is the first row not skipped. This affects the numbering of rows for munge strings that are constant over the sheet as well. If `rows_to_skip = 2`, then '<row_0>' will denote the third row of the actual Excel sheet -- the highest unskipped row. The system recognizes the leftmost non-blank cell as the content to be read.
   * (optional) `all_rows` If the file has no column headers but only data rows with counts, set this parameter to 'data'
 
 
Put each formula for parsing information from the results file into the corresponding munge formula section (`[in_field_values]`, `[in_count_headers]` or `[constant_over_sheet]`. Constant items can be give either:
 * as comma separated list in constant_over_sheet parameter in .munger file, with values in the .ini file
 * as a constant formula in any of the munge formula sections, in which case a corresponding entry must be made in the jurisdictions `dictionary.txt`.
 
Sometimes the election agency provides counts by ids for, say, candidates, and also provides definitions for those ids. For example, the data may be in the NIST common data format, or it may have been exported as separate tables from a relational database. In this case the file containing the counts may have foreign keys. The lookup formulas are given in lookup sections named by each foreign key column (or set of columns). Each lookup section must have:
 * a `source_file` parameter with a path to the file with the information to be looked up
 * a `lookup_id` parameter indicating the location of the primary key in that file. 
 * for each item that uses this lookup, a formula for what should replace the foreign key in the main formula for the item. E.g., to define what should replace the foreign key in the `Candidate=` formula previously defined, the lookup section should have a `Candidate_replacement=` formula.
For example, here is a munger for Michigan 2018 General election results using lookups.
```
[format]
file_type=flat_text
count_locations=by_column_numbers
munge_strings=in_field_values

encoding=ASCII
flat_text_delimiter=tab
count_column_numbers=11
all_rows=data
constant_over_file=CountItemType

[in_field_values]
ReportingUnit=<column_6>;<column_6,column_7>;Ward <column_8>;Precinct <column_9> Label <column_10>
Party=<column_5>
CandidateContest=<column_2,column_3>
Candidate=<column_5>

[column_2,column_3 lookup]
source_file=Michigan/2018GEN/2018offc.txt
file_type=flat_text
flat_text_delimiter=tab
munge_strings=in_field_values
encoding=ASCII
all_rows=data
lookup_id=column_2,column_3
CandidateContest_replacement=<column_5>

[column_5 lookup]
source_file=Michigan/2018GEN/2018name.txt
file_type=flat_text
flat_text_delimiter=tab
munge_strings=in_field_values
encoding=ASCII
all_rows=data
lookup_id=column_5
Candidate_replacement=<column_7> <column_8> <column_6>
Party_replacement=<column_9>

[column_6 lookup]
source_file=Michigan/2018GEN/county.txt
file_type=flat_text
flat_text_delimiter=tab
munge_strings=in_field_values
encoding=ASCII
all_rows=data
lookup_id=column_0
ReportingUnit_replacement=<column_1>

[column_6,column_7 lookup]
source_file=Michigan/2018GEN/2018city.txt
file_type= flat_text
flat_text_delimiter=tab
munge_strings=in_field_values
encoding=ASCII
all_rows=data
lookup_id=column_2,column_3
ReportingUnit_replacement=<column_4>

```


 
 You may find it helpful to follow the example of the mungers in the repository.

## Formulas for parsing munge string information
For many results files, it is enough to create concatenation formulas, referencing field names from your file by putting them in angle brackets. 

### flat text `[in_field_values]` 
 Consider this snippet from a comma-separated Philadelphia, Pennsylvania voting results file:
```
WARD,DIVISION,VOTE TYPE,CATEGORY,SELECTION,PARTY,VOTE COUNT
01,01,A,JUDGE OF THE SUPERIOR COURT,AMANDA GREEN-HAWKINS,DEMOCRATIC,2
01,01,M,JUDGE OF THE SUPERIOR COURT,AMANDA GREEN-HAWKINS,DEMOCRATIC,146
01,01,P,JUDGE OF THE SUPERIOR COURT,AMANDA GREEN-HAWKINS,DEMOCRATIC,0
```
The formula `Ward <WARD>;Division <DIVISION>` would yield 'Ward 01;Division 01'.

### xml `[in_field_values]`
Consider this snippet from a Georgia voting results file:
 ```
        <Choice key="25" text="Raphael Warnock (Dem)" totalVotes="2279559">
            <VoteType name="Election Day Votes" votes="485573">
                <County name="Appling" votes="391" />
                <County name="Atkinson" votes="262" />
                <County name="Bacon" votes="162" />
```
The formula `<VoteType.name>` would yield 'Election Day Votes'.

### json-nested `[in_field_values]` 
Consider this snippet from the beginning of a Texas results file (with tabs and line breaks for clarity -- original is all one line):
 ```
{
  "48001":{
    "N":"ANDERSON","TV":29274,"C":"#ffa11e","Races":{
      "1001":{"OID":1001,"ON":"PRESIDENT/VICE-PRESIDENT","T":19155,"C":{
        "9240":{"id":9240,"N":"DONALD J. TRUMP/MICHAEL R. PENCE (I)","P":"REP","V":15062,"PE":51.45,"C":"#E30202","O":1,"EV":11103},
        "9160":{"id":9160,"N":"JOSEPH R. BIDEN/KAMALA D. HARRIS","P":"DEM","V":3934,"PE":13.44,"C":"#007BBD","O":2,"EV":3234},
        "9156":{"id":9156,"N":"JO JORGENSEN/JEREMY \"SPIKE\" COHEN","P":"LIB","V":132,"PE":0.45,"C":"#e0b30e","O":3,"EV":96}
```
The colons (:) and braces ({}) determine the nesting and the syntax. Leaving out a lot of information we have
```
{"48001":{ ... "Races":{"1001":{..."ON":"PRESIDENT/VICE-PRESIDENT" ... "C":{"9240":{..."P":"REP"...}...}...}...}...}...}
```
The party ("REP") is nested with keys `48001`, `Races`, `1001`, `C`, `9240` and `P`. The formula `Party=<C.P>` tells the system to find the string whose nested keys contain `C` and then `P`, in that order. Note that json files from other jurisdictions may have different nesting conventions. 

### flat text `[in_count_headers]`
Consider this snippet from a tab-separated North Carolina voting results file:
```
County	Election Date	Precinct	Contest Group ID	Contest Type	Contest Name	Choice	Choice Party	Vote For	Election Day	One Stop	Absentee by Mail	Provisional	Total Votes	Real Precinct
ALAMANCE	11/06/2018	064	1228	S	NC COURT OF APPEALS JUDGE SEAT 3	Michael Monaco, Sr.	LIB	1	59	65	2	1	127	Y
ALAMANCE	11/06/2018	03N	1228	S	NC COURT OF APPEALS JUDGE SEAT 3	Michael Monaco, Sr.	LIB	1	59	38	1	0	98	Y
ALAMANCE	11/06/2018	03S	1228	S	NC COURT OF APPEALS JUDGE SEAT 3	Michael Monaco, Sr.	LIB	1	106	108	0	3	217	Y
```
Here the CountItemType value ('Election Day','One Stop' a.k.a. early voting, 'Absentee by Mail','Provisional' must be read from the column headers, i.e., the information in row 0 of the file. For the first data row, the formula <header_0> would yield CountItemType 'Election Day' for the VoteCount of 59, 'One Stop' for the vote count of 65, etc.

### excel `[constant_over_sheet]`
To be read automatically, information that is constant over a sheet must be read either from the sheet name (using `<sheet_name>`) or from the left-most, non-blank entry in a row of the sheet using `<row_j>`, where `j` is the row number. Row numbers start with `0` after skipping the number of rows given in `rows_to_skip`.

### Values constant over a file
Whenever an element is constant for all data in a results file, you can specify it in the parameter (`*.ini`) file. 
Maine publishes results in separate files for each contest, and the contests are not specified in the file contents. Here is an example from a file for the 2018 Governor contest.
```
		Hayes, Teresea M.	Mills, Janet T.	Moody, Shawn H.	Others	Blank	
		Buckfield	Farmington	Gorham			
		Independent	Democratic	Republican			
CTY	TOWN						TOTAL VOTES CAST
AND	Auburn	700	4,578	4,235	13	229	9,755
AND	Durham	113	926	1,141	0	79	2,259
AND	Greene	151	620	1,249	0	28	2,048
AND	Leeds	85	372	565	0	16	1,038
AND	Lewiston	962	7,056	5,497	13	350	13,878
```
In this case the munger needs the line `constant_over_file=CandidateContest` 
while the `*.ini` file for the file containing the Governor contest results has the line `CandidateContest=ME Governor`. 
Note what is capitalized and what is not. The CandidateContest name (here `ME Governor`) should match the internal database name given in `CandidateContest.txt`.

Another option is to specify the element with a constant concatenation formula in any one of the other munge string locations. E.g., 
```
`[in_field_values]`
ReportingUnit=<CTY>;<TOWN>
CandidateContest=Guv
```
In this case there must be an entry in `dictionary.txt` matching the constant formula to the internal database name given in `CandidateContest.txt`:
```CandidateContest	ME Governor	Guv```

### Regular Expressions
Sometimes it is necessary to use regular expressions to extract information from fields in the results file. For example, a single field might hold a candidate name along with the candidate's party.

Some jurisdictions require regular expression (regex) analysis to extract information from the data. For example, in a primary both the Party and the Candidate may be in the same string (e.g., "Robin Perez (DEM)"). Curly brackets indicate that regex analysis is needed. Inside the curly brackets there are two parts, separated by a comma. The first part is the concatenation formula for creating a string from the data in the file. The second is a python regex formula whose first group (enclosed by parentheses) marks the desired substring.
```Party	{<header_1>,^.*\(([a-zA-Z]{3})\)$}	row```

The system will report (in the `.warnings` files) any strings that did not match the regex. 


## Create or Improve a Jurisdiction
It's easiest to use the JurisdictionPrepper() object to create or update jurisdiction files.

 (0) Create a `jurisdiction_prep.ini` file, following the example in `src/parameter_file_templates/jurisdiction_prep.ini.template`. You will need to specify the number of congressional, state house and state senate districts.

 (1) From the directory containing `jurisdiction_prep.ini`, open a python interpreter. Import the package and initialize a JurisdictionPrepper(), e.g.:
```
>>> import election_data_analysis as ea
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
Counties must be added by hand. (NB: in some states, the word 'county' is not used. For instance, Louisiana's major subdivisions are called 'parish'.)

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
  * `Election.txt`.

 (6) Revise `XX_starter_dictionary.txt` so that it has entries for any of the items created in the steps above (except that there is no need to add Elections to the dictionary, as they are never munged from the contents of the results file). The 'cdf_internal_name' column should match the names in the jurisdiction files. The 'raw_identifier_value' column should hold the corresponding names that will be created from the results file via the munger. 
    * It is helpful to edit the starter dictionary in an application where you can use formulas, or to manipulate the file with regular expression replacement. If you are not fluent in manipulating text some other way, you may want to use Excel and its various text manipulation formulas (such as =CONCAT()). However, beware of Excel's tendency to revise formats on the sly. You may want to check `.txt` and `.csv` files manipulated by Excel in a plain text editor if you run into problems. (If you've been curious to learn regex replacement, now's a good time!)
 
 (7) Add entries to the starter dictionary for CountItemType and BallotMeasureSelection. 
    * Internal database names for the BallotMeasure Selections are 'Yes' and 'No'. There are no alternatives.
    * Some common standard internal database names for CountItemTypes are 'absentee', 'early', 'election-day', 'provisional' and 'total'. You can look at the CountItemType table in the database to see the full list, and you can use any other name you like.
```
cdf_element	cdf_internal_name	raw_identifier_value
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

 (12) If you want to add elements (other than ReportingUnits) in bulk from all results files in a directory (with `.ini` files in that same directory -- see Load Data below), use  `add_elements_from_multi_results_file(<list of elements>,<directory>, <error>)`. For example:
```
>>> jp.add_elements_from_multi_results_file(['Candidate'],'/Users/singer3/Documents/Temp/000_to-be-loaded',err)
```
Corresponding entries will be made in `dictionary.txt`, using the munged name for both the `cdf_internal_name` and the `raw_identifier_value`. Note:

   * Candidate
      * In every file enhanced this way, look for possible variant names (e.g., 'Fred S. Martin' and 'Fred Martin' for the same candidate in two different counties. If you find variations, pick an internal database name and put a line for each raw_identfier_value variation into `dictionary.txt`.
      * Look for non-candidate lines in the file. Take a look at `Candidate.txt` to see if there are lines (such as undervotes) that you may not want included in your final results. 
      * Look in `Candidate.txt` for BallotMeasureSelections you might not have noticed before. Remove these from `Candidate.txt` and revise their lines in `dictionary.txt`.   
      * Our convention for internal names for candidates with quoted nicknames is to use single quotes. Make sure there are no double-quotes in the Name column in `Candidate.txt` and none in the cdf_internal_name column of `dictionary.txt`. E.g., use `Rosa Maria 'Rosy' Palomino`, not `Rosa Maria "Rosy" Palomino`. However, if your results file has `Rosa Maria "Rosy" Palomino`, you will need double-quotes in the raw_identifier column in `dictionary.txt`:
      * Our convention for internal names for multiple-candidate tickets (e.g., 'Biden/Harris' is to use the full name of the top candidate, e.g., 'Joseph R. Biden'). There should be a line in `dictionary.txt` for each variation used in the results files.
```
cdf_element	cdf_internal_name	raw_identifier_value
Candidate	Joseph R. Biden	Biden / Harris
Candidate	Joseph R. Biden	Joseph R. Biden
Candidate	Rosa Maria 'Rosy' Palomino	Rosa Maria "Rosy" Palomino
```

   * CandidateContest: Look at the new `CandidateContest.txt` file. Many may be contests you do *not* want to add -- the contests you already have (such as congressional contests) that will have been added with the raw identifier name. Some may be BallotMeasureContests that do not belong in `CandidateContest.txt`. For any new CandidateContest you do want to keep you will need to add the corresponding line to `Office.txt` (and the ElectionDistrict to `ReportingUnit.txt` if it is not already there). 
    * You may want to remove from `dictionary.txt` any lines corresponding to items removed in the bullet points above.

 (13) Finally, if you will be munging primary elections, and if you are confident that your `CandidateContest.txt`, `Party.txt` and associated lines in `dictionary.txt` are correct, use the `jp.add_primaries_to_candidate_contest()` and `jp.add_primaries_to_dict()` methods
```
>>> jp.add_primaries_to_candidate_contest()
>>> jp.add_primaries_to_dict()
```

### The JurisdictionPrepper class details
There are routines in the `JurisdictionPrepper()` class to help prepare a jurisdiction.
 * `JurisdictionPrepper()` reads parameters from the file (`jurisdiction_prep.ini`) to create the directories and basic necessary files. 
 * `new_juris_files()` builds a directory for the jurisdiction, including starter files with the standard contests. It calls some methods that may be independently useful:
   * `add_standard_contests()` creates records in `CandidateContest.txt` corresponding to contests that appear in many or most jurisdictions, including all federal offices as well as state house and senate offices. 
   * `add_primaries_to_candidate_contest()` creates a record in `CandidateContest.txt` for every CandidateContest-Party pair that can be created from `CandidateContest.txt` entries with no assigned PrimaryParty and `Party.txt` entries. (Note: records for non-existent primary contests will not break anything.) 
   * `starter_dictionary()` creates a `starter_dictionary.txt` file in the current directory. Lines in this starter dictionary will *not* have the correct `raw_identifier_value` entries. Assigning the correct raw identifier values must be done by hand before proceeding.
 * `add_primaries_to_dict()` creates an entry in `dictionary.txt` for every CandidateContest-Party pair that can be created from the CandidateContests and Parties already in `dictioary.txt`. (Note: entries in `dictionary.txt` that never occur in your results file won't break anything.)
 * Adding precincts automatically:
     *`add_sub_county_rus_from_results_file()` is useful when:
         * county names can be munged from the rows
         * precinct (or other sub-county reporting unit) names can be munged from the rows
         * all counties are already in `dictionary.txt`
   
       can be read from _rows_ of the datafile. The method adds a record for each precinct to `ReportingUnit.txt` and `dictionary.txt`, with internal db name obeying the semicolon convention. For instance, if:
         * `ReportingUnit\tFlorida;Alachua County\tAlachua` is in `dictionary.txt
         * County name `Alachua` and precinct name `Precinct 1` can both be munged from the same row of the results file
     
         then:
         * `Florida;Alachua County;Precinct 1\tprecinct` will be added to `ReportingUnit.txt`
         * `ReportingUnit\tFlorida;Alachua County;Precinct 1\tAlachua;Precinct 1` will be added to `dictionary.txt`
     * `add_sub_county_rus_from_multi_results_file(directory)` does the same for every results file/munger in the directory named in a `.ini` file in the directory.
 * adding other elements automatically:
     * `add_elements_from_results_file(result_file,munger)` pulls raw identifiers for all instances of the element from the datafile and inserts corresponding rows in `<element>.txt` and `dictionary.txt`. These rows may have to be edited by hand to make sure the internal database names match any conventions (e.g., for ReportingUnits or CandidateContests, but maybe not for Candidates or BallotMeasureContests.)
     * `add_elements_from_multi_results_file(directory)` does the same for every file/munger in the directory named in a `.ini` file in the directory
 
## Load Data
In the `results_dir` directory indicated in `run_time.ini`, create a `.ini` file for each results file you want to use. The file `src/parameter_file_templates/results.ini.template` is a template for the individual `.ini` files.  The results files and the `.ini` files must both be in the directory specified in the 'results_dir' parameter in `run_time.ini`. The files can have arbitrary names.

If all the `.ini` files in a single directory will use the same munger, jurisdiction and election, you can use `make_par_files` to create these `.ini` files in batches. For example, 
```
>>> dir = '/Users/singer3/Documents/Data/Florida/Precinct-Level Election Results/precinctlevelelectionresults2016gen'
>>> munger = 'fl_gen_by_precinct'
>>> jurisdiction_path = '/Users/singer3/Documents/Data_Loading/Florida'
>>> top_ru='Florida'
>>> election = '2016 General'
>>> date = '2020-08-09'
>>> source = 'Florida Board of Elections: https://dos.myflorida.com/elections/data-statistics/elections-data/precinct-level-election-results/'
>>> note = 'These statewide compiled files are derived from county-specific data submitted by supervisors of elections after each primary election, general election, special primary, and special general election and presidential preference primary election'
>>> ea.make_par_files(dir,munger, jurisdiction_path, top_ru, election, date, source=source, results_note=note)
>>> 
```
  
The DataLoader class allows batch uploading of all data in a given directory. That directory should contain the files to be uploaded, as well as a `.ini` file for each file to be uploaded. See `templates/parameter_file_templates/results.ini.tempate`. You can use `make_par_files()` to create parameter files for multiple files when they share values of the following parameters:
 * directory in which the files can be found
 * munger
 * jurisdiction
 * election
 * download_date
 * source
 * note
The `load_all()` method will read each `.ini` file and make the corresponding upload.
From a directory containing a `run_time.ini` parameter file, run
```
import election_data_analysis as ea
dl = ea.DataLoader()
dl.load_all()
```

Some results files may need to be munged with multiple mungers, e.g., if they have combined absentee results by county with election-day results by precinct. If the `.ini` file for that results file has `munger_name` set to a comma-separated list of mungers, then all those mungers will be run on that one file.

If every file in your directory will use the same munger(s) -- e.g., if the jurisdiction offers results in a directory of one-county-at-a-time files, such AZ or FL -- then you may want to use `make_par_files()`, whose arguments are:
 * the directory holding the results files,
 * the munger name (for multiple mungers, pass a string that is a comma-separated list of munger names)
 * jurisdiction (can be, e.g., 'Florida' as long as every file has Florida results)
 * election (has to be just one election),
 * download_date
 * source
 * note (optional)
 * aux_data_dir (optional -- use it if your files have all have the same auxiliary data files, which might never happen in practice)


### Error reporting
System errors will be printed as the system runs.

Fatal errors related to the jurisdiction or the munger will be noted in a file `*.errors` named after the results `*.ini` file. 

Even when the upload has worked, there may be warnings about lines not loaded. The system will ignore lines that cannot be munged. For example, the only contests whose results are uploaded will be those in the `CandidateContest.txt` or `BallotMeasureContest.txt` files that are correctly described in `dictionary.txt`.

If there are no errors, the results and their `.ini` files will be moved to the archive directory specified in `run_time.ini`. Any warnings for the `*.ini` will be saved in the archive directory in a file `*.warn`.

## Pull Data
The Analyzer class uses parameters in the file `run_time.ini`, which should be in the directory from which you are running the program. This class has a number of functions that allow you to aggregate the data for analysis purposes. For example, running the `.top_counts()` function exports files into your rollup directory which with counts summed up at a particular reporting unit level. This function expects 4 arguments: the election, the jurisdiction, the reporting unit level at which the aggregation will occur, and a boolean variable indicating whether you would like the data aggregated by vote count type. For example:
```
from election_data_analysis import Analyzer
analyzer = Analyzer()
analyzer.top_counts('2018 General', 'North Carolina', 'county', True)
```
This code will produce all North Carolina data from the 2018 general election, grouped by contest, county, and vote type (total, early, absentee, etc).

## Unload and reload data
To unload existing data for a given jurisdiction and a given election -- or more exactly, to remove data from any datafiles with that election and that jurisdiction as "top ReportingUnit" -- you can use the routine 
```user_interface.reload_juris_election(juris_name,election_name,test_dir)```

where `test_dir` is the directory holding the tests to perform on the data before upload. For example, `test_dir` might be the repository's `tests` directory. This routine will move any files associated with unloaded data to the directory specified in the optional `unloaded_dir` in `run_time.ini`.

## Testing
The routine `tests/load_and_test_all.py` can be used to run tests. If the directory `tests/TestingData` does not exist, the function will download files from `github.com/ElectionDataAnalysis/TestingData`, load it all and run all tests. If `tests/TestingData` exists, the routine will test all data in that directory (without downloading anything). Election-jurisdiction pairs can be specified with the -e and -j flags, e.g. `load_all_from_repo.py -e '2018 General' -j 'Arkansas'` to restrict the loading and testing to just that pair.


## Miscellaneous helpful hints
Beware of:
 - Different names for same contest in different counties (if munging from a batch of county-level files)
 - Different names for candidates, especially candidates with name suffixes or middle/maiden names
 - Different "party" names for candidates without a party affiliation 
 - Any item with an internal comma (e.g., 'John Sawyer, III')
 - A county that uses all caps (e.g., Seminole County FL)
 - % signs in .ini files, particularly as web addresses for results_source (e.g.,https://elections.wi.gov/sites/elections.wi.gov/files/2020-11/UNOFFICIAL%20WI%20Election%20Results%202020%20by%20County%2011-5-2020.xlsx)
 - Files that total over all candidates (e.g., Nebraska 2020 General). Make sure not to include the totals in the counts as a nominal "candidate".
 - Excel files that show a thousands separator when you view them (2,931) but don't have a thousands separator under the hood (2931). If all your count are zero, try adding or removing the 'thousands-separator' parameter in `format.config`.

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

### NIST Exports
This package also provides functionality to export the data according to the [NIST](https://nvlpubs.nist.gov/nistpubs/SpecialPublications/NIST.SP.1500-100r2.pdf) common data format specifications. This is as simple as identifying an election and jurisdiciton of interest:
```
from election_data_analysis import Analyzer
analyzer = Analyzer()
election_report = analyzer.export_nist("2020 General", "Georgia")
```
