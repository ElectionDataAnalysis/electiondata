# Overview
This repository hopes to provide reliable tools for consolidation and analysis of raw election results from the most reliable sources -- the election agencies themselves. 
 * Consolidation: take as input election results files from a wide variety of sources and load the data into a relational database patterned after the [common data format](https://github.com/usnistgov/ElectionResultsReporting) from the National Institute of Standards and Technology (NIST)
 * Export: create tab-separated flat export files of results sets rolled up to any desired intermediate geography (e.g., by county, or by congressional district)
 * Analysis: (in progress as of 6/17/20) provide a variety of analysis tools
 * Visualization: (in progress as of 6/17/20) provide a variety of visualization tools.

# Target Audience
This system is intended to be of use to candidates and campaigns, election officials, students of politics and elections, and anyone else who is interested in assembling and understanding election results.

# How to Help
If you have skills to contribute to building the system, we can definitely use your help:
 * Creating visualizations
 * Importing and exporting data via xml feeds
 * Preparing for intake of specific states' results files
 * Managing collection of data files in real time
 * Writing documentation
 * Merging other data sets of interest (e.g., demographics)
 * Building our open source community
 * What else? Let us know!
 
If you are a potential end user -- an election official, political scientist or campaign consultant, for instance -- we would love to talk with you about what you want to from this system.
 
If you are interested in contributing, or just staying updated on the progress of this project, please [contact Stephanie Singer](http://symmetrysinger.com/index.php?id=contact). 

# How to run the app
Clone the repository to a local machine. Navigate to `/path/to/repo/election_anomaly` and run `python3 setup.py install`. 

# How to use the app
Detailed instructions can be found [here](https://github.com/sfsinger19103/election_anomaly/blob/master/docs/User_Guide.md).

## Environment
### Database
You will need `python3`. If you use the alias `python`, make sure it points to `python3`.

The system runs out of the box with a postgresql database; to use other varieties of SQL, you will need to modify the routines in the `db_routines` module. 

Store your database login credentials stored in a file (template is `src/templates/parameter_file_templates/database.par`)
  
### Jurisdiction information
Because each original raw results file comes from a particular election agency, and each election agency has a fixed jurisdiction, it is natural to organize information by jurisdiction. 

Each jurisdiction should have its own subdirectory of the directory `src/jurisdictions`.

The system assumes that internal database names of ReportingUnits carry information about the nesting of the basic ReportingUnits (e.g., counties, towns, wards, etc., but not congressional districts) via semicolons. For example: `
 * `Pennsylvania;Philadelphia;Ward 8;Division 6` is a precinct in 
 * `Pennsylvania;Philadelphia;Ward 8`, which is a ward in
 * `Pennsylvania;Philadelphia`, which is a county in
 * `Pennsylvania`, which is a state.
 
Other nesting relationships (e.g., `Pennsylvania;Philadelphia;Ward 8;Division 6` is in `Pennsylvania;PA Senate District 1`) are not yet recorded in the system (as of 6/17/2020).

## Mungers
Election result data comes in a variety of file formats. Even when the basic format is the same, file columns may have different interpretations. The code is built to ease -- as much as possible -- the chore of processing and interpreting each format. Following the [Jargon File](http://catb.org/jargon/html/M/munge.html), which gives one meaning of "munge" as "modify data in some way the speaker doesn't need to go into right now or cannot describe succinctly," we call each set of basic information about interpreting an election result file a "munger". The munger template is in the directory `src/templates/munger_templates`.

The directory `src/mungers` holds the munger directories. Each munger directory needs the following component files:
 * `format.txt` holds information about the file format
   * `count_columns` is a comma-separated list of integers identifying the columns that contain vote counts. Our convention is to count from the left of the file, with leftmost column as 0.
   * `header_row_count` is an integer, the number of header rows in the file
   * `field_name_row` is the single header row containing field names for columns that do not hold counts. (Columns containing vote counts may have field value information in more than one header row, e.g., one header row for contest and a second header row for candidate). Our convention is to count from the top of the file, with the top row as 0. If there is no field names in the file, the value should be 'None'.
   * `file_type` is one of a certain list of types recognized by the system. As of 6/4/2020, the list is:
   * `file_type` is one of a certain list of types recognized by the system. As of 6/4/2020, the list is:
     * `txt` for tab-separated text
     * `csv` for comma-separated text
     * `xls` or `xlsx` for Excel files (and any other files readable by the `read_excel` function in the `pandas` package.
   * `encoding` is the file encoding, e.g., `iso-8859-1`.
   * `thousands_separator` indicates whether one thousand is written `1000` or `1,000` in the file, or some other way. Usually `,` or `None`.
   
  * `cdf_elements.txt` One line for each main table in the database. Specifies how to read the values for that table from the source file. Columns are:
    * `name` Name of the table (e.g., 'ReportingUnit')
    * `raw_identifier_formula` Formula for creating the raw identifier from the results file. 
    * `source` Identifies the placement in the file of the relevant information. The system recognizes these possibilities:
      * `row` for classes calculated from values in same row as a given vote count value. In this case the `raw_identifier_formula` can reference entries within the row via the relevant column name in angle brackets (e.g., <COUNTY>)
      * `column` for classes calculated from values in the same column as a given vote count value. In this case the `raw_identifier_formula can reference entries within the column via the relevant row number in angle brackets (e.g., <0>)
      * If a value is the same for everything in the results file (e.g., all results in the file are of CountItemType 'election-day'), use 'row' for the source and set the formula to the constant. For example, the entry might look like: 
      ```CountItemType  election-day    row```
    
### row-sourced formula example
Consider this snippet from a comma-separated Philadelphia, Pennsylvania voting results file:
```
WARD,DIVISION,VOTE TYPE,CATEGORY,SELECTION,PARTY,VOTE COUNT
01,01,A,JUDGE OF THE SUPERIOR COURT,AMANDA GREEN-HAWKINS,DEMOCRATIC,2
01,01,M,JUDGE OF THE SUPERIOR COURT,AMANDA GREEN-HAWKINS,DEMOCRATIC,146
01,01,P,JUDGE OF THE SUPERIOR COURT,AMANDA GREEN-HAWKINS,DEMOCRATIC,0
```
The formula `Ward <WARD>;Division <DIVISION>` would yield `Ward 01;Division 01`.

### column-source formula example
Consider this snippet from a tab-separated North Carolina voting results file:
```
County	Election Date	Precinct	Contest Group ID	Contest Type	Contest Name	Choice	Choice Party	Vote For	Election Day	One Stop	Absentee by Mail	Provisional	Total Votes	Real Precinct
ALAMANCE	11/06/2018	064	1228	S	NC COURT OF APPEALS JUDGE SEAT 3	Michael Monaco, Sr.	LIB	1	59	65	2	1	127	Y
ALAMANCE	11/06/2018	03N	1228	S	NC COURT OF APPEALS JUDGE SEAT 3	Michael Monaco, Sr.	LIB	1	59	38	1	0	98	Y
ALAMANCE	11/06/2018	03S	1228	S	NC COURT OF APPEALS JUDGE SEAT 3	Michael Monaco, Sr.	LIB	1	106	108	0	3	217	Y
```
Here the CountItemType value ('Election Day','One Stop' a.k.a. early voting, 'Absentee by Mail','Provisional' must be read from the column headers, i.e., the information in row 0 of the file. For the first data row, the formula <0> would yield CountItemType 'Election Day' for the VoteCount of 59, 'One Stop' for the vote count of 65, etc.

# Code components

## About the `CDF_schema_def_info` directory:
The information in this directory determines the structure of the database created by the system to store election results information. Subdirectories and their contents are:
 * `elements` subdirectory contains a subdirectory for each main tables in the database. Most of these correspond to classes in the Common Data Format; other tables (e.g., `_datafile`) start with an underscore. 
 * `enumerations` subdirectory contains a file for each relevant enumerated list from by the Common Data Format. We treat `BallotMeasureSelection` as an enumerated list.
 * `joins` subdirectory contains a subdirectory for each join table in the database.

## Conventions
### Strings used as names and dictionary keys
Each element (each election, candidate, reporting unit, etc.) has a name -- a character string used in the `name` field in the corresponding database table, and also used in the files in the `context`  folder as keys in python dictionaries containing more info about the element. 

For ReportingUnits, the naming convention is to list as much of the composing information as possible in the name of the element, using `;` as a separator. E.g., 
 * `North Carolina` -- the state of NC
 * `North Carolina;Alamance County` -- Alamance County, which is contained in North Carolina
 * `North Carolina;Alamance County;Precinct 12W` -- Precinct 12W in Alamance County
The semicolons are used by the code to roll up results from smaller Reporting Units into larger Reporting Units.

### Numerical row and column labels start at 0
Yes, even though this choice makes the second row into "row 1". 

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
        
    * Candidate Contest
        * `Constitution Party Primary for US Senate CO`

# Contributors
 * [Stephanie Singer](http://campaignscientific.com/), Hatfield School of Government (Portland State University), former Chair, Philadelphia County Board of Elections
 * Janaki Raghuram Srungavarapu, Hatfield School of Government (Portland State University)
 * Eric Tsai, Hatfield School of Government (Portland State University)

# Funding
Funding provided October 2019 - April 2021 by the National Science Foundation
 * Award #1936809, "EAGER: Data Science for Election Verification" 
 * Award #2027089, "RAPID: Election Result Anomaly Detection for 2020"

# License
See [LICENSE.md](./LICENSE.md)

