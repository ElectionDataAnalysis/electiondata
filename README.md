[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/4078/badge)](https://bestpractices.coreinfrastructure.org/projects/4078)


# Overview
This repository hopes to provide reliable tools for consolidation and analysis of raw election results from the most reliable sources -- the election agencies themselves. 
 * Consolidation: take as input election results files from a wide variety of sources and load the data into a relational database
 * Export: create tab-separated flat export files of results sets rolled up to any desired intermediate geography (e.g., by county, or by congressional district)
 * Analysis: provide a variety of analysis tools
 * Visualization: provide a variety of visualization tools.

# Target Audience
This system is intended to be of use to candidates and campaigns, election officials, students of politics and elections, and anyone else who is interested in assembling and understanding election results.

# How to Contribute Code
Please contribute code that works in python 3.7, with the package versions specified in [requirements.txt](requirements.txt). We follow the [black](https://pypi.org/project/black/) format.

# How to Help in Other Ways
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

# How to use the app
Detailed instructions can be found [here](docs/User_Guide.md).
  
### Jurisdiction
Because each original raw results file comes from a particular election agency, and each election agency has a fixed jurisdiction, it is natural to organize information by jurisdiction. 

The system assumes that internal database names of ReportingUnits carry information about the nesting of the basic ReportingUnits (e.g., counties, towns, wards, etc., but not congressional districts) via semicolons. For example: `
 * `Pennsylvania;Philadelphia;Ward 8;Division 6` is a precinct in 
 * `Pennsylvania;Philadelphia;Ward 8`, which is a ward in
 * `Pennsylvania;Philadelphia`, which is a county in
 * `Pennsylvania`, which is a state.
 
Other nesting relationships (e.g., `Pennsylvania;Philadelphia;Ward 8;Division 6` is in `Pennsylvania;PA Senate District 1`) are not yet recorded in the system (as of 6/17/2020).

## Mungers
Election result data comes in a variety of file formats. Even when the basic format is the same, file columns may have different interpretations. The code is built to ease -- as much as possible -- the chore of processing and interpreting each format. Following the [Jargon File](http://catb.org/jargon/html/M/munge.html), which gives one meaning of "munge" as "modify data in some way the speaker doesn't need to go into right now or cannot describe succinctly," we call each set of basic information about interpreting an election result file a "munger". The munger template is in the directory `src/templates/munger_templates`.

Each munger directory needs the following component files:
 * `format.txt` holds information about the file format
 * `cdf_elements.txt` holds formulas for parsing information from the fields of the results file
 
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
 * Bryan Loy
 * Jon Wolgamott
 * Elliot Meyerson

# Funding
Funding provided October 2019 - April 2021 by the National Science Foundation
 * Award #1936809, "EAGER: Data Science for Election Verification" 
 * Award #2027089, "RAPID: Election Result Anomaly Detection for 2020"

# License
See [LICENSE.md](./LICENSE.md)

