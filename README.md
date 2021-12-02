[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/4078/badge)](https://bestpractices.coreinfrastructure.org/projects/4078)


# Overview
This repository provides tools for consolidation and analysis of raw election results from the most reliable sources -- the election agencies themselves. 
 * Consolidation: take as input election results files from a wide variety of sources and load the data into a relational database
 * Export: create consistent-format export files of results sets rolled up to any desired intermediate geography
   * tabular (tab-separated text)
   * xml (following NIST Election Results Reporting Common Data Format V2)
   * json (following NIST Election Results Reporting Common Data Format V2)
 * Analysis: 
   * Curates one-county outliers of interest
   * Calculates difference-in-difference for results available by vote type
 * Visualization: 
   * Scatter plots
   * Bar charts

# Target Audience
This system is intended to be of use to news media, campaigns, election officials, students of politics and elections, and anyone else who is interested in assembling and understanding election results.

# How to Contribute Code
Please contribute code that works in python 3.9, with the package versions specified in [requirements.txt](requirements.txt). We follow the [black](https://pypi.org/project/black/) format.

# How to Help in Other Ways
If you have skills to contribute to building the system, we can definitely use your help:
 * Creating visualizations
 * Preparing for intake of specific states' results files
 * Managing collection of data files in real time
 * Writing documentation
 * Merging other data sets of interest (e.g., demographics)
 * Building our open source community
 * What else? Let us know!
 
If you are a potential end user -- an election official, political scientist or campaign consultant, for instance -- let us know what you want to from this system.
 
If you are interested in contributing, or just staying updated on the progress of this project, please [contact Stephanie Singer](http://symmetrysinger.com/index.php?id=contact). 

# How to use the app
See [documentation directory](docs), which includes
 * for users
   * [Installation instructions](docs/Installation.md)
   * Instructions for a [sample dataloading session](docs/Sample_Dataloading_Session.md)
   * Detailed [User Guide](docs/User_Guide.md)
 * for developers
   * [Information about the code](docs/About_the_Code.md)
   * [Information about the tests](docs/Testing_Code_with_pytest.md)
   
# Contributors
 * [Stephanie Singer](http://campaignscientific.com/), Hatfield School of Government (Portland State University), former Chair, Philadelphia County Board of Elections
 * Janaki Raghuram Srungavarapu, Hatfield School of Government (Portland State University)
 * Eric Tsai, Hatfield School of Government (Portland State University)
 * Bryan Loy
 * Jon Wolgamott
 * Elliot Meyerson

# Funding
Funding provided October 2019 - November 2021 by the National Science Foundation
 * Award #1936809, "EAGER: Data Science for Election Verification" 
 * Award #2027089, "RAPID: Election Result Anomaly Detection for 2020"
Data collection and consolidation for the 2020 US General Election funded in part by the Verified Voting Foundation.

# License
See [LICENSE.md](./LICENSE.md)

