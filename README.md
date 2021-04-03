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
  

# Contributors
 * [Stephanie Singer](http://campaignscientific.com/), Hatfield School of Government (Portland State University), former Chair, Philadelphia County Board of Elections
 * Janaki Raghuram Srungavarapu, Hatfield School of Government (Portland State University)
 * Eric Tsai, Hatfield School of Government (Portland State University)
 * Bryan Loy
 * Jon Wolgamott
 * Elliot Meyerson

# Funding
Funding provided October 2019 - September 2021 by the National Science Foundation
 * Award #1936809, "EAGER: Data Science for Election Verification" 
 * Award #2027089, "RAPID: Election Result Anomaly Detection for 2020"

# License
See [LICENSE.md](./LICENSE.md)

