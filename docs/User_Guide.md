# How to Use the System

## Get Started
 * In a python interpreter, import the `election_anomaly` module. 
```python
>>> import election_anomaly as ea
```
Some routines in the Analyzer class are useful even in the data-loading process, so  create an analyzer before you start loading data.
```python
>>> an = ea.Analyzer()
>>> 
```

## Load Data
Create a DataLoader instance and check for errors in the Jurisdiction and Munger directories specified in `run_time.par`
```python
>>> phila=ea.DataLoader();phila.check_errors()
(None, None, None, None, None)
>>> 
```
If all errors are `None`, the DataLoader initialization will have loaded information in the Jurisdiction folder (e.g, contest and candidate names) into the database. You are ready to process the results file. The five error categories are: 

 0. parameter errors 
 1. missing files or consistency errors in existing jurisdiction directory
 2. errors due to non-existent jurisdiction directory
 3. errors arising during loading of jurisdiction data to the database
 4. errors in the munger. 

Insert information about your results file into the database. 
```python
>>> phila.track_results('phila_2018g','2018 General')
>>> 
```
The arguments are a shorthand name for your results file and the name of the election. The name of the election must match the name of an election already in the database. To find all available election names, use the analyzer:
```python
>>> an.display_options('election')
['2018 General', 'none or unknown']
>>> 
```
Finally, load results to the database. Even on a fast laptop this may take a few minutes for, say, a 7MB file. 
```python
>>> phila.load_results()
Datafile contents uploaded to database Engine(postgresql://postgres:***@localhost:5432/Combined_0608)
>>> 
```
Note that only lines with data corresponding to contests, selections and reporting units listed in the Jurisdiction directory will be processed. 

## Pull Data
There are two options: pulling total vote counts, or pulling vote counts by vote type. To pull totals, use `top_counts()` in the Analyzer class.
```python
>>> an.top_counts('Pennsylvania;Philadelphia','ward')
Results exported to /Users/user/Documents/rollups/2018 General/Pennsylvania;Philadelphia/by_ward/TYPEall_STATUSunknown.txt
```

To pull totals by vote type, use `top_counts_by_vote_type()`
```python
>>> an.top_counts_by_vote_type('Pennsylvania;Philadelphia','ward')
Results exported to /Users/singer3/Documents/rollups/2018 General/Pennsylvania;Philadelphia/by_ward/TYPEmixed_STATUSunknown.txt
>>> 
```

Results are exported to the `rollup_directory` specified in `run_time.par`.

Note that both arguments -- the name of the top reporting unit ('Pennsylvania;Philadelphia') and the reporting unit type for the breakdown of results ('ward') must be the internal database names. To see the list of options, use `display_options()`:
```python
>>> an.display_options('reporting_unit_type')
['ballot-batch', 'ballot-style-area', 'borough', 'city', 'city-council', 'combined-precinct', 'congressional', 'country', 'county', 'county-council', 'drop-box', 'judicial', 'municipality', 'polling-place', 'precinct', 'school', 'special', 'split-precinct', 'state', 'state-house', 'state-senate', 'town', 'township', 'utility', 'village', 'vote-center', 'ward', 'water', 'other']
```
Lists of reporting units will be quite long, in which case searching by substring can be useful.
```python
>>> counties = an.display_options('county')
>>> [x for x in counties if 'Phila' in x]
['Pennsylvania;Philadelphia']
>>> 
```
Here we have used the capability of `display_options()` to take as an argument either a general database category ('reporting unit') or a type ('county'). 

## Create or Repair a Jurisdiction
If the `juris_name` given in `run_time.par` does not exist,`DataLoader()` will create a folder for that jurisdiction, with template files and record error. Then `check_error()` will show the errors. Before proceeding, edit the jurisdiction files appropriately for your jurisdiction. The system may detect errors.

Once all errors are fixed, you are ready to load data as above.

## Create or Repair a Munger
If the munger given in `run_time.par` does not exist, `DataLoader()` will create a folder for that munger, with template files and record the error. Then `check_error()` will show the errors. E.g.
```python
>>> import election_anomaly as ea
>>> phila=ea.DataLoader(); phila.check_errors()
(None, None, None, None, {'newly_created': '/Users/username/PycharmProjects/results_analysis/src/mungers/phila_general2018, cdf_elements.txt, format.txt'})
>>> 
```
Before proceeding, edit the munger files appropriately for your data file. The system may detect errors in your files. E.g.
```python
>>> phila=ea.DataLoader();phila.check_errors()
(None, None, None, None, {'format.txt': {'format_problems': 'Wrong number of rows in format.txt. \nFirst column must be exactly:\nheader_row_count\nfield_name_row\ncount_columns\nfile_type\nencoding\nthousands_separator'}})
>>> 
```
Once all errors are fixed, you are ready to load data as above.
```python
>>> phila=ea.DataLoader();phila.check_errors()
(None, None, None, None, None)
>>> 
```

