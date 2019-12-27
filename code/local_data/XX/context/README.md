# About the files
## `main_reporting_units.txt`
Python dictionary with main reporting units as keys, and a dictionary of identifiertype-identifier pairs as values. The names of the main reporting units should be in the form:
`state_name`;`reporting_unit_name`
E.g., `North Carolina;Alamance County`

## `main_reporting_unit_type.txt`
This file contains one line, with the ReportingUnitType corresponding to the major subdivision of the state (usually "county"). Wherever possible, this type should be one of the enumerated ReportingUnitTypes from the Common Data Format.

## `name.txt`
The name of the state

## `parser_string.txt`
The python string to be used by the python re module to parse the lines of the metafiles.

## `schema_name.txt`
The name of the postgresql schema to hold the raw data from this state.

## `type_map.txt`
Contains any necessary translation from the data type language used in the metafiles to data type language PostgreSQL can understand.
