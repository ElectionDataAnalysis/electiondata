# R Cheat Sheet

## Package commands
- `find.packages('package_name')` shows the location of the named package
- `install.packages('package_name')`
- `library('package_name')` or `library(package_name)` loads the package for use

## Useful packages
- `outliers` package has various tests for outliers
- `obdc` for connecting to postgresql, mysql instances
- `RPostgreSQL` for connecting to postgres instances
- `getpass` will prompt for password if authentication is desired

## `obdc` prerequisites
Following source  `https://www.rdocumentation.org/packages/odbc/versions/1.0.1` 
- Install a certain prerequisite (driver?) on macOS:`$ brew install unixodbc` See source for other systems
- Install driver for postgres on macOS: `$ brew install psqlodbc` See source for other systems
- Install driver and db for mysql on macOS: `$ brew install mysql` See source for other systems

