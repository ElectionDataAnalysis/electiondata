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

## Misc R-Studio
Tab is a generic auto-complete function. If you start typing in the console or editor and hit the tab key, RStudio will suggest functions or file names; simply select the one you want and hit either tab or enter to accept it.
Control + the up arrow (command + up arrow on a Mac) is a similar auto-complete tool. Start typing and hit that key combination, and it shows you a list of every command you've typed starting with those keys. Select the one you want and hit return. This works only in the interactive console, not in the code editor window.
Control + enter (command + enter on a Mac) takes the current line of code in the editor, sends it to the console and executes it. If you select multiple lines of code in the editor and then hit ctrl/cmd + enter, all of them will run.
