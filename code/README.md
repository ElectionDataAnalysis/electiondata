# Code components

## Docker files

### requirements.txt
Specifies necessary python packages that are not part of the standard python distribution

### docker-compose.yml
Defines two services: `db` (postgres to store data) and `web` (serves up web content). 

### Dockerfile
- Imports python and packages listed in `requirements.txt`
- specifies the python file (`app.py`) defining the content served by Flask to the web browser
- starts the web server via Flask


## Python files
### warnings


## `local_data` folder
Contains one subfolder for each state, each containing folders `meta` and `data` where the original state metadata and state data (respectively) reside.

Also contains a subfolder `tmp` to hold temporary cleaned files for upload to db


