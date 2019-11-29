# results_analysis

## Set-up

### Docker
Docker must be installed on your system.

### .gitignore
Folders you will need in your local repo:
`code/local_data` holds your state-by-state data. Each state has a subfolder, e.g., `code/local_data/NC` for North Carolina. Each state subfolder has two subfolders, one for source data files (`code/local_data/NC/data`) and one for metadata files (`code/local_data/NC/meta`)

`code/local_logs` will receive log files as the program runs.

Other folders are optional, e.g., `local`.

## How to run the app

The following may need to be modified for your operating system. 

### On MacOS 
#### Start the servers
Using Terminal, navigate to the `code` directory. Run `docker-compose up`:

    `$ docker-compose up
    Creating network "code_default" with the default driver
    Creating code_web_1 ... done
    Creating code_db_1  ... done
    Attaching to code_web_1, code_db_1`

This Terminal window will show messages from the web and db servers.

#### Run the application
In a web browser, navigate to `localhost:5000`

#### Stop the application
In a second Terminal window, navigate to the `code` directory. Run `docker-compose down`:

    `$ docker-compose down
    Stopping code_db_1  ... done
    Stopping code_web_1 ... done
    Removing code_db_1  ... done
    Removing code_web_1 ... done
    Removing network code_default
    $`

## Getting to the container command line
For example, to get a bash command line inside the postgres container image:
    `MacBook-Pro-3:code Steph-Airbook$ docker container ls
    CONTAINER ID        IMAGE               COMMAND                  CREATED             STATUS              PORTS                    NAMES
    21ba1253b28c        postgres:11         "docker-entrypoint.s…"   14 seconds ago      Up 13 seconds       0.0.0.0:5432->5432/tcp   code_db_1
    cef03ffb9daa        code_web            "flask run"              14 seconds ago      Up 13 seconds       0.0.0.0:5000->5000/tcp   code_web_1
    MacBook-Pro-3:code Steph-Airbook$ docker exec -i -t 21ba1253b28c /bin/bash
`

## docs folder
The `docs` folder has documentation for local set-up of programs and packages that are likely to be useful for dev.

# Code components

## Docker files

### `code/requirements.txt`
Specifies necessary python packages that are not part of the standard python distribution

### `code/docker-compose.yml`
Defines two services: `db` (postgres to store data) and `web` (serves up web content). 

### `code/Dockerfile`
- Imports python and packages listed in `requirements.txt`
- specifies the python file (`app.py`) defining the content served by Flask to the web browser
- starts the web server via Flask



## `code/local_data` folder
Contains one subfolder for each state, each containing three folders:
* `data` containing data files 
* `meta` containing metadata files for the `data` files
* `external` containing necessary state-specific information from sources other than the data files

Also contains a subfolder `tmp` to hold temporary cleaned files for upload to db

## Naming conventions

