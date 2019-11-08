# How to use r-studio after docker is installed

## Prerequisites
- Path to your project's home directory on your local machine (e.g., `~/gits/results_analysis/rstudio_sandbox`), called `path/to/project` below
- Path to a directory within the docker container, e.g., `/home/rstudio/sandbox`
- Pick a password for rStudio (e.g., `not.secure`) called `rstudio_password` below

## To open a docker container running rStudio
Following tutorial at `https://ropenscilabs.github.io/r-docker-tutorial/`
- In terminal window (after replacing `path/to/project` with the path to your project's home directory:  `$ docker run -e PASSWORD='rstudio_password' --rm -p 8787:8787 -v path/to/project:/home/rstudio/sandbox rocker/verse`
- Open browser, point to `localhost:8787`. RStudio sign in page should appear
- Log into RStudio with username `rstudio` and password (e.g. `not.secure` )

## To close a docker container running rStudio
Following tutorial at `https://ropenscilabs.github.io/r-docker-tutorial/`
- Close all browser tabs pointing to `localhost:8787`
- In terminal window where container was opened: `$ ctrl-C`


## delete below (temporary workspace)

docker run -e PASSWORD='rstudio_password' --rm -p 8787:8787 -v ~/Documents/CampaignScientific/NSF2019/State_Data/results_analysis/rstudio_sandbox:/home/rstudio/sandbox rocker/verse
