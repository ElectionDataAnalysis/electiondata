# Installation Instructions

## Environment
You will need:
 * `python3.9` 
 * `postgresql`
 * all python packages given in [requirements.txt](../requirements.txt), and any packages those packages may require

Not absolutely required, but recommended, is a package manager, such as `homebrew` for macOS.

### python3.9   
If your environment has a `python` command without a specified version, that `python` may or may not point to `python3.9`. In linux-flavored shells, you can check the version with the command `python --version`. Similarly, your system may have a `python3` command, whose version can be checked with `python3 --version`.

### postgresql
If postgresql is present, the command `postgres --version` will yield the version number; otherwise the command will fail. The `electiondata` package has been tested with postgres version 13, but probably any reasonably recent version will do. If `postgresql` is not present, you should be able to install it with any reasonable package manager. (On macOS with package manager `homebrew`, use the command `brew install postgresql`) The default values you will need to connect to your `postgresql` instance are:
 * host: `localhost`
 * port: `5432`
 * user: `postgres`
 * password: (leave the password blank)

### python packages
To install the required packages, run `python3.9 -m pip install -r requirements.txt` from the [root folder of the repository](../).  Because some of the required packages have requirements of their own, which may or may not be installed already, your system prompt you to install some other packages. If so, install the suggested packages and try `python3.9 -m pip install -r requirements.txt` again.

## Installation
From the [root folder of the repository](../) run `python3.9 setup.py install`. (You may be able to use `python setup.py install` or `python3 setup.py install` instead, if those point to `python3.9`, as described above.)