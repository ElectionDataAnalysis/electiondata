import electiondata.constants
import electiondata.juris
from electiondata import (
    database as db,
    juris as jm,
    userinterface as ui,
    munge as m,
    analyze as an,
    nist,
    visualize as viz,
    otherdata as exd,
    multielection as multi,
    constants,
)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session, engine
from typing import List, Dict, Optional, Any, Tuple, Union, Iterable
import datetime
import os
import re
import pandas as pd
import inspect
from pathlib import Path
import xml.etree.ElementTree as ET
import itertools
import shutil
import json

# nb: jurisdiction_path is for backward compatibility

# classes
class SingleDataLoader:
    def __init__(
        self,
        results_dir: str,
        results_param_file: str,
        session: Session,
        mungers_path: str,
        juris_true_name: str,
        path_to_jurisdiction_dir: str,
    ):
        """
        Inputs:
            results_dir: str, path to directory containing election result files
            results_param_file: str, path to file containing basic parameters for reading result file (*.ini)
            session: Session,
            mungers_path: str, path to directory containing munger parameter files with extension .munger
            juris_true_name: str, name of jurisdiction (with spaces, not hyphens)
            path_to_jurisdiction_dir: str, path to directory containing subdirectories labeled by jurisdiction

        Returns SingleDataLoader instance with attributes:
            session
            results_dir
            juris_true_name
            juris_system_name, name of jurisdiction with hyphens, not spaces
            param_file (for results file)
            path_to_jurisdiction_dir, path to directory containing the particular jurisdiction's files
            d, dictionary of other parameters read from the param_file
            mungers_path, path to directory containing munger files
            munger_list, list of mungers to apply
            munger_err, error dictionary
        """
        # adopt passed variables needed in future as attributes
        self.session = session
        self.results_dir = results_dir
        self.juris_true_name = juris_true_name
        self.param_file = results_param_file

        # calculate useful parameters
        self.juris_system_name = jm.system_name_from_true_name(self.juris_true_name)
        self.path_to_jurisdiction_dir = path_to_jurisdiction_dir

        # grab parameters (known to exist from __new__, so can ignore error variable)
        self.d, _ = ui.get_parameters(
            required_keys=constants.sdl_pars_req,
            optional_keys=constants.sdl_pars_opt,
            param_file=results_param_file,
            header="election_results",
        )

        # change any blank parameters describing the results file to 'none'
        for k in self.d.keys():
            if self.d[k] == "" and k[:8] == "results_":
                self.d[k] = "none"

        # pick mungers (Note: munger_list is comma-separated list of munger names)
        self.mungers_path = mungers_path
        self.munger_list = [x.strip() for x in self.d["munger_list"].split(",")]

    def track_results(self) -> (int, int, Optional[dict]):
        """
        Looks up Ids for jurisdiction in the ReportingUnit table and election in the
            Election table of the session database.
            Inserts a record for the results file into the _database table.
        Returns:
            int,  _datafile.Id (or 0 if none found)
            int, Election.Id (or 0 if none found)
            Optional[dict], error dictionary
        """
        err = None
        filename = self.d["results_file"]

        # find id for jurisdiction in ReportingUnit table (or return fatal error)
        jurisdiction_id = db.name_to_id(
            self.session, "ReportingUnit", self.d["jurisdiction"]
        )
        if jurisdiction_id is None:
            err = ui.add_new_error(
                err,
                "ini",
                self.param_file,
                f"No ReportingUnit named {self.d['jurisdiction']} found in database",
            )
            return 0, 0, err

        # find id for election in Election table (or return fatal error)
        election_id = db.name_to_id(self.session, "Election", self.d["election"])
        if election_id is None:
            err = ui.add_new_error(
                err,
                "ini",
                self.param_file,
                f"No election named {self.d['election']} found in database",
            )
            return 0, 0, err

        # insert record into _datafile table
        datafile_id, err = datafile_info(
            self.session.bind,
            self.param_file,
            self.d["results_short_name"],
            filename,
            self.d["results_download_date"],
            self.d["results_source"],
            self.d["results_note"],
            jurisdiction_id,
            election_id,
            self.d["is_preliminary"],
        )
        return datafile_id, election_id, err

    def load_results(
        self, rollup: bool = False, rollup_rut: Optional[str] = None
    ) -> Optional[dict]:
        """
        Optional inputs:
            rollup: bool = False, if True, roll results up to subdivision before inserting in db
            rollup_rut: Optional[str] = None, subdivision type to roll up to (typically 'county')

        Load results from the file referenced in self.param_file

        Returns:
            Optional[dict], error dictionary
        """
        err = None
        print(f'\n\nProcessing {self.d["results_file"]}')

        # Enter datafile info to db and collect _datafile_Id and Election_Id
        datafile_id, election_id, new_err = self.track_results()
        if new_err:
            err = ui.consolidate_errors([err, new_err])
            return err

        else:
            constants = self.collect_constants_from_ini()

            # load results to db
            for mu in self.munger_list:
                print(f"\twith munger {mu}")
                f_path = os.path.join(self.results_dir, self.d["results_file"])
                mu_path = os.path.join(self.mungers_path, f"{mu}.munger")
                new_err = load_results_file(
                    self.session,
                    mu_path,
                    f_path,
                    self.juris_true_name,
                    datafile_id,
                    election_id,
                    constants,
                    self.results_dir,
                    self.path_to_jurisdiction_dir,
                    rollup=rollup,
                    rollup_rut=rollup_rut,
                )
                if new_err:
                    err = ui.consolidate_errors([err, new_err])
        return err

    def collect_constants_from_ini(self) -> dict:
        """
        Returns:
             dict, dictionary of constant elements from .ini file, omitting constants that have no content
        """
        constants = dict()
        for k in [
            "Party",
            "ReportingUnit",
            "CountItemType",
            "CandidateContest",
            "BallotMeasureContest",
            "BallotMeasureSelection",
            "Candidate",
        ]:
            # if element was given in .ini file
            if (self.d[k] is not None) and self.d[k] != "":
                constants[k] = self.d[k]
        return constants

    def list_values(self, element: str) -> (list, Optional[dict]):
        """
        Inputs:
            element: str, name of database table
        Returns:
            list, list of values for the given <element> found in the results file referenced in self.param_file
            Optional[dict], error dictionary
            lists all values for the element found in the file"""
        err = None
        values = list()
        constants = self.collect_constants_from_ini()
        if element in constants.keys():
            return [constants[element], None]
        else:
            try:
                for mu in self.munger_list:
                    print(f"\twith munger {mu}")
                    f_path = os.path.join(self.results_dir, self.d["results_file"])
                    munger_path = os.path.join(self.mungers_path, f"{mu}.munger")
                    p, err = m.get_and_check_munger_params(munger_path)
                    if ui.fatal_error(err):
                        return values, err

                    df, err = m.to_standard_count_frame(
                        f_path,
                        munger_path,
                        p,
                        suffix="_SOURCE",
                    )
                    if ui.fatal_error(err):
                        return values, err
                    df, new_err = m.munge_source_to_raw(
                        df,
                        munger_path,
                        "_SOURCE",
                        self.results_dir,
                        f_path,
                    )
                    err = ui.consolidate_errors([err, new_err])
                    if ui.fatal_error(new_err):
                        return values, err
                    values = list(set(values.extend(df[f"{element}_raw"].unique())))

            except Exception as exc:
                err = ui.add_new_error(
                    err,
                    "system",
                    f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
                    f"Unexpected exception while converting data to standard form or munging to raw: {exc}",
                )
                return values, err

        # TODO complete
        return values, err


# noinspection PyTypeChecker
class DataLoader:
    def __new__(cls, param_file: Optional[str] = None, dbname: Optional[str] = None):
        """Checks if parameter file exists and is correct. If not, does
        not create DataLoader object."""

        # default param_file is run_time.ini in current directory
        if not param_file:
            param_file = "run_time.ini"

        d, err = ui.get_parameters(
            required_keys=constants.multi_data_loader_pars,
            optional_keys=constants.optional_mdl_pars,
            param_file=param_file,
            header="electiondata",
        )
        if err:
            print(f"DataLoader object not created. Error:\n{err}")
            return None

        # create db if it does not already exist and have right tables
        err = db.create_db_if_not_ok(
            d["repository_content_root"], db_param_file=param_file, dbname=dbname
        )
        if err:
            print(f"DataLoader object not created. Error:\n{err}")
            return None
        if not os.path.isdir(d["results_dir"]):
            print(f"Warning: DataLoader results directory does not exist.")
        return super().__new__(cls)

    def __init__(
        self,
        param_file: Optional[str] = None,
        dbname: Optional[str] = None,
        major_subdivision_file: Optional[str] = None,
    ):
        """
        Inputs:
            param_file: Optional[str] = None, path to file of necessary parameters (defaults to `run_time.ini`)
            dbname: Optional[str] = None, name of database (defaults to name specified in param_file)
            major_subdivision_file: str = None, path to file with columns
                'jurisdiction', 'major_subjurisdiction_type'

        Returns DataLoader instance with attributes:
            d, dictionary of parameters from param_file
            db_engine, sqlalchemy engine connecting to postgres database specified in param_file
            session, sqlalchemy session for interacting with the database
            analyzer, Analyzer instance for exporting or analyzing data in the database,
                using subdivisions from <major_subdivision_file>, if given
        """
        # default param_file is run_time.ini in current directory
        if not param_file:
            param_file = "run_time.ini"

        # grab parameters
        self.d, self.parameter_err = ui.get_parameters(
            required_keys=constants.multi_data_loader_pars,
            optional_keys=constants.optional_mdl_pars,
            param_file=param_file,
            header="electiondata",
        )

        # define parameters derived from run_time.ini
        self.d["ini_dir"] = os.path.join(
            self.d["repository_content_root"], "ini_files_for_results"
        )
        self.d["mungers_dir"] = os.path.join(
            self.d["repository_content_root"], "mungers"
        )

        # connect to db
        self.db_engine = None  # will be set in connect_to_db
        self.session = None  # will be set in connect_to_db
        self.connect_to_db(dbname=dbname, db_param_file=param_file)

        # create analyzer with same db
        self.analyzer = Analyzer(
            dbname=dbname,
            param_file=param_file,
            major_subdivision_file=major_subdivision_file,
        )

    def connect_to_db(
        self,
        dbname: Optional[str] = None,
        db_param_file: Optional[str] = None,
        db_params: Optional[Dict[str, str]] = None,
    ):
        """
        Inputs:
            dbname: Optional[str] = None,
            db_param_file: Optional[str] = None,
            db_params: Optional[Dict[str, str]] = None,

            Sets engine attribute to an open connection with the database specified by
                dname and/or db_params and/or db_param_file, and sets session attribute
                to a newly-opened session on that engine
        """
        try:
            self.db_engine, err = db.sql_alchemy_connect(
                db_param_file=db_param_file, dbname=dbname, db_params=db_params
            )
            Session = sessionmaker(bind=self.db_engine)
            self.session = Session()
        except Exception as exc:
            if dbname:
                label = dbname
            elif db_params and dbname in db_params.keys():
                label = db_params["dbname"]
            elif db_param_file:
                label = db_param_file
            else:
                label = "connection"
            err = ui.add_new_error(
                None, "database", label, f"Connection failed due to exception: {exc}"
            )
        if err:
            print("Unexpected error connecting to database.")
            ui.report(err, self.d["reports_and_plots_dir"], file_prefix="dataloading_")
            print(f"Exiting. See {self.d['reports_and_plots_dir']} for details")
            quit()
        else:
            return

    def change_db(
        self,
        new_db_name: str,
        db_param_file: Optional[str] = None,
        db_params: Optional[Dict[str, str]] = None,
    ) -> Optional[dict]:
        """
        Input:
            new_db_name: str, name for new database
            db_param_file: Optional[str], file with parameters for connecting to db
            db_params: Optional[Dict[str,str]], set of parameters for connecting to db

        If both db_param_file and db_params are given, uses db_params
        Closes self.session, then redefines self.engine and self.session to connect to a different results database
            (which is created if necessary)
        Changes self.analyzer.session to connect to the new database

        Returns:
            Optional[dict], error dictionary
        """
        err = None
        if not db_params:
            # get db_params from current session, but with new database name
            db_params = {
                "host": self.db_engine.url.host,
                "port": self.db_engine.url.port,
                "user": self.db_engine.url.username,
                "password": self.db_engine.url.password,
                "dbname": new_db_name,
            }

        # disconnect from current db and connect to new db, updating self.session and self.dbname
        self.session.close()
        # # nb: next command updates self.session
        new_err = self.connect_to_db(
            db_params=db_params,
        )
        err = ui.consolidate_errors([err, new_err])
        if ui.fatal_error(new_err):
            return err
        self.d["dbname"] = new_db_name

        # create new electiondata database if necessary
        new_err = db.create_db_if_not_ok(
            self.d["repository_content_root"],
            dbname=new_db_name,
            db_param_file=db_param_file,
            db_params=db_params,
        )
        err = ui.consolidate_errors([err, new_err])
        if ui.fatal_error(new_err):
            return err

        # create new session for analyzer
        self.analyzer.session.close()
        self.analyzer.session = self.session
        return err

    def close_and_erase(self) -> Optional[dict]:
        """
        Closes and removes the database specified by self.engine. Creates new engine and session
        attributes pointing to the postgres database instead
        Returns:
            Optional[dict], error dictionary
        """
        err = None
        db_params = {
            "host": self.db_engine.url.host,
            "port": self.db_engine.url.port,
            "user": self.db_engine.url.username,
            "password": self.db_engine.url.password,
            "dbname": self.db_engine.url.database,
        }
        # point dataloader to default database
        new_err = self.change_db("postgres", db_params=db_params)
        err = ui.consolidate_errors([err, new_err])
        if ui.fatal_error(new_err):
            return err
        # remove the db
        new_err = db.remove_database(db_params)
        err = ui.consolidate_errors([err, new_err])
        return err

    def get_testing_data_from_git_repo(self, git_repo_url: str):
        """
        Inputs:
            git_repo_url: str, url of git repository with test results

        Puts shallow copy of git repository contents in to the directory whose path is
            self.d["results_dir"] (creating that directory if necessary)

        """

        # if the dataloader's results directory doesn't exist, create it
        results_dir = self.d["results_dir"]
        if not os.path.isdir(results_dir):
            # create the results_dir
            Path(results_dir).mkdir(parents=True, exist_ok=True)
            # create a shallow copy of the git directory in the results_directory
            cmd = f"git clone --depth 1 -b main {git_repo_url} {Path(results_dir).absolute()}"
            os.system(cmd)
            # remove the git information
            shutil.rmtree(os.path.join(results_dir, ".git"), ignore_errors=True)
            os.remove(os.path.join(results_dir, ".gitignore"))
            print(
                f"Files downloaded from {git_repo_url} into {Path(results_dir).absolute()}"
            )

        else:
            print(
                f"Tests will load data from existing directory: {Path(results_dir).absolute()}"
            )
        return

    def load_one_from_ini(
        self,
        ini_path: str,
        path_to_jurisdiction_dir: str,
        juris_true_name: str,
        rollup: bool = False,
    ) -> (Optional[SingleDataLoader], Optional[dict]):
        """
        Inputs:
            ini_path: str, path to file with parameters describing single results file
            path_to_jurisdiction_dir: str, path to directory holding the jurisdiction's files
            juris_true_name: str, name of jurisdiction (with spaces, not strings)
            rollup: bool = False, if True, finds and rolls up to major subdivision level;
                otherwise, loads results at level given in results file

        Loads results from the single results file specified by the parameters in <ini_path>.
        Returns:
             Optional[SingleDataLoader], SingleDataLoader object for the results file
                (or None if fatal error occurred)
             Optional[dict], error dictionary
        """
        sdl, err = check_and_init_singledataloader(
            self.d["results_dir"],
            ini_path,
            self.session,
            self.d["mungers_dir"],
            juris_true_name,
            path_to_jurisdiction_dir,
        )
        if ui.fatal_error(err):
            return None, err
        elif sdl is None:
            err = ui.add_new_error(
                err,
                "ini",
                Path(ini_path).stem,
                f"Unexpected failure to load data (no SingleDataLoader object created)",
            )
            return None, err
        # TODO check for file problems discernible before loading?

        # get rollup unit if required
        if rollup:
            if sdl.d["jurisdiction"] in self.analyzer.major_subdivision_type.keys():
                rollup_rut = self.analyzer.major_subdivision_type[sdl.d["jurisdiction"]]
            else:
                rollup_rut = None
                err = ui.add_new_error(
                    err,
                    "warn-file",
                    "major_subjurisdiction_types.txt",
                    f"Rollup cannot be done because no major subdivision found for {sdl.d['jurisdiction']}",
                )

        else:
            rollup_rut = None
        load_error = sdl.load_results(rollup=rollup, rollup_rut=rollup_rut)
        err = ui.consolidate_errors([err, load_error])
        return sdl, err

    def load_ej_pair(
        self,
        election: str,
        juris_true_name: str,
        rollup: bool = False,
        report_missing_files: bool = False,
        status: Optional[str] = None,
        run_tests: bool = True,
    ) -> (List[str], List[str], str, Optional[dict]):
        """
        Inputs:
            election: str,
            juris_true_name: str,
            rollup: bool = False, if true, roll up results to major subdivisions before loading to db
            report_missing_files: bool = False, if true, report any files referenced by .ini files but not found in
                results directory as a warning in the error dictionary
            status: Optional[str] = None, if given, use only reference results of that particular status for testing
            run_tests: bool = True, controls whether tests are run

        Looks within ini_files_for_results/<jurisdiction> for
        all ini files matching  given election and jurisdiction.
        For each, attempts to load file if it exists; reports missing data files.
        If files load successfully without fatal error and <run_tests> is True, runs tests on the loaded results
            and reports any failures to error report
        Returns
            List[str], list of successfully-loaded files,
            List[str], list of files that failed to load
            str, latest download date for successfully-loaded files
            Optional[dict], error report with outright error for results files whose loading failed.
        """
        err = None
        success_by_ini = list()
        failure_by_ini = list()
        latest_download_date = "0000-00-00"
        juris_system_name = jm.system_name_from_true_name(juris_true_name)
        path_to_jurisdiction_dir = os.path.join(
            self.d["repository_content_root"], "jurisdictions", juris_system_name
        )
        ini_subdir = os.path.join(self.d["ini_dir"], juris_system_name)
        if not os.path.isdir(ini_subdir):
            err = ui.add_new_error(
                err,
                "jurisdiction",
                juris_true_name,
                f"No matching subfolder in ini_files_for_results",
            )
            return list(), err
        for ini in os.listdir(ini_subdir):
            if ini[-4:] == ".ini":
                ini_path = os.path.join(ini_subdir, ini)
                params, new_err = ui.get_parameters(
                    required_keys=["election", "jurisdiction", "results_file"],
                    param_file=ini_path,
                    header="election_results",
                )
                if new_err:
                    err = ui.consolidate_errors([err, new_err])
                if ui.fatal_error(new_err):
                    continue
                if not ui.fatal_error(new_err) and params["election"] == election:
                    if params["jurisdiction"] == juris_true_name:
                        if not os.path.isfile(
                            os.path.join(self.d["results_dir"], params["results_file"])
                        ):
                            if report_missing_files:
                                err = ui.add_new_error(
                                    err,
                                    "warn-file",
                                    params["results_file"],
                                    f"File not found in directory {self.d['results_dir']}",
                                )
                            continue

                        sdl, load_error = self.load_one_from_ini(
                            ini_path,
                            path_to_jurisdiction_dir,
                            juris_true_name,
                            rollup=rollup,
                        )
                        if ui.fatal_error(load_error):
                            failure_by_ini.append(ini)
                        else:
                            success_by_ini.append(ini)
                            if latest_download_date < sdl.d["results_download_date"]:
                                latest_download_date = sdl.d["results_download_date"]
                        if load_error:
                            err = ui.consolidate_errors([err, load_error])

                    else:
                        err = ui.add_new_error(
                            err,
                            "warn-ini",
                            ini,
                            f"Ini in subdirectory {ini_subdir} has non-matching jurisdiction: {params['jurisdiction']}",
                        )
        # add totals if necessary
        add_err = self.add_totals_if_missing(election, juris_true_name)
        if add_err:
            err = ui.consolidate_errors([err, add_err])

        if run_tests:
            # if load successful to this point, test the db's data for this ej-pair and add any failures to err
            if not ui.fatal_error(err):
                new_err = self.analyzer.test_loaded_results(
                    election, juris_true_name, juris_system_name, status=status
                )
                err = ui.consolidate_errors([err, new_err])
        return success_by_ini, failure_by_ini, latest_download_date, err

    def load_all(
        self,
        report_dir: Optional[str] = None,
        load_jurisdictions: bool = True,
        move_files: bool = True,
        election_jurisdiction_list: Optional[List[Tuple[str, str]]] = None,
        election_list: Optional[List[str]] = None,
        rollup: bool = False,
        report_missing_files: bool = False,
        run_tests: bool = True,
        suppress_warnings: bool = False,
    ) -> (Dict[str, List[str]], Dict[str, List[str]], Dict[str, bool], Optional[dict]):
        """
        Inputs:
            report_dir: Optional[str] = None, directory for reports
            load_jurisdictions: bool = True, if true, load jurisdiction info to db before loading results
            move_files: bool = True, if true, move files to self.d["archive_dir"] after successful loading
            election_jurisdiction_list: Optional[List[Tuple[str, str]]] = None, if given, processes only files for
                ej_pairs in the list
            election_list: Optional[List[str]] = None,
            rollup: bool = False, if True, loads results rolled up to major subdivision
            report_missing_files: bool = False, if True, reports files referenced in .ini files
                but not found in results directory
            run_tests: bool = True, if false, do not run tests on loaded data
            suppress_warnings: bool = False, if True, report only errors
                to directory specified by self.d["reports_and_plots_dir"]

        Processes all results (or all results corresponding to pairs in
        ej_list if given) in DataLoader's results directory using
            .ini files from repository.
        If load is successful for all files for a single election-jurisdicction pair,
            then add records for total vote counts whereever necessary.
        By default, loads (or updates) the info from the jurisdiction files
            into the db first. By default, moves files to the DataLoader's archive directory.
        Returns a post-reporting error dictionary (including errors from post-load testing, if
            <run_tests> is True, and a dictionary of
            successfully-loaded files (by election-jurisdiction pair).
        Note: errors initializing loading process (e.g., results file not found) do *not* generate
            <success> = False, though those errors are reported in <err>
        If <archive> is true, archive the files

        Returns:
            Dict[str, List[str]], for each e-j pair attempted,
                a list of files loading successfully
            Dict[str, List[str]], for each e-j pair attempted,
                a list of files that were attempted but failed to load
            Dict[str, bool], for each e-j pair attempted,
                a boolean (True if no files failed and all tests passed; otherwise False)
            Optional[dict], error dictionary
        """
        # initialize
        err = None
        successfully_loaded = dict()
        failed_to_load = dict()
        all_tests_passed = dict()
        ts = datetime.datetime.now().strftime("%m%d_%H%M")
        if not report_dir:
            report_dir = os.path.join(self.d["reports_and_plots_dir"], f"load_all_{ts}")

        # if no election_jurisdiction_list given and no election_list is given,
        #  default to all represented in ini files in repository; if election_list is given,
        #  use only elections in the list
        if not election_jurisdiction_list:
            election_jurisdiction_list = ui.election_juris_list(
                self.d["ini_dir"], results_path=self.d["results_dir"]
            )
            if election_list:
                election_jurisdiction_list = [
                    (e, j)
                    for (e, j) in election_jurisdiction_list
                    if e in election_list
                ]
        election_jurisdiction_list.sort()

        jurisdictions = list(set(j for (e, j) in election_jurisdiction_list))
        jurisdictions.sort()
        elections = {
            j: list(set(e for (e, k) in election_jurisdiction_list if k == j))
            for j in jurisdictions
        }
        if load_jurisdictions:
            ok_jurisdictions = list()
            for juris in jurisdictions:
                # check jurisdiction
                juris_system_name = jm.system_name_from_true_name(juris)
                juris_path = os.path.join(
                    self.d["repository_content_root"],
                    "jurisdictions",
                    juris_system_name,
                )
                new_err = jm.ensure_jurisdiction_dir(
                    self.d["repository_content_root"], juris_path
                )
                if new_err:
                    err = ui.consolidate_errors([err, new_err])
                    if ui.fatal_error(new_err):
                        print(f"Jurisdiction {juris} did not load. See .error file.")
                        # remove j from jurisdictions whose files will be loaded
                        election_jurisdiction_list = [
                            (e, j)
                            for (e, j) in election_jurisdiction_list
                            if j != juris
                        ]
                        # add juris to failure list
                        failed_to_load[
                            juris
                        ] = "Jurisdiction files did not load. See .errors"
                        err = ui.report(
                            err,
                            report_dir,
                            key_list=constants.juris_load_report_keys,
                            suppress_warnings=suppress_warnings,
                        )
                        continue
                print(f"Loading/updating jurisdiction {juris} to {self.session.bind}")
                try:
                    new_err = jm.load_or_update_juris_to_db(
                        self.session,
                        self.d["repository_content_root"],
                        juris,
                        juris_system_name,
                    )
                    if new_err:
                        err = ui.consolidate_errors([err, new_err])
                    if not ui.fatal_error(new_err):
                        ok_jurisdictions.append(juris)
                except Exception as exc:
                    err = ui.add_new_error(
                        err, "jurisdiction", juris, f"Exception during loading: {exc}"
                    )
        else:
            print(
                "No jurisdictions loaded because load_jurisdictions==False. All jurisdictions assumed to be OK"
            )
            ok_jurisdictions = jurisdictions

        latest_download_date = dict()
        for jurisdiction in ok_jurisdictions:
            juris_system_name = jm.system_name_from_true_name(jurisdiction)
            juris_err = None
            for election in elections[jurisdiction]:
                # load the relevant files
                (
                    success_list,
                    failure_list,
                    latest_download_date[jurisdiction],
                    new_err,
                ) = self.load_ej_pair(
                    election,
                    jurisdiction,
                    rollup=rollup,
                    report_missing_files=report_missing_files,
                    run_tests=run_tests,
                )
                if new_err:
                    juris_err = ui.consolidate_errors([juris_err, new_err])

                # set all_test_passed boolean for this e-j pair
                if not run_tests:
                    all_tests_passed[f"{election};{jurisdiction}"] = True
                elif failure_list or (
                    new_err and ("warn-test" in new_err.keys()) and new_err["warn-test"]
                ):
                    all_tests_passed[f"{election};{jurisdiction}"] = False
                else:
                    all_tests_passed[f"{election};{jurisdiction}"] = True

                successfully_loaded[f"{election};{jurisdiction}"] = success_list
                failed_to_load[f"{election};{jurisdiction}"] = failure_list

            if move_files:
                # if all existing files referenced in any results.ini
                # for the jurisdiction
                # -- for any election -- loaded correctly
                if not ui.fatal_error(juris_err):
                    juris_results_path = os.path.join(
                        self.d["results_dir"], juris_system_name
                    )
                    # copy the jurisdiction's results file to archive directory
                    # (subdir named with latest download date; if exists already, create backup with timestamp)
                    if os.path.isdir(juris_results_path):
                        new_err = ui.copy_directory_with_backup(
                            juris_results_path,
                            os.path.join(
                                self.d["archive_dir"],
                                f"{juris_system_name}_{latest_download_date[jurisdiction]}",
                            ),
                            report_error=False,
                        )
                        err = ui.consolidate_errors([err, new_err])
                        # remove jurisdiction's results file from results directory
                        shutil.rmtree(juris_results_path)
                    else:
                        print(
                            f"Directory not copied, because not found: {juris_results_path}\n"
                            f"This may be caused by having results for two different elections"
                            f"in the directory."
                        )

            err = ui.consolidate_errors([err, juris_err])

            err = ui.report(
                err,
                report_dir,
                key_list=constants.juris_load_report_keys,
                file_prefix=juris_system_name,
                suppress_warnings=suppress_warnings,
            )

        # report remaining errors
        ui.report(
            err, report_dir, file_prefix="system_", suppress_warnings=suppress_warnings
        )

        # keep all election-juris pairs in success report, but remove empty failure reports
        failed_to_load = {k: v for k, v in failed_to_load.items() if v}

        return successfully_loaded, failed_to_load, all_tests_passed, err

    def strip_dates_from_results_folders(self) -> Dict[str, str]:
        """
        Remove any date-suffixes from sub-folders of self.d["results_dir"]

        Returns:
            Dict[str, str], dictionary mapping old names to new
        """
        rename: Dict[str, str] = dict()
        p = re.compile(r"^(.*)_\d\d\d\d-\d\d-\d\d$")
        for folder in os.listdir(self.d["results_dir"]):
            if p.findall(folder):
                juris = p.findall(folder)[0]
                os.rename(
                    os.path.join(self.d["results_dir"], folder),
                    os.path.join(self.d["results_dir"], juris),
                )
                rename[folder] = juris
        return rename

    def remove_data(
        self,
        election_id: int,
        juris_id: int,
    ) -> Optional[str]:
        """
        Inputs:
            election_id: int, an Id value to be looked up in Election table
            juris_id: int, an Id value to be looked up in ReportingUnit table

        Remove from the db all data in datafiles associated (in _datafile table) to
            the Election designated by <election_id> and the jurisdiction
            designated in the ReportingUnit table by <juris_id>

        Returns:
            Optional[str], error string (or None if no errors)
        """
        # get connection & cursor
        connection = self.session.bind.raw_connection()
        cursor = connection.cursor()

        # find all datafile ids matching the given election and jurisdiction
        df_list, err_str = db.data_file_list_cursor(
            cursor, election_id, reporting_unit_id=juris_id
        )
        if err_str:
            return err_str

        # remove data from all those datafiles
        err_str_list = list()
        for idx in df_list:
            err_str = db.remove_vote_counts(self.session, idx)
            if err_str:
                err_str_list.append(err_str)
            err_str = db.remove_record_from_datafile_table(self.session, idx)
            if err_str:
                err_str_list.append(err_str)
        if err_str_list:
            return ";".join(err_str_list)
        else:
            return None

    def add_totals_if_missing(self, election: str, jurisdiction: str) -> Optional[dict]:
        """
        Inputs:
            election: str, name of election
            jurisdiction: str, name of jurisdiction

        For each contest, adds records for 'total' vote type to VoteCount table in database wherever it's missing,
            calculated from other vote types

        Returns:
            Optional[dict], error dictionary
        """
        err = None
        # pull results from db
        election_id = db.name_to_id(self.session, "Election", election)
        jurisdiction_id = db.name_to_id(self.session, "ReportingUnit", jurisdiction)
        fields = [
            "Contest_Id",
            "Selection_Id",
            "ReportingUnit_Id",
            "Election_Id",
            "_datafile_Id",
            "CountItemType",
            "Count",
        ]
        aliases = [
            "Contest_Id",
            "Selection_Id",
            "ReportingUnit_Id",
            "Election_Id",
            "_datafile_Id",
            "CountItemType",
            "Count",
        ]
        df = db.read_vote_count(
            self.session,
            election_id=election_id,
            jurisdiction_id=jurisdiction_id,
            fields=fields,
            aliases=aliases,
        )
        if df.empty:
            err = ui.add_new_error(
                err,
                "jurisdiction",
                jurisdiction,
                f"Could not add totals for {election} because no data was found",
            )
        else:
            # find total records that are missing
            m_df = m.missing_total_counts(df, self.session)
            # load new total records to db
            try:
                new_err = db.insert_to_cdf_db(
                    self.session.bind,
                    m_df,
                    "VoteCount",
                    "system",
                    f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
                )
                err = ui.consolidate_errors([err, new_err])
            except Exception as exc:
                err = ui.add_new_error(
                    err,
                    "system",
                    f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
                    f"Unexpected exception while adding totals to existing db for {election} - {jurisdiction}",
                )
        return err

    def load_data_from_db_dump(
        self, dbname: str, dump_file: str, delete_existing: bool = False
    ) -> Optional[str]:
        """
        Inputs:
            dbname: str, name for database to be created and loaded with data
            dump_file: str, path to file with data to be loaded to the new database

        If <dbname> does not already exists, creates a database from a file dumped from another
            database (but only if db does not already exist).

        Returns:
             Optional[str], error (or None if no error)
        """
        connection = self.session.bind.raw_connection()
        cursor = connection.cursor()
        err_str = db.create_database(
            connection, cursor, dbname=dbname, delete_existing=delete_existing
        )

        cursor.close()
        connection.close()

        if not err_str:
            # read contents of dump into db
            err_str = db.restore_to_db(dbname, dump_file, self.db_engine.url)
        return err_str

    def load_single_external_data_set(
        self,
        df: pd.DataFrame,
        source: str,
        year: str,
        note: str,
        # TODO add optional parameter replace_existing: bool = False,
    ) -> Optional[dict]:
        """
        Inputs:
            df: pd.DataFrame, results dataframe with columns
                Category, Label, OrderWithinCategory, ReportingUnit, Value,
                where:
                    ReportingUnit values match values in database specified by self.session,
                    Category choice will be used to present user with a set of labels
                    ReportingUnit-Category-Label choice will be used to identify a count (Value)
                    OrderWithinCategory is an integer (should be a function of Category-Pair) to determine
                        in what order Labels will be presented to user
            source: str, source of data
            year: str, year of data
            note: str, note about data

        Loads data description records into ExternalDataSet table, and loads data records into ExternalData table,
            linked to correct ExternalDataset record

        Returns:
            Optional[dict], error dictionary
        """
        err = None
        # TODO check columns of df
        # TODO error handling
        working = df.copy()
        working["Source"] = source
        working["Year"] = year
        working["Note"] = note

        # put info into ExternalDataSet table and retrieve Id
        eds = (
            working[
                ["Category", "Label", "OrderWithinCategory", "Source", "Year", "Note"]
            ]
            .drop_duplicates()
            .copy()
        )
        load_err = db.insert_to_cdf_db(
            self.session.bind,
            eds,
            "ExternalDataSet",
            "database",
            f"Data did not load to ExternalDataSet",
        )
        if load_err:
            err = ui.consolidate_errors([err, load_err])
            return err

        # put info into ExternalData
        eds_col_map = {c: c for c in eds.columns}
        working = db.append_id_to_dframe(
            self.session.bind, working, "ExternalDataSet", col_map=eds_col_map
        )
        ru = pd.read_sql_table("ReportingUnit", self.session.bind).rename(
            columns={"Id": "ReportingUnit_Id"}
        )
        working = working.merge(
            ru[["ReportingUnit_Id", "Name"]],
            how="left",
            left_on="ReportingUnit",
            right_on="Name",
        )
        ed = working[working.ReportingUnit_Id.notnull()][
            ["Value", "ReportingUnit_Id", "ExternalDataSet_Id"]
        ]
        load_err = db.insert_to_cdf_db(
            self.session.bind,
            ed,
            "ExternalData",
            "database",
            f"Data from did not load to ExternalData",
        )
        if load_err:
            err = ui.consolidate_errors([err, load_err])
            return err
        # TODO

        return err

    # TODO
    def load_acs5_data(self, census_year: int, election: str) -> Optional[dict]:
        """
        Incomplete!
        Inputs:
            census_year: int, year for which American Community Survey data is available from census.gov
            election: str, name matching an election in the Election table of the database specified in self.session

        Downloads census.gov American Community Survey data by county
        for <census_year>;
        uploads data to database specified in self.session; associates all datasets for <census_year> to
        election-jurisdiction
        pairs for any jurisdiction that has nontrivial join.

        Returns:
            Optional[dict], error dictionary
        """
        err = None
        df = pd.DataFrame()
        for category in electiondata.constants.acs5_columns.keys():
            columns_to_get = electiondata.constants.acs5_columns[category].keys()
            working = exd.get_raw_acs5_data(columns_to_get, census_year)
            if working.empty:
                err = ui.add_new_error(
                    err, "census", census_year, "No data found at census.gov"
                )
                continue
            working = exd.combine_and_rename_columns(
                working, electiondata.constants.acs_5_label_summands
            )
            working = exd.normalize(
                working, electiondata.constants.acs_5_label_summands.keys()
            )
            working["Category"] = category
            df = pd.concat(df, working)
        self.load_single_external_data_set()
        # TODO
        return err

    def temp_reload_existing_acs5_data(self, census_file: str) -> Optional[dict]:
        """Transitional function for taking data from Version 1.0 system to later version (June 2021)
        load data from census file exported from old db (e.g. census_no_ids.csv)

        Inputs:
            census_file: str, path to a file containing census data exported from Version 1.0 database

        Loads data from census_file for years 2016 and 2018 to database specified in self.session

        Returns:
            Optional[dict], error dictionary
        """
        cdf = pd.read_csv(census_file, index_col=None)

        els = pd.read_sql_table("Election", self.session.bind)
        els = els[els.Name != "none or unknown"]
        els["ElectionYear"] = els.Name.str[0:4]
        els["ElectionYear"] = pd.to_numeric(els["ElectionYear"])

        data_df = els.merge(cdf, how="left", on="ElectionYear").drop("Id", axis=1)

        for y in [2016, 2018]:
            working = data_df[data_df.Year == y]

            self.load_single_external_data_set(
                working,
                "American Community Survey 5",
                f"{y}",
                "",
            )

        col_map = {c: c for c in ["Category", "Label", "Year"]}
        df_appended = db.append_id_to_dframe(
            self.session.bind, data_df, "ExternalDataSet", col_map=col_map
        )
        join_df = df_appended.merge(els, on="Name").rename(
            columns={"Id": "Election_Id"}
        )[["Election_Id", "ExternalDataSet_Id"]]
        err = db.insert_to_cdf_db(
            self.session.bind,
            join_df,
            "ElectionExternalDataSetJoin",
            "database",
            "join data not loaded",
        )
        return err

    def update_juris_from_multifile(
        self, df: pd.DataFrame, juris_true: str, juris_system: str
    ) -> Optional[dict]:
        """
        This has not been generalized past the MIT multi-election file format.

        Inputs:
            df: pd.DataFrame, data read from MIT multi-election/jurisdiction results file, assumed to have columns
                Candidate_raw,
            juris_true: str, name of jurisdiction (with spaces, not hyphens)
            juris_system: str, name of jurisdiction (with hyphens, not spaces)

        Adds any new elections from electiondata.constants.mit_elections to 000_for_all_jurisdictions/Election.txt file
        Adds any candidates in Candidate_raw column to the jurisdiction's Candidate.txt and dictionary.txt files
        Adds lines to jurisdiction's dictionary.txt to map 'US PRESIDENT' and 'PRESIDENT' to 'US President (XX)',
            where XX is the abbreviation for the jurisdiction
        Adds parties in electiondata.constants.mit_party to dictionary.txt
        Adds vote counts in electiondata.constants.mit_cit to dictionary.txt
        Loads jurisdiction info into the database specified by self.session

        Returns:
            Optional[dict], error dictionary
        """
        update_err = None

        # add elections to 000_for_all_jurisdictions/Election.txt
        election_df = pd.DataFrame(
            [[f"{e} General", "general"] for e in electiondata.constants.mit_elections],
            columns=["Name", "ElectionType"],
        )
        path_to_election_dir = os.path.join(
            self.d["repository_content_root"],
            "/jurisdictions/000_for_all_jurisdictions",
        )
        old = jm.get_element(path_to_election_dir, "Election")
        new = pd.concat([old, election_df]).drop_duplicates()
        jm.write_element(path_to_election_dir, "Election", new)

        # add candidates to Candidate.txt
        candidate_col = "Candidate_raw"
        candidates = sorted(df[candidate_col].unique())

        # add candidates to dictionary
        candidate_map = {
            x: multi.correct(x.title()) for x in electiondata.constants.mit_candidates
        }
        update_err = ui.consolidate_errors(
            [
                update_err,
                multi.add_candidates(
                    juris_system,
                    self.d["repository_content_root"],
                    candidates,
                    candidate_map,
                ),
            ]
        )

        # add contests to dictionary  # TODO this belongs with constants, but depends on jurisdiction...
        contest_d = {
            "PRESIDENT": f"US President ({electiondata.constants.abbr[juris_true]})",
            "US PRESIDENT": f"US President ({electiondata.constants.abbr[juris_true]})",
        }
        update_err = ui.consolidate_errors(
            [
                update_err,
                multi.add_dictionary_entries(
                    juris_system,
                    self.d["repository_content_root"],
                    "CandidateContest",
                    contest_d,
                ),
            ]
        )
        # add parties to dictionary
        update_err = ui.consolidate_errors(
            [
                update_err,
                multi.add_dictionary_entries(
                    juris_system,
                    self.d["repository_content_root"],
                    "Party",
                    electiondata.constants.mit_party,
                ),
            ]
        )

        # add vote_count_types to dictionary
        update_err = ui.consolidate_errors(
            [
                update_err,
                multi.add_dictionary_entries(
                    juris_system,
                    self.d["repository_content_root"],
                    "CountItemType",
                    electiondata.constants.mit_cit,
                ),
            ]
        )
        ## load jurisdiction info to db
        juris.load_or_update_juris_to_db(
            self.session, self.d["repository_content_root"], juris_true, juris_system
        )

        return update_err

    def load_multielection_from_ini(
        self,
        ini: str,
        dictionary_path: Optional[str] = None,
        overwrite_existing: bool = False,
        load_jurisdictions: bool = True,
        report_err_to_file: bool = True,
        suppress_warnings: bool = False,
    ) -> (Dict[str, List[str]], Optional[dict]):
        """
        Required inputs:
            ini: str, path to file with initialization parameters for dataloader and secondary source
        Optional inputs:
            dictionary_path: Optional[str] = None, if given, path to dictionary. If not given, path assumed
                to be <repository_content_root>/secondary_sources/<ini_params["secondary_source"]
            overwrite_existing: bool = False, if true, existing data will be deleted for each election-jurisdiction pair
                represented in the file indicated in <ini>
            load_jurisdictions: bool = True, if true, jurisdiction information will be loaded to database before processing
                results data
            report_err_to_file: bool = True, if true, errors reported to file; otherwise errors returned
            suppress_warnings: bool = False, if true, only errors (not warnings) reported to file

        Loads results from the file indicated in <ini> to the database specified by self.session

        Returns:
            Dict[str, List[str]], a dictionary of successful mungers (for each election-jurisdiction pair)
            Optional[dict], error dictionary
        """
        err = None
        success = dict()
        ts = datetime.datetime.now().strftime("%m%d_%H%M")
        ini_params, new_err = ui.get_parameters(
            required_keys=constants.req_for_combined_file_loading,
            param_file=ini,
            header="election_results",
        )
        err = ui.consolidate_errors([err, new_err])
        if ui.fatal_error(new_err):
            if report_err_to_file:
                err = ui.report(
                    err,
                    os.path.join(self.d["reports_and_plots_dir"], f"multi_{ts}"),
                    file_prefix="multi_dictionary",
                    suppress_warnings=suppress_warnings,
                )
            return success, err

        results_path = os.path.join(self.d["results_dir"], ini_params["results_file"])
        multi_file_name = Path(ini_params["results_file"]).name
        ini_file = Path(ini).name
        if dictionary_path is None:
            dictionary_path = os.path.join(
                self.d["repository_content_root"],
                "secondary_sources",
                ini_params["secondary_source"],
                "dictionary.txt",
            )
        # check that secondary_source directory has necessary files without flaws
        new_err = juris.check_dictionary(dictionary_path)
        err = ui.consolidate_errors([err, new_err])
        if ui.fatal_error(new_err):
            if report_err_to_file:
                err = ui.report(
                    err,
                    os.path.join(self.d["reports_and_plots_dir"], f"multi_{ts}"),
                    file_prefix="multi_dictionary",
                    suppress_warnings=suppress_warnings,
                )
            return success, err

        # load items from the secondary source tables to the db
        # NB: Office, CandidateContest and sub-jurisdiction ReportingUnits are specific to the various jurisdictions
        for (element, table) in [
            ("Election", "Election"),
            ("Jurisdiction", "ReportingUnit"),
        ]:
            new_err = multi.load_to_db(
                self.session,
                table,
                os.path.join(
                    self.d["repository_content_root"],
                    "secondary_sources",
                    f"{element}.txt",
                ),
            )
            err = ui.consolidate_errors([err, new_err])
            if ui.fatal_error(new_err):
                if report_err_to_file:
                    err = ui.report(
                        err,
                        os.path.join(self.d["reports_and_plots_dir"], f"multi_{ts}"),
                        file_prefix="multi_dictionary",
                        suppress_warnings=suppress_warnings,
                    )
                return success, err

        # get list of mungers to apply
        mungers = [x.strip() for x in ini_params["munger_list"].split(",")]
        for munger in mungers:
            # read file format parameters from munger
            munger_path = os.path.join(
                self.d["repository_content_root"], "mungers", f"{munger}.munger"
            )
            p, new_err = m.get_and_check_munger_params(munger_path)
            err = ui.consolidate_errors([err, new_err])
            if ui.fatal_error(new_err):
                continue  # go to next munger

            # read file to dict of dataframes, one for each ej-pair
            working, new_err = m.file_to_raw_df(
                munger_path,
                p,
                results_path,
                self.d["results_dir"],
                extra_formula_keys=["Election", "Jurisdiction"],
            )
            err = ui.consolidate_errors([err, new_err])
            if ui.fatal_error(new_err):
                continue  # go to next munger
            # collect election-jurisdiction pairs
            new_err = dict()
            dictionary_df = pd.read_csv(
                dictionary_path,
                sep="\t",
            )
            for element in ["Jurisdiction", "Election"]:
                working, new_err[element] = m.replace_raw_with_internal_name(
                    working,
                    munger,
                    multi_file_name,
                    element,
                    dictionary_df,
                    dictionary_path,
                    drop_unmatched=True,
                )
            # use multi-file's dictionary to get internal names of election and jurisdiction
            err = ui.consolidate_errors(
                [err, new_err["Election"], new_err["Jurisdiction"]]
            )
            if ui.fatal_error(new_err["Election"]) or ui.fatal_error(
                new_err["Jurisdiction"]
            ):
                continue  # go to next munger
            working.set_index(["Election", "Jurisdiction"], inplace=True)
            ej_pairs = sorted(list(set(working.index)))
            # split into dictionary of dataframes, one for each ej_pair
            df_dict = {pair: working[working.index == pair] for pair in ej_pairs}

            # get juris system names:
            system_name = dict()
            jurisdictions = sorted(list({j for (e, j) in ej_pairs}))

            for jurisdiction in jurisdictions:
                system_name[jurisdiction] = juris.system_name_from_true_name(
                    jurisdiction
                )

            # track which jurisdictions have been loaded
            loaded = {j: False for j in jurisdictions}

            # get db indices for elections and jurisdictions
            e_id = j_id = dict()
            for (election, jurisdiction) in ej_pairs:
                e_id[election] = db.name_to_id(self.session, "Election", election)
                j_id[jurisdiction] = db.name_to_id(
                    self.session, "ReportingUnit", jurisdiction
                )

            for (election, jurisdiction) in ej_pairs:
                success[(election, jurisdiction)] = list()
                # get list of datafiles in db with the election and jurisdiction
                df_list, err_str = db.data_file_list(
                    self.session, e_id[election], reporting_unit_id=j_id[jurisdiction]
                )
                if err_str:
                    err = ui.add_new_error(
                        err,
                        "database",
                        f"{self.session.bind.url.database}",
                        err_str,
                    )
                    f"Not loaded: {err_str}"
                    continue

                # remove existing data if overwriting
                if df_list and overwrite_existing:
                    # remove old data
                    err_str = self.remove_data(e_id[election], j_id[jurisdiction])
                    if err_str:
                        err = ui.add_new_error(
                            err,
                            "database",
                            f"{self.session.bind.url.database}",
                            f"Error removing existing data for {election} {jurisdiction}:\n{err}",
                        )
                        print(f"\t\tError removing data: {err_str}")
                    # warn that data was removed:
                    err = ui.add_new_error(
                        err,
                        "warn-database",
                        f"{self.session.bind.url.database}",
                        f"data removed for {election} {jurisdiction}",
                    )
                # skip, with warning if not overwriting
                elif df_list and not overwrite_existing:
                    err = ui.add_new_error(
                        err,
                        "warn-database",
                        f"{self.session.bind.url.database}",
                        f"Data in db for {election} {jurisdiction} exists; will not be replaced with new data.",
                    )
                    print(f"\t\tData exists in db; will not be replaced with new data.")
                    continue

                # # If we reach this point, we'll be uploading the data for this e-j pair
                # create record in _datafile table
                datafile_id, new_err = datafile_info(
                    self.session.bind,
                    ini_file,
                    f"{ini_params['results_short_name']}_{system_name[jurisdiction]}_{election}",
                    multi_file_name,
                    ini_params["results_download_date"],
                    ini_params["results_source"],
                    ini_params["results_note"],
                    j_id[jurisdiction],
                    e_id[election],
                    False,
                )
                if new_err:
                    err = ui.consolidate_errors([err, new_err])
                    if ui.fatal_error(new_err):
                        print(f"\t\tNot loaded due to error creating datafile record")
                        continue

                if load_jurisdictions:
                    if not loaded[jurisdiction]:
                        # update juris in db
                        juris_err = juris.ensure_jurisdiction_dir(
                            self.d["repository_content_root"],
                            system_name[jurisdiction],
                        )
                        err = ui.consolidate_errors([err, juris_err])
                        if not ui.fatal_error(juris_err):
                            load_err = juris.load_or_update_juris_to_db(
                                self.session,
                                self.d["repository_content_root"],
                                jurisdiction,
                                system_name[jurisdiction],
                            )
                            # track loading
                            err = ui.consolidate_errors([err, load_err])
                            if not ui.fatal_error(load_err):
                                loaded[jurisdiction] = True

                if (not load_jurisdictions) or (loaded[jurisdiction]):
                    # load results
                    try:
                        # load data
                        new_err = load_results_df(
                            self.session,
                            df_dict[(election, jurisdiction)],
                            dict(),
                            jurisdiction,
                            multi_file_name,
                            munger,
                            os.path.join(
                                self.d["repository_content_root"],
                                "jurisdictions",
                                system_name[jurisdiction],
                            ),
                            datafile_id,
                            e_id[election],
                        )
                        if new_err:
                            err = ui.consolidate_errors([err, new_err])
                            if ui.fatal_error(new_err):
                                print(f"\t\tError during data loading: {new_err}")
                                err_str = db.remove_record_from_datafile_table(
                                    self.session, datafile_id
                                )
                                if err_str:
                                    ui.add_new_error(
                                        err,
                                        "database",
                                        self.session.bind.url.database,
                                        f"Error removing record with id {datafile_id}: {err_str}",
                                    )
                                    print("Error removing datafile record")
                                continue
                            else:
                                success[(election, jurisdiction)].append(munger)
                                print(
                                    f"Successful load: {election} {jurisdiction}; "
                                    f"however, see warnings when program finishes"
                                )
                        else:
                            print(f"Successful load: {election} {jurisdiction}")
                    except Exception as exc:
                        err = ui.add_new_error(
                            err,
                            "munger",
                            "load_multi_ej_file",
                            f"Unexpected error while loading results dataframe "
                            f"for {election} {jurisdiction}: {exc}",
                        )
                        err_str = db.remove_vote_counts(self.session, datafile_id)
                        if err_str:
                            err = ui.add_new_error(
                                err,
                                "warn-database",
                                f"{self.session.bind.url.database}",
                                f"Vote counts not removed for datafile id {datafile_id} because of error: {err_str}",
                            )
                        else:
                            err_str = db.remove_record_from_datafile_table(
                                self.session, datafile_id
                            )
                            if err_str:
                                err = ui.add_new_error(
                                    err,
                                    "warn-database",
                                    f"{self.session.bind.url.database}",
                                    f"Vote counts removed but datafile record not removed for datafile id {datafile_id} "
                                    f"because of error: {err_str}",
                                )
                        continue
        ts = datetime.datetime.now().strftime("%m%d_%H%M")
        report_dir = os.path.join(
            self.d["reports_and_plots_dir"],
            f"multielection_{self.session.bind.url.database}_{ts}",
        )
        if report_err_to_file:
            err = ui.report(err, report_dir, suppress_warnings=suppress_warnings)

        return success, err


def check_param_file_elements(
    ini_d: dict,
    mungers_path: str,
    ini_file_name: str,
) -> Optional[dict]:
    """
    Inputs:
        ini_d: dict, dictionary of parameters (presumably taken from parameter file for a particular results file)
        mungers_path: str, path to directory containing mungers
        ini_file_name: str, name of the parameter file (for error reporting)

    Checks consistency of mungers from ini_d["munger_list"] with other parameters in ini_d:
        For each munger:
            All constant_over_file elements from munger are specified in <ini_d>
    Returns:
        Optional[dict], error dictionary
    """
    err = None
    # for each munger,
    for mu in ini_d["munger_list"].split(","):
        # check any constant_over_file elements are defined in .ini file
        munger_file = os.path.join(mungers_path, f"{mu}.munger")
        params, p_err = ui.get_parameters(
            required_keys=list(),
            param_file=munger_file,
            optional_keys=["constant_over_file"],
            header="format",
        )
        if p_err:
            err = ui.consolidate_errors([err, p_err])
        elif params["constant_over_file"]:
            bad_constants = list()
            for c in params["constant_over_file"].split(","):
                if c not in ini_d.keys() or ini_d[c] is None:
                    bad_constants.append(c)
            if bad_constants:
                err = ui.add_new_error(
                    err,
                    "ini",
                    ini_file_name,
                    f"Munger {mu} requires constants to be defined:\n{bad_constants}",
                )
    return err


def check_and_init_singledataloader(
    results_dir: str,
    results_param_file: str,
    session: Session,
    mungers_path: str,
    juris_true_name: str,
    path_to_jurisdiction_dir: str,
) -> (Optional[SingleDataLoader], Optional[dict]):
    """
    Inputs:
        results_dir: str, path to directory containing election result files
        results_param_file: str, path to file containing basic parameters for reading result file (*.ini)
        session: Session,
        mungers_path: str, path to directory containing munger parameter files with extension .munger
        juris_true_name: str, name of jurisdiction (with spaces, not hyphens)
        path_to_jurisdiction_dir: str, path to directory containing subdirectories labeled by jurisdiction

    Checks results_param_file parameters

    Returns:
        Optional[SingleDataLoader], SingleDataLoader instance if it could be successfully initialized (or None if not),
        error dictionary (including munger errors noted in SDL initialization)
    """

    # test parameters
    par_file = os.path.join(results_dir, results_param_file)
    sdl = None
    d, err = ui.get_parameters(
        required_keys=constants.sdl_pars_req,
        optional_keys=constants.sdl_pars_opt,
        param_file=par_file,
        header="election_results",
    )
    if err:
        return sdl, err

    # check consistency of munger and .ini file regarding elements to be read from ini file
    new_err_2 = check_param_file_elements(d, mungers_path, results_param_file)
    if new_err_2:
        err = ui.consolidate_errors([err, new_err_2])
        sdl = None
    if not ui.fatal_error(new_err_2):
        sdl = SingleDataLoader(
            results_dir,
            results_param_file,
            session,
            mungers_path,
            juris_true_name,
            path_to_jurisdiction_dir,
        )
        if sdl is None:
            err = ui.add_new_error(
                err,
                "ini",
                results_param_file,
                f"Error creating SingleDataLoader instance. Required header is [election_results]. "
                f"Required parameters are: {constants.sdl_pars_req}",
            )
    # check download date
    try:
        datetime.datetime.strptime(d["results_download_date"], "%Y-%m-%d")
        err_str = None
    except TypeError:
        err_str = f"No download date found"
    except ValueError:
        err_str = f"Date could not be parsed. Expected format is 'YYYY-MM-DD', actual is {d['results_download_date']}"
    if err_str:
        err = ui.add_new_error(err, "ini", results_param_file, err_str)
    return sdl, err


class JurisdictionPrepper:
    def __new__(
        cls,
        prep_param_file: str = "jurisdiction_prep.ini",
        run_time_param_file: str = "run_time.ini",
        target_dir: Optional[str] = None,
    ):
        """Checks if parameter file exists and is correct. If not, does
        not create JurisdictionPrepper object."""
        for param_file, required in [
            (prep_param_file, constants.prep_pars),
            (run_time_param_file, ["repository_content_root", "reports_and_plots_dir"]),
        ]:
            try:
                d, parameter_err = ui.get_parameters(
                    required_keys=required,
                    param_file=param_file,
                    header="electiondata",
                )
            except FileNotFoundError:
                print(
                    f"File {param_file} not found. Ensure that it is located"
                    " in the current directory. DataLoader object not created."
                )
                return None

            if parameter_err:
                print(f"File {param_file} missing requirements.")
                print(parameter_err)
                print("JurisdictionPrepper object not created.")
                return None
        return super().__new__(cls)

    def new_juris_files(
        self,
        target_dir_for_starter_dictionary: Optional[str] = None,
        templates: Optional[str] = None,
    ) -> Optional[dict]:
        """
        Inputs:
            target_dir_for_starter_dictionary: Optional[str] = None, path to directory where starter dictionary
                should be placed. Default is to put the starter dictionary in the current directory.
            templates: Optional[str] = None, path to directory with templates. Default is
                jurisdictions/000_jurisdiction_templates

        Creates jurisdiction files in the directory self.d["jurisdiction_path"], creating that directory if necessary.
        Creates starter dictionary (to keep new dictionary entries separate from existing entries)

        Returns:
            Optional[dict], error dictionary
        """
        # create and fill jurisdiction directory
        # TODO Feature: allow other districts to be set in param_file
        print(f"\nStarting {inspect.currentframe().f_code.co_name}")
        error = jm.ensure_jurisdiction_dir(
            self.d["repository_content_root"], self.d["jurisdiction_path"]
        )
        # add default entries
        # default templates are from repo
        if not templates:
            templates = os.path.join(
                self.d["repository_content_root"],
                "jurisdictions/000_jurisdiction_templates",
            )
        for element in ["Party"]:
            new_err = electiondata.juris.add_defaults(
                self.d["jurisdiction_path"], templates, element
            )
            if new_err:
                error = ui.consolidate_errors([error, new_err])

        # add all standard Offices/RUs/CandidateContests
        asc_err = self.add_standard_contests()

        # Feature create starter dictionary.txt with cdf_internal name
        #  used as placeholder for raw_identifier_value
        dict_err = self.starter_dictionary(
            target_dir_for_starter_dictionary=target_dir_for_starter_dictionary
        )

        error = ui.consolidate_errors([error, asc_err, dict_err])
        ui.report(
            error,
            self.d["reports_and_plots_dir"],
            file_prefix=f"prep_{self.d['name']}",
        )
        return error

    def add_standard_contests(
        self, juriswide_contests: list = None, other_districts: dict = None
    ) -> Optional[dict]:
        """
        Inputs:
            juriswide_contests: list = None, list of jurisdiction-wide contests (default is calculated from
                self.d["abbreviated_name"] via constants.jurisdiction_wide_contests())
            other_districts: dict = None, optional dictionary mapping other district contest types to the
            number of districts

        Adds lines as necessary to jurisdiction files and starter dictionary for contests derived from the
            jurisdiction prep parameter file and the function arguments

        Returns:
             Optional[dict], error dictionary
        """
        # initialize error dictionary
        err = None

        name = self.d["name"]
        abbr = self.d["abbreviated_name"]
        count = {
            f"{abbr} House": self.state_house,
            f"{abbr} Senate": self.state_senate,
            f"US House {abbr}": self.congressional,
        }
        ru_type = {
            f"{abbr} House": "state-house",
            f"{abbr} Senate": "state-senate",
            f"US House {abbr}": "congressional",
        }
        if other_districts:
            for k in other_districts.keys():
                count[k] = other_districts[k]["count"]
                ru_type[k] = other_districts[k]["ReportingUnitType"]

        w_office = electiondata.juris.get_element(self.d["jurisdiction_path"], "Office")
        w_ru = electiondata.juris.get_element(
            self.d["jurisdiction_path"], "ReportingUnit"
        )
        w_cc = electiondata.juris.get_element(
            self.d["jurisdiction_path"], "CandidateContest"
        )
        cols_off = ["Name", "ElectionDistrict"]
        cols_ru = ["Name", "ReportingUnitType"]
        cols_cc = ["Name", "NumberElected", "Office", "PrimaryParty"]

        # add all district offices/contests/reportingunits
        for k in count.keys():
            # create office records for each district
            w_office = w_office.append(
                pd.DataFrame(
                    [
                        [f"{k} District {i + 1}", f"{name};{k} District {i + 1}"]
                        for i in range(count[k])
                    ],
                    columns=cols_off,
                ),
                ignore_index=True,
            )
            w_ru = w_ru.append(
                pd.DataFrame(
                    [
                        [f"{name};{k} District {i + 1}", ru_type[k]]
                        for i in range(count[k])
                    ],
                    columns=cols_ru,
                ),
                ignore_index=True,
            )
            w_cc = w_cc.append(
                pd.DataFrame(
                    [
                        [f"{k} District {i + 1}", 1, f"{k} District {i + 1}", ""]
                        for i in range(count[k])
                    ],
                    columns=cols_cc,
                ),
                ignore_index=True,
            )

        # append top jurisdiction reporting unit
        top_ru = {
            "Name": self.d["name"],
            "ReportingUnitType": self.d["reporting_unit_type"],
        }
        w_ru = w_ru.append(top_ru, ignore_index=True)

        # add standard jurisdiction-wide offices
        if not juriswide_contests:
            juriswide_contests = constants.jurisdiction_wide_contests(abbr)

        # append jurisdiction-wide offices to the office df
        jw_off = pd.DataFrame(
            [[x, self.d["name"]] for x in juriswide_contests], columns=cols_off
        )
        w_office = w_office.append(jw_off, ignore_index=True)

        # append jurisdiction-wide contests to the working candidate contest df
        jw_cc = pd.DataFrame(
            [[x, 1, x, ""] for x in juriswide_contests], columns=cols_cc
        )
        w_cc = w_cc.append(jw_cc, ignore_index=True)

        # write office df to Office.txt
        new_err = electiondata.juris.write_element(
            self.d["jurisdiction_path"], "Office", w_office.drop_duplicates()
        )
        if new_err:
            err = ui.consolidate_errors([err, new_err])
            if ui.fatal_error(new_err):
                return err

        new_err = electiondata.juris.write_element(
            self.d["jurisdiction_path"], "ReportingUnit", w_ru.drop_duplicates()
        )
        if new_err:
            err = ui.consolidate_errors([err, new_err])
            if ui.fatal_error(new_err):
                return err
        new_err = electiondata.juris.write_element(
            self.d["jurisdiction_path"], "CandidateContest", w_cc.drop_duplicates()
        )
        if new_err:
            err = ui.consolidate_errors([err, new_err])
            if ui.fatal_error(new_err):
                return err
        return err

    def starter_dictionary(
        self,
        include_existing=True,
        target_dir_for_starter_dictionary: Optional[str] = None,
    ) -> Optional[dict]:
        """
        Inputs:
            include_existing=True, if True, includes lines from dicionary.txt in self.d["jurisdiction_path"] into
                the starter dictionary
            target_dir_for_starter_dictionary: Optional[str] = None, path to directory for starter dictionary
                (default is current directory)

        Creates a starter dictionary.txt wtih raw_identifiers the same as cdf_internal names.

        Returns:
             Optional[dict], error dictionary
        """
        w = dict()
        elements = [
            "BallotMeasureContest",
            "Candidate",
            "CandidateContest",
            "Party",
            "ReportingUnit",
        ]
        old = electiondata.juris.get_element(self.d["jurisdiction_path"], "dictionary")
        if not include_existing:
            old.drop()
        for element in elements:
            w[element] = electiondata.juris.get_element(
                self.d["jurisdiction_path"], element
            )
            name_field = db.get_name_field(element)
            w[element] = m.add_constant_column(w[element], "cdf_element", element)
            w[element].rename(columns={name_field: "cdf_internal_name"}, inplace=True)
            w[element]["raw_identifier_value"] = w[element]["cdf_internal_name"]

        # add lines for CountItemTypes to dictionary
        w["CountItemType"] = pd.DataFrame(
            [
                ["CountItemType", cit, cit]
                for cit in constants.nist_standard["CountItemType"]
            ],
            columns=["cdf_element", "cdf_internal_name", "raw_identifier_value"],
        )

        starter_file_name = f'{self.d["abbreviated_name"]}_starter_dictionary.txt'
        starter = pd.concat(
            [
                w[element][["cdf_element", "cdf_internal_name", "raw_identifier_value"]]
                for element in (elements + ["CountItemType"])
            ]
        ).drop_duplicates()
        if target_dir_for_starter_dictionary:
            ssd_str = starter_dict_dir = target_dir_for_starter_dictionary
        else:
            ssd_str = "current directory (not in jurisdiction directory)"
            starter_dict_dir = "."
        err = electiondata.juris.write_element(
            starter_dict_dir, "dictionary", starter, file_name=starter_file_name
        )
        print(f"Starter dictionary created in {ssd_str}:\n{starter_file_name}")
        return err

    def add_sub_county_rus(
        self,
        par_file_name: str,
        sub_ru_type: str = "precinct",
        county_type=constants.default_subdivision_type,
        dbname: Optional[str] = None,
        paraam_file: Optional[str] = None,
    ) -> Optional[dict]:
        """Do not use! May be obsolete, may be broken"""
        err_list = list()
        dl = DataLoader(dbname=dbname, param_file=paraam_file)
        path_to_jurisdiction_dir = os.path.join(
            self.d["jurisdiction_path"], self.d["___"]
        )
        sdl, err = check_and_init_singledataloader(
            dl.d["results_dir"],
            par_file_name,
            dl.session,
            dl.d["mungers_dir"],
            self.d["name"],
            self.d["jurisdiction_path"],
        )
        if not sdl:
            return err

        for mu in sdl.munger_list:
            # get parameters
            m_path = os.path.join(sdl.mungers_path, f"{mu}.munger")
            mu_d, new_err = m.get_and_check_munger_params(m_path)
            # get ReportingUnit formula
            ru_formula = ""
            headers = [
                x
                for x in electiondata.constants.req_munger_parameters[
                    "munge_field_types"
                ]
                if x != "constant_over_file"
            ]
            for header in headers:
                formulas, formula_err = ui.get_parameters(
                    required_keys=[],
                    optional_keys=["ReportingUnit"],
                    header=header,
                    param_file=m_path,
                )
                if formula_err or (formulas["ReportingUnit"] is None):
                    pass
                else:  # TODO tech debt: we assume at most one formula
                    ru_formula = formulas["ReportingUnit"]
            # check that ReportingUnit formula is <county_type>;<sub_ru>
            if ";" not in ru_formula:
                new_err = ui.add_new_error(
                    new_err,
                    "warn-munger",
                    mu,
                    "ReportingUnit formula has no ';'",
                )
                if new_err:
                    err_list.append(new_err)
            else:
                # create raw -> internal dictionary of county_type names
                jd_df = electiondata.juris.get_element(
                    self.d["jurisdiction_path"], "dictionary"
                )
                ru_df = electiondata.juris.get_element(
                    self.d["jurisdiction_path"], "ReportingUnit"
                )
                internal = (
                    jd_df[jd_df["cdf_element"] == "ReportingUnit"]
                    .merge(
                        ru_df[ru_df["ReportingUnitType"] == county_type],
                        left_on="cdf_internal_name",
                        right_on="Name",
                        how="inner",
                    )[["raw_identifier_value", "cdf_internal_name"]]
                    .set_index("raw_identifier_value")
                    .to_dict()["cdf_internal_name"]
                )
                # get list of ReportingUnit raw values from results file
                vals, new_err = sdl.list_values("ReportingUnit")
                county = {v: v.split(";")[0] for v in vals}
                remainder = {v: v[len(county[v]) + 1 :] for v in vals}
                good_vals = [v for v in vals if county[v] in internal.keys()]

                # write to ReportingUnit.txt
                new_err = electiondata.juris.write_element(
                    self.d["jurisdiction_path"],
                    "ReportingUnit",
                    pd.concat(
                        [
                            ru_df,
                            pd.DataFrame(
                                [
                                    [
                                        f"{internal[county[v]]};{remainder[v]}",
                                        sub_ru_type,
                                    ]
                                    for v in good_vals
                                ],
                                columns=["Name", "ReportingUnitType"],
                            ),
                        ]
                    ),
                )
                if new_err:
                    err_list.append(new_err)

                # write to dictionary.txt
                new_err = electiondata.juris.write_element(
                    self.d["jurisdiction_path"],
                    "dictionary",
                    pd.concat(
                        [
                            jd_df,
                            pd.DataFrame(
                                [
                                    [
                                        "ReportingUnit",
                                        f"{internal[county[v]]};{remainder[v]}",
                                        v,
                                    ]
                                    for v in good_vals
                                ],
                                columns=[
                                    "cdf_element",
                                    "cdf_internal_name",
                                    "raw_identifier_value",
                                ],
                            ),
                        ]
                    ),
                )
                if new_err:
                    err_list.append(new_err)
        err = ui.consolidate_errors(err_list)
        return err

    def make_ini_file(
        self,
        ini_name: str,
        munger_name_list: str,
        is_preliminary: bool = False,
    ):
        """Do not use! May be broken"""
        juris_true_name = self.d["name"]
        juris_system_name = self.d["system_name"]

        # make ini file
        inis_dir = os.path.join(
            Path(self.d["mungers_dir"]).parent, "ini_files_for_results"
        )
        juris_ini_dir = os.path.join(inis_dir, juris_system_name)
        new_ini_file = os.path.join(juris_ini_dir, ini_name)
        if not os.path.isdir(juris_ini_dir):
            os.mkdir(juris_ini_dir)
        if not os.path.isfile(new_ini_file):
            ini_replace = {
                "results_file=": f"results_file={juris_system_name}/",
                f"munger_list=": f"munger_list={munger_name_list}",
                "jurisdiction=": f"jurisdiction={juris_true_name}",
                "results_short_name=": f"results_short_name={Path(ini_name).stem}",
            }
            if is_preliminary:
                ini_replace.update({"is_preliminary=False": "is_preliminary=True"})
            create_from_template(
                os.path.join(inis_dir, "single_election_jurisdiction_template.ini"),
                new_ini_file,
                ini_replace,
            )
        return

    def make_munger_file(self, munger_name: str):
        """Do not use! May be broken"""
        new_munger_file = os.path.join(self.d["mungers_dir"], f"{munger_name}.munger")
        if not os.path.isfile(new_munger_file):
            munger_replace = dict()
            create_from_template(
                os.path.join(self.d["mungers_dir"], "000_template.munger"),
                new_munger_file,
                munger_replace,
            )
        return

    def add_contests_from_file(
        self,
        file_path: str,
    ) -> Optional[dict]:
        """Do not use! May be broken!"""
        err = None
        try:
            df = pd.read_csv(file_path, sep="\t")
        except FileNotFoundError:
            err = ui.add_new_error(err, "file", file_path, f"File not found")
            return err
        except Exception as e:
            err = ui.add_new_error(
                err, "file", file_path, f"Exception while reading file: {e}"
            )
            return err
        err = juris.add_candidate_contests(self.d[""], df, file_path)
        return err

    def __init__(
        self,
        prep_param_file: str = "jurisdiction_prep.ini",
        run_time_param_file: str = "run_time.ini",
        target_dir: Optional[str] = None,
    ):
        """
        Inputs:
            prep_param_file: str = "jurisdiction_prep.ini", path to file with parameters for the new jurisdiction
            run_time_param_file: str = "run_time.ini", path to main parameter file
            target_dir: Optional[str] = None, path to directory where new files should be placed. Default is a
                subdirectory named for the jurisdiction specified in <prep_param_file> in the src/jurisdictions
                directory in the repository.

        Returns a JurisdictionPrepper instance with attributes:
            d, dictionary of parameters from the main system parameter file and the parameters in the jurisdiction prep
                parameter file
            state_house, integer number of state house districts (per jurisdiction prep parameter file)
            state_senate, integer number of state senate districts (per jurisdiction prep parameter file)
            congressional , integer number of congressional districts (per jurisdiction prep parameter file)
        """
        self.d = dict()
        # get parameters from jurisdiction_prep.ini and run_time.ini
        for param_file, required in [
            (prep_param_file, constants.prep_pars),
            (run_time_param_file, ["repository_content_root", "reports_and_plots_dir"]),
        ]:
            d, parameter_err = ui.get_parameters(
                required_keys=required,
                param_file=param_file,
                header="electiondata",
            )
            self.d.update(d)

        # add dictionary attributes derived from other parameters
        if target_dir:
            juris_path = target_dir
        else:
            juris_path = os.path.join(
                self.d["repository_content_root"],
                "jurisdictions",
                jm.system_name_from_true_name(self.d["name"]),
            )
        derived = {
            "system_name": jm.system_name_from_true_name(self.d["name"]),
            "mungers_dir": os.path.join(self.d["repository_content_root"], "mungers"),
            "jurisdiction_path": juris_path,
        }
        self.d.update(derived)

        # add direct attributes derived from other parameters
        self.state_house = int(self.d["count_of_state_house_districts"])
        self.state_senate = int(self.d["count_of_state_senate_districts"])
        self.congressional = int(self.d["count_of_us_house_districts"])


def make_ini_file_batch(
    results_directory: str,
    target_directory: str,
    munger_list: str,
    jurisdiction: str,
    election: str,
    download_date: str = "1900-01-01",
    source: str = "unknown",
    results_note: str = "none",
    extension: Optional[str] = None,
):
    """
    Required inputs:
        results_directory: str, directory holding results files to be described in *.ini files
            created by this routine
        output_directory: str, directory to receive files created by this routine
        munger_list: str, value of munger_list parameter in files created
        jurisdiction: str, value of jurisdiction parameter in files created
        election: str, value of election parameter in files created

    Optional inputs:
        download_date: str = "1900-01-01", value of download_date parameter in files created
        source: str = "unknown", value of source parameter in files created
        results_note: str = "none", value of results_note parameter in files created
        extension: Optional[str] = None, if given, restrict to results files with the given extenstion

    Creates a SingleDataLoader parameter file in the <target_directory> for each result file
        in the <results_directory> (or each whose extension matches <extension>, if given).
        Files in the <results_directory> whose names start with ".", or whose extensions are ".ini",
        are ignored.
    """
    if extension:
        data_file_list = [
            f
            for f in os.listdir(results_directory)
            if f[-len(extension) :] == extension
        ]
    else:
        data_file_list = [
            f
            for f in os.listdir(results_directory)
            if (f[-4:] != ".ini") & (f[0] != ".")
        ]
    juris_system_name = jm.system_name_from_true_name(jurisdiction)
    for f in data_file_list:
        par_text = (
            f"[election_results]\nresults_file={juris_system_name}/{f}\n"
            f"munger_list={munger_list}\njurisdiction={jurisdiction}\nelection={election}\n"
            f"results_short_name={jurisdiction}_{f}\nresults_download_date={download_date}\n"
            f"results_source={source}\nresults_note={results_note}\n"
        )
        ini_file_name = ".".join(f.split(".")[:-1]) + ".ini"
        with open(os.path.join(target_directory, ini_file_name), "w") as p:
            p.write(par_text)
    return


class Analyzer:
    def __new__(
        cls,
        param_file: str = None,
        dbname: str = None,
        major_subdivision_file: str = None,
    ):
        """
        Optional inputs:
            param_file: str = None, path to file with main parameters for [electiondata] and [postgres].
                Default is "run_time.ini"
            dbname: str = None, name of database. Default is name in <param_file>
            major_subdivision_file: str = None, path to file with columns
                'jurisdiction', 'major_subjurisdiction_type'
        Checks:
            parameter file exists and is has necessary parameters
            connection to database
            Major subdivision type available for each jurisdiction with data in the database
        """
        try:
            if not param_file:
                param_file = "run_time.ini"
            db_params, postgres_param_err = ui.get_parameters(
                required_keys=["dbname", "host", "port", "user", "password"],
                param_file=param_file,
                header="postgresql",
            )
            d, eda_err = ui.get_parameters(
                required_keys=["reports_and_plots_dir", "repository_content_root"],
                param_file=param_file,
                header="electiondata",
            )
            if eda_err:
                print(eda_err)
                if ui.fatal_error(eda_err):
                    return None
        except FileNotFoundError:
            print(
                f"Parameter file '{param_file}' not found. .\nAnalyzer object not created."
            )
            return None

        if postgres_param_err or eda_err:
            print(f"Parameter file {param_file} missing requirements.")
            print(f"postgres: {postgres_param_err}")
            print(f"elections: {eda_err}")
            print("Analyzer object not created.")
            return None

        # test connection to db
        try:
            db_engine, err = db.sql_alchemy_connect(db_params=db_params, dbname=dbname)
            Session = sessionmaker(bind=db_engine)
            session = Session()
            # if error is fatal
            if ui.fatal_error(err):
                print(
                    f"No Analyzer created, because connection not established to database with these parameters: \n"
                    f"dbname={dbname}\ndb_params={db_params}"
                )
                return None
            # if err contains warning (but no fatal error)
            elif err:
                print(f"Warning: {err}")
        except Exception as exc:
            print(
                f"No Analyzer created, because connection not established to database with these parameters: \n"
                f"dbname={dbname}\ndb_params={db_params}\n\n"
                f"Exception raised: {exc}"
            )
            return None

        # test major subdivision dictionary:
        ok, err_str = check_major_subdivisions(
            session=session,
            content_root=d["repository_content_root"],
            major_subdivision_file=major_subdivision_file,
        )
        if not ok:
            print(err_str)
            return None

        if db_engine:
            db_engine.dispose()
        return super().__new__(cls)

    def __init__(
        self,
        param_file: str = None,
        dbname: str = None,
        major_subdivision_file: str = None,
    ):
        """
        Optional inputs:
            param_file=None, if given, path to file with parameters necessary for creating an Analyzer
                instance. Default is "run_time.ini"
            dbname=None, if given, name of database from which to export & analyze data. Default
                is the value of the dbname parameter in the [postgres] section of the param_file
            major_subdivision_file: str = None, path to file with columns
                'jurisdiction', 'major_subjurisdiction_type'

        Creates instance of Analyzer with attributes:
            d, dictionary of parameters from param_file
            reports_and_plots_dir, path to directory for plots and reports created by the Analyzer
            repository_content_root, path to repository content root (for access to, e.g., the
                file major_subdivision_types.txt
            session, sqlalchemy session connected to database
            major_subdivision_type, dictionary mapping jurisdiction names to ReportingUnitType of major subdivision
        """
        if not param_file:
            param_file = "run_time.ini"

        # read reports_and_plots_dir from param_file
        d, error = ui.get_parameters(
            required_keys=["reports_and_plots_dir", "repository_content_root"],
            param_file=param_file,
            header="electiondata",
        )
        self.reports_and_plots_dir = d["reports_and_plots_dir"]
        self.repository_content_root = d["repository_content_root"]

        # create session
        eng, err = db.sql_alchemy_connect(db_param_file=param_file, dbname=dbname)
        Session = sessionmaker(bind=eng)
        self.session = Session()

        # get dictionary of major subdivision types
        self.major_subdivision_type, new_err = get_major_subdivisions(
            session=self.session,
            content_root=self.repository_content_root,
            major_subdivision_file=major_subdivision_file,
        )

    # testing methods
    def test_loaded_results(
        self,
        election: str,
        juris_true_name: str,
        juris_system_name: str,
        reference_results: Optional[str] = None,
        status: Optional[str] = None,
    ) -> Optional[dict]:
        """
        Required inputs:
             election: str,
             juris_true_name: str,
             juris_system_name: str,
        Optional inputs:
             reference: Optional[str] = None, if given, use this path to file of reference results. If not
                 given, reference results are taken from file
                 <repository_content_root>/reference_results/<juris_system_name>.tsv
             status: Optional[str] = None, if given, checks only the reference results with that same status

         Reports information from tests to self.reports_and_plots.
         Returns:
             Optional[dict], error dictionary with only "warn-test" and no "test" keys (since calling function may or
                 may not view failing tests as fatal
        """
        err = None

        # consistency tests
        if not self.data_exists(election, juris_true_name):
            err = ui.add_new_error(
                err,
                "warn-test",
                f"Election: {election}; Jurisdiction: {juris_true_name}",
                f"\nNo data found",
            )
        if not self.check_totals_match_vote_types(
            election,
            juris_true_name,
            sub_unit_type=self.major_subdivision_type[juris_true_name],
        ):
            err = ui.add_new_error(
                err,
                "warn-test",
                f"Election: {election}; Jurisdiction: {juris_true_name}",
                f"\nSum of other vote types does not match total "
                f"for every {self.major_subdivision_type[juris_true_name]}",
            )
        # report contests with unknown candidates
        bad_contests = self.get_contests_with_unknown_candidates(
            election, juris_true_name, report_dir=self.reports_and_plots_dir
        )
        if bad_contests:
            bad_str = "\n".join(bad_contests)
            err = ui.add_new_error(
                err,
                "warn-test",
                f"Election: {election}; Jurisdiction: {juris_true_name}",
                f"\nSome contests have at least one unknown candidate:\n{bad_str}",
            )

        # test against reference result totals
        if reference_results is None:
            reference_results = os.path.join(
                self.repository_content_root,
                "reference_results",
                f"{juris_system_name}.tsv",
            )
        (
            not_found,
            ok,
            wrong,
            significantly_wrong,
            sub_dir,
            new_err,
        ) = self.compare_to_results_file(
            reference_results,
            single_election=election,
            single_jurisdiction=juris_true_name,
            report_dir=self.reports_and_plots_dir,
            status=status,
        )
        err = ui.consolidate_errors([err, new_err])

        return err

    def data_exists(
        self,
        election: str,
        jurisdiction: str,
    ) -> bool:
        """
        Inputs:
            election: str, name of election (e.g., "2020 General")
            jurisdiction: str, name of jurisdiction (e.g., "New Mexico")

        Returns:
            bool, True if the database of self.session has data for this election-jurisdiction pair
        """
        if not self:
            return False

        election_id = db.name_to_id(self.session, "Election", election)
        jurisdiction_id = db.name_to_id(self.session, "ReportingUnit", jurisdiction)

        # if the database doesn't have the election or the reporting unit
        if not election_id or not jurisdiction_id:
            # data doesn't exist
            return False

        # read all contests with records in the VoteCount table
        df = db.read_vote_count(
            self.session,
            election_id=election_id,
            jurisdiction_id=jurisdiction_id,
            fields=["ContestName"],
            aliases=["contest_name"],
        )
        # if no contest found
        if df.empty:
            # no data exists.
            return False
        # otherwise
        else:
            # then data exists!
            return True

    def check_totals_match_vote_types(
        self,
        election: str,
        jurisdiction: str,
        sub_unit_type: Optional[str] = None,
    ) -> bool:
        """
        Required inputs:
            election: str, name of election
            jurisdiction: str, name of jurisdiction

        Optional inputs:
            sub_unit_type: str = None, the subdivision type (e.g., 'county') to which results should be rolled up.
                Default is constants.default_subdivision_type

        Returns:
            bool, True if only count type is 'total' or if sum of rolled-up counts for other vote types matches
                value of 'total' for each subdivision of the given type
        """

        if sub_unit_type is None:
            sub_unit_type = constants.default_subdivision_type
        active = db.active_vote_types(self.session, election, jurisdiction)
        if len(active) > 1 and "total" in active:
            # pull type 'total' only
            df_candidate = self.aggregate(
                election,
                jurisdiction,
                contest_type="Candidate",
                vote_type="total",
                sub_unit_type=sub_unit_type,
                exclude_redundant_total=False,
            )
            df_ballot = self.aggregate(
                election,
                jurisdiction,
                contest_type="BallotMeasure",
                vote_type="total",
                sub_unit_type=sub_unit_type,
                exclude_redundant_total=False,
            )
            df_total_type_only = pd.concat([df_candidate, df_ballot])

            # pull all types but total
            df_candidate = self.aggregate(
                election,
                jurisdiction,
                contest_type="Candidate",
                sub_unit_type=sub_unit_type,
                exclude_redundant_total=True,
            )
            df_ballot = self.aggregate(
                election,
                jurisdiction,
                contest_type="BallotMeasure",
                exclude_redundant_total=True,
                sub_unit_type=sub_unit_type,
            )
            df_sum_nontotal_types = pd.concat([df_candidate, df_ballot])
            return (
                df_total_type_only["count"].sum()
                == df_sum_nontotal_types["count"].sum()
            )
        else:
            return True

    def contest_total(
        self,
        election: str,
        jurisdiction: str,
        contest: str,
        reporting_unit: str,
        vote_type: Optional[str] = None,
        contest_type: Optional[str] = "Candidate",
    ) -> Optional[int]:
        """
        Required inputs:
            election: str, name of election for contest
            jurisdiction: str, name of jurisdiction for contest
            contest: str, name of contest
            reporting_unit: str, reporting unit over which total should be taken

        Optional inputs:
            vote_type: Optional[str] = None, if given, give total for that vote type only
            contest_type: Optional[str] = "Candidate", type of contest (other option is "BallotMeasure") since
                CandidateContests and BallotMeasureContests are handled differently in database

        Returns:
            Optional[int], total number of votes in the contest in the given election and jurisdiction within the given
        reporting unit. If vote type is given, restricts to that vote type. If no data is found, returns None.
        """
        sub_unit_type = db.get_reporting_unit_type(self.session, reporting_unit)
        if vote_type == "total":
            exclude_redundant_total = False
        else:
            exclude_redundant_total = True
        df = self.aggregate(
            election=election,
            jurisdiction=jurisdiction,
            vote_type=vote_type,
            sub_unit=reporting_unit,
            sub_unit_type=sub_unit_type,
            contest=contest,
            contest_type=contest_type,
            exclude_redundant_total=exclude_redundant_total,
        )
        if df.empty:
            return None
        else:
            return df["count"].sum()

    def check_count_types_standard(
        self,
        election: str,
        jurisdiction: str,
    ) -> bool:
        """
        Required inputs:
            election: str,
            jurisdiction: str,

        Returns:
            bool, True if CountItemType for all vote counts for the given election and jurisdiction
                are on the standard list given in the NIST Election Results Reporting common data format
        """
        standard_ct_list = constants.nist_standard["CountItemType"]

        active = db.active_vote_types(self.session, election, jurisdiction)
        for vt in active:
            # if even one fails, count types are not standard
            if vt not in standard_ct_list:
                return False
        # if nothing failed, count types are standard
        return True

    def get_contests_with_unknown_candidates(
        self,
        election: str,
        jurisdiction: str,
        report_dir: Optional[str] = None,
    ) -> List[str]:
        """
        Required inputs:
            election: str,
            jurisdiction: str,
        Optional inputs:
            report_dir: Optional[str] = None, if given, path to directory for file reporting bad contests;
                if not given, no report created.

        Returns:
             List[str], list of contests with unknown candidates
        """
        election_id = db.name_to_id(self.session, "Election", election)
        if not election_id:
            return [f"Election {election} not found"]
        jurisdiction_id = db.name_to_id(self.session, "ReportingUnit", jurisdiction)
        if not jurisdiction_id:
            return [f"Jurisdiction {jurisdiction} not found"]

        contests = db.get_contest_with_unknown(
            self.session, election_id, jurisdiction_id
        )
        if contests and report_dir:
            ts = datetime.datetime.now().strftime("%m%d_%H%M")
            subdir = os.path.join(report_dir, f"bad_contests_{ts}")
            Path(subdir).mkdir(exist_ok=True, parents=True)
            election_system_name = jm.system_name_from_true_name(election)
            juris_system_name = jm.system_name_from_true_name(jurisdiction)
            open(
                os.path.join(subdir, f"{election_system_name}_{juris_system_name}.tsv"),
                "w",
            ).write("\n".join(contests))

        return contests

    # visualization methods
    # May need to add back 'verbose' option for compatibility with VoteVisualizer front end.
    #    See github issue #524 for details
    def display_options(
        self, input_str: str, filters: List[str] = None
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Required Inputs:
            input_str: str, one of: 'election', 'jurisdiction', 'contest_type', 'contest',
                'category' or 'count'
        Optional Inputs:
            filters: List[str] = None, if given, these strings will be used to filter the results

        Returns:
            Optional[List[Dict[str, Any]]], on exception, returns None. Otherwise, # TODO

        """
        try:
            filters_mapped = ui.get_contest_type_mappings(filters)
            results = ui.get_filtered_input_options(
                self.session, input_str, filters_mapped, self.major_subdivision_type
            )
        except Exception as exc:
            results = None
        return results

    def scatter(
        self,
        jurisdiction: str,
        h_election: str,
        h_category: str,
        h_count: str,  # horizontal axis params
        v_election: str,
        v_category: str,
        v_count: str,  # vertical axis params
        fig_type: str = None,
    ) -> Optional[dict]:
        """
        Required inputs:
            jurisdiction: str,
            for horizontal axis of scatter plot:
                h_election: str, election parameter
                h_category: str, category parameter (e.g., Population by Race or Candidate total)
                h_count: str,  count label parameter (e.g., "Black" or "Joseph R. Biden", depending on category)
            for vertical axis of scatter plot (same definitions as for horizontal):
                v_election: str,
                v_category: str,
                v_count: str,
        Optional input:
            fig_type: str = None, an image format string from plotly - as of 8/2021, includes
                html, png, jpeg, webp, svg, pdf, and eps. Note that some filetypes may need
                plotly-orca installed as well.

        If <fig_type> is given and points for scatter are found, creates a scatter plot
            in the self.reports_and_plots_dir directory with file extension and format determined by fig_type.

        Returns:
            dict, dictionary of data for creating the scatter plot (including title, axis titles, etc.)
        """
        jurisdiction_id = db.name_to_id(self.session, "ReportingUnit", jurisdiction)
        subdivision_type = self.major_subdivision_type[jurisdiction]
        h_election_id = db.name_to_id(self.session, "Election", h_election)
        v_election_id = db.name_to_id(self.session, "Election", v_election)
        # *_type is either candidates or contests or parties
        h_type, h_count_item_type = self.split_category_input(h_category)
        v_type, v_count_item_type = self.split_category_input(v_category)
        h_runoff = h_count.endswith("Runoff")
        v_runoff = v_count.endswith("Runoff")
        h_count = h_count.split(" - ")[0].strip()
        v_count = v_count.split(" - ")[0].strip()

        agg_results = an.create_scatter(
            self.session,
            jurisdiction_id,
            subdivision_type,
            h_election_id,
            h_count_item_type,
            h_count,
            h_type,
            h_runoff,
            v_election_id,
            v_count_item_type,
            v_count,
            v_type,
            v_runoff,
        )
        if fig_type and agg_results:
            viz.plot("scatter", agg_results, fig_type, self.reports_and_plots_dir)
        return agg_results

    def bar(
        self,
        election: str,
        jurisdiction: str,
        contest_type: str = None,
        contest: str = None,
        fig_type: str = None,
    ) -> Optional[List[dict]]:
        """
        Required inputs:
            election: str,
            jurisdiction: str,
        Optional input:
            contest_type: str = None, an election district type, e.g.,
                state, congressional, state-senate, state-house, territory, etc.
                Complete list is given by the keys of <constants.contest_type_mapping>
            contest: str = None,
            fig_type: str = None, an image format string from plotly - as of 8/2021, includes
                html, png, jpeg, webp, svg, pdf, and eps. Note that some filetypes may need
                plotly-orca installed as well.

        If <fig_type> is given and points for scatter are found, creates a scatter plot
            in the self.reports_and_plots_dir directory with file extension and format determined by fig_type.

        Returns:
            List[dict],
        """
        election_id = db.name_to_id(self.session, "Election", election)
        jurisdiction_id = db.name_to_id(self.session, "ReportingUnit", jurisdiction)
        # for now, bar charts can only handle jurisdictions where major subdivision type is one level
        # down from the jurisdiction
        # TODO is this still true? ^^
        agg_results = an.create_bar(
            self.session,
            election_id,
            jurisdiction_id,
            self.major_subdivision_type[jurisdiction],
            contest_district_type=contest_type,
            contest_or_contest_group=contest,
            for_export=False,
        )
        if fig_type and agg_results:
            for agg_result in agg_results:
                viz.plot("bar", agg_result, fig_type, self.reports_and_plots_dir)
        return agg_results

    def split_category_input(self, input_str: str):
        """Helper function. Takes an input from the front end that is the cartesian
        product of the CountItemType and {'Candidate', 'Contest', 'Party'}. So something like:
        Candidate total or Contest absentee-mail. Cleans this and returns
        something usable for the system to identify what the user is asking for."""
        if input_str.startswith("Candidate"):
            return "candidates", input_str.replace("Candidate", "").strip()
        elif input_str.startswith("Contest"):
            return "contests", input_str.replace("Contest", "").strip()
        elif input_str.startswith("Party"):
            return "parties", input_str.replace("Party", "").strip()
        elif input_str.startswith("Population"):
            return "Population", input_str.replace("Population", "").strip()

    def export_outlier_data(
        self,
        election: str,
        jurisdiction: str,
        contest: str = None,
    ) -> Optional[List[dict]]:
        """Not ready for prime time
        contest_type is one of state, congressional, state-senate, state-house"""
        election_id = db.name_to_id(self.session, "Election", election)
        jurisdiction_id = db.name_to_id(self.session, "ReportingUnit", jurisdiction)
        # bar chart always at one level below top reporting unit
        agg_results = an.create_bar(
            self.session,
            election_id,
            jurisdiction_id,
            self.major_subdivision_type[jurisdiction],
            contest_or_contest_group=contest,
            for_export=True,
        )
        return agg_results

    def top_counts(
        self, election: str, jurisdiction: str, sub_rutype: str, by_vote_type: bool
    ) -> Optional[str]:
        """
        Inputs:
            election: str,
            jurisdiction: str,
            sub_rutype: str, ReportingUnitType (e.g., 'county') to which the results should be rolled up
            by_vote_type: bool, if true, results will be reported by vote type. If false, only totals will be reported

        Puts file with results into a subdirectory (labeled by election and jurisdiction name)
            of the reports_and_plots_dir specified in the Analyzer's param_file.
        """
        jurisdiction_id = db.name_to_id(self.session, "ReportingUnit", jurisdiction)
        election_id = db.name_to_id(self.session, "Election", election)
        err = an.export_rollup(
            self.session,
            self.reports_and_plots_dir,
            jurisdiction_id=jurisdiction_id,
            sub_rutype=sub_rutype,
            election_id=election_id,
            by_vote_type=by_vote_type,
        )
        return err

    def export_nist(
        self,
        election: str,
        jurisdiction,
    ) -> Union[str, Dict[str, Any]]:
        """picks either json or xml based on value of constants.nist_version"""
        if electiondata.constants.default_nist_format == "json":
            return self.export_nist_json(election,jurisdiction)
        elif electiondata.constants.default_nist_format == "xml":
            return self.export_nist_xml_as_string(election,jurisdiction)
        else:
            return ""

    def export_nist_json(self,election: str,jurisdiction: str) -> Dict[str,Any]:
        election_id = db.name_to_id(self.session, "Election", election)
        jurisdiction_id = db.name_to_id(self.session, "ReportingUnit", jurisdiction)

        election_report = dict()

        election_report["Contest"] = an.nist_candidate_contest(
            self.session, election_id, jurisdiction_id
        )
        election_report["GpUnit"] = an.nist_reporting_unit(
            self.session, election_id, jurisdiction_id
        )
        election_report["Party"] = an.nist_party(
            self.session, election_id, jurisdiction_id
        )
        election_report["Election"] = an.nist_election(
            self.session, election_id, jurisdiction_id
        )
        election_report["Office"] = an.nist_office(
            self.session, election_id, jurisdiction_id
        )
        election_report["Candidate"] = an.nist_candidate(
            self.session, election_id, jurisdiction_id
        )

        return election_report

    def export_nist_json_as_string(
        self,
        election: str,
        jurisdiction: str,
    ) -> str:
        """exports NIST v2 json string"""
        json_string = json.dumps(self.export_nist_json(election,jurisdiction))
        return json_string

    def export_nist_xml_as_string(
        self,
        election: str,
        jurisdiction: str,
    ) -> str:
        """exports NIST v2 xml string"""
        xml_tree, err = nist.nist_v2_xml_export_tree(
            self.session,
            election,
            jurisdiction,
            rollup_subdivision_type=self.major_subdivision_type[jurisdiction],
            issuer=electiondata.constants.default_issuer,
            issuer_abbreviation=electiondata.constants.default_issuer_abbreviation,
            status=electiondata.constants.default_status,
            vendor_application_id=electiondata.constants.default_vendor_application_id,
        )
        if (xml_tree.getroot() is None) or ui.fatal_error(err):
            xml_string = ""
        else:
            xml_string = ET.tostring(
                xml_tree.getroot(),
                encoding="unicode",  # to ensure string is returned, per ET docs
                method="xml",
            )
        return xml_string

    def export_election_to_tsv(
        self, target_file: str, election: str, jurisdiction: Optional[str] = None
    ):
        """
        Required inputs:
            target_file: str, path to file
            election: str,
        Optional inputs:
            jurisdiction: Optional[str] = None,

        Exports all election results from <self.session>'s database for the election <election> (and the jurisdiction
            <jurisdiction>, if given) to the <target_file>. Columns exported are:  "Election",
            "Contest", "Selection", "Party", "ReportingUnit", "VoteType", "Count", "Preliminary"
        """
        # get internal ids for election (and maybe jurisdiction too)
        election_id = db.name_to_id(self.session, "Election", election)
        if jurisdiction is not None:
            jurisdiction_id = db.name_to_id(self.session, "ReportingUnit", jurisdiction)
        else:
            jurisdiction_id = None

        # get counts
        df = db.read_vote_count(
            self.session,
            election_id=election_id,
            jurisdiction_id=jurisdiction_id,
            fields=[
                "ElectionName",
                "ContestName",
                "BallotName",
                "PartyName",
                "GPReportingUnitName",
                "CountItemType",
                "Count",
                "is_preliminary",
            ],
            aliases=[
                "Election",
                "Contest",
                "Selection",
                "Party",
                "ReportingUnit",
                "VoteType",
                "Count",
                "Preliminary",
            ],
        )
        #  export to file
        df.sort_values(
            by=["Election", "Contest", "Selection", "ReportingUnit", "VoteType"],
            inplace=True,
        )
        df.to_csv(target_file, sep="\t", index=False)
        return

    def diff_in_diff_dem_vs_rep(
        self,
        election: str,
    ) -> (pd.DataFrame, list):
        """
        Not ready for prime time
        for each jurisdiction in the election that has more than just 'total',
        Calculate all possible diff-in-diff values per Herron
        http://doi.org/10.1089/elj.2019.0544.
        Return df with columns election, overall jurisdiction, county-type jurisdiction,
        election district type, contest-pair, vote-type pair, diff-in-diff value
        as well as minimum # of votes over both contests (to help eliminate
        unenlightening variations from small contests)"""

        party_list = ["Democratic Party", "Republican Party"]
        missing = list()  # track items missing info for diff-in-diff calculation
        msgs = (
            set()
        )  # track expected but missing counts (by vote type or contest over all county)
        rows = list()  # collects rows for output dataframe
        cols = [
            "state",
            constants.default_subdivision_type,
            "district_type",
            "party",
            "contest_pair",
            "min_count_by_contest",
            "vote_type_pair",
            "abs_diff_in_diff",
        ]
        election_id = db.name_to_id(self.session, "Election", election)

        # loop through state-like jurisdictions with multiple vote types
        vts = db.vote_types_by_juris(self.session, election)

        contests_df = db.contest_families_by_juris(
            self.session,
            election,
        )
        # loop through states, etc.
        for jurisdiction in vts.keys():
            state_rows = list()
            # get vote-types, contest roll-up by vote type and contest-by-election-district types
            data_file_list, _ = db.data_file_list(self.session, election_id)
            vote_types = {x for x in vts[jurisdiction] if x != "total"}
            district_types = contests_df[contests_df["jurisdiction"] == jurisdiction]

            # get dataframe of results, adding column for political party
            res, _ = db.export_rollup_from_db(
                self.session,
                jurisdiction,
                election,
                self.major_subdivision_type[jurisdiction],
                "Candidate",
                data_file_list,
                exclude_redundant_total=True,
                by_vote_type=True,
                include_party_column=True,
            )
            # loop through counties
            for county in res.reporting_unit.unique():
                # get dictionary of vote counts by contest
                county_id = db.name_to_id(self.session, "ReportingUnit", county)
                vc_by_contest = self.vote_count_by_element(
                    "Contest", election_id, county_id
                )
                vc_by_vote_type = self.vote_count_by_element(
                    "CountItemType", election_id, county_id
                )

                # loop through contest district types (congressional, state-house, statewide)
                for cdt in district_types.ReportingUnitType.unique():
                    # find set of "good" contests of that type in that county --
                    # contests with candidates from all parties on party_list
                    good_dt_contests = contests_df[
                        (contests_df.jurisdiction == jurisdiction)
                        & (contests_df.ReportingUnitType == cdt)
                    ]["contest"].unique()
                    good_dt_contests_results = res[
                        (res.reporting_unit == county)
                        & (res.contest.isin(good_dt_contests))
                    ][["contest", "selection", "party", "count_item_type", "count"]]
                    good_contest_list = [
                        c
                        for c in good_dt_contests_results.contest.unique()
                        if all(
                            [
                                p in res[res.contest == c]["party"].unique()
                                for p in party_list
                            ]
                        )
                    ]

                    if len(good_contest_list) > 1:
                        # create dataframe with convenient index for calculations below
                        # note: sum is necessary because, e.g., may have two selections of same party
                        ww = (
                            good_dt_contests_results[
                                ["contest", "party", "count_item_type", "count"]
                            ]
                            .groupby(["contest", "party", "count_item_type"])
                            .sum("count")
                        )
                        ww["vote_type_total"] = ww.groupby(
                            ["contest", "count_item_type"]
                        ).transform("sum")
                        ww["pct_of_vote_type"] = ww["count"] / ww["vote_type_total"]
                        # sort index to help performance
                        ww.sort_index(inplace=True)

                        # calculate contest-to-contest vote shares and votetype-to-votetype vote shares
                        # that will be used to exclude comparisons involving small

                        # loop through vote-type pairs
                        for vt_pair in itertools.combinations(vote_types, 2):
                            try:
                                min_count_by_vote_type = min(
                                    vc_by_vote_type[vt_pair[0]],
                                    vc_by_vote_type[vt_pair[1]],
                                )
                            except KeyError as ke:
                                msgs.update(
                                    {
                                        f"No results of vote type {ke} available in {county}"
                                    }
                                )
                                continue
                                # TODO better error handling?
                            # loop through contest-pairs in county
                            for con_pair in itertools.combinations(
                                good_contest_list, 2
                            ):
                                try:
                                    min_count_by_contest = min(
                                        vc_by_contest[con_pair[0]],
                                        vc_by_contest[con_pair[0]],
                                    )
                                except KeyError as ke:
                                    msgs.update(
                                        {
                                            f"No results for contest {ke} available in {county}"
                                        }
                                    )
                                    continue
                                    # TODO better error handling?

                                for party in party_list:
                                    ok = True
                                    pct = dict()
                                    for i in (0, 1):
                                        pct[i] = dict()
                                        for j in (0, 1):
                                            try:
                                                pct[i][j] = ww.loc[
                                                    (con_pair[i], party, vt_pair[j]),
                                                    "pct_of_vote_type",
                                                ]
                                                if pd.isnull(pct[i][j]) or (
                                                    not isinstance(pct[i][j], float)
                                                ):
                                                    ok = False
                                                    missing.append(
                                                        [
                                                            county,
                                                            con_pair[i],
                                                            party,
                                                            vt_pair[j],
                                                            "non-numeric",
                                                        ]
                                                    )
                                            except KeyError as ke:
                                                ok = False
                                                missing.append(
                                                    [
                                                        county,
                                                        con_pair[i],
                                                        party,
                                                        vt_pair[j],
                                                        ke,
                                                    ]
                                                )
                                    if ok:
                                        # append diff-in-diff row
                                        did = abs(
                                            abs(pct[0][0] - pct[1][0])
                                            - abs(pct[0][1] - pct[1][1])
                                        )
                                        state_rows.append(
                                            [
                                                jurisdiction,
                                                county,
                                                cdt,
                                                party,
                                                con_pair,
                                                min_count_by_contest,
                                                vt_pair,
                                                did,
                                            ]
                                        )
            rows += state_rows
            state_with_hyphens = jm.system_name_from_true_name(jurisdiction)
            pd.DataFrame(state_rows, columns=cols).to_csv(
                f"{state_with_hyphens}_state_export.csv", index=False
            )

        diff_in_diff = pd.DataFrame(rows, columns=cols)
        for msg in sorted(list(msgs)):
            print(msg)
        return diff_in_diff, missing

    def vote_share_comparison(
        self,
        element: str,
        election_id: int,
        reportingunit_id: int,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Not ready for prime time
        given an election, a reporting unit -- not necessarily a whole jurisdiction--
        and an element for pairing (e.g., "Contest"), return a dictionary mapping pairs of elements
        to pairs of vote shares (summing over everything else)"""
        vote_share = dict()
        vc = self.vote_count_by_element(element, election_id, reportingunit_id)
        for item in vc.keys():
            vote_share[item] = dict()
        for (x, y) in itertools.combinations(vc.keys(), 2):
            vote_total = vc[x] + vc[y]
            vote_share[x][y] = (vc[x] / vote_total, vc[y] / vote_total)
            vote_share[y][x] = (vc[y] / vote_total, vc[x] / vote_total)
        return vote_share

    def vote_count_by_element(
        self,
        element: str,
        election_id: int,
        jurisdiction_id: int,
    ) -> dict:
        """Not ready for prime time
        Returns dictionary of vote counts by element (summing over everything else
        within the given election and reporting unit)"""
        if element == "CountItemType":
            name_field = "CountItemType"
            fields = aliases = ["CountItemType", "Count"]
        elif element in ["Contest", "Party", "Office", "ReportingUnit", "Election"]:
            name_field = f"{element}Name"
            fields = aliases = [name_field, "CountItemType", "Count"]
        else:  # TODO tech debt there are more field possibilities in read_vote_count
            name_field = element
            fields = aliases = [name_field, "CountItemType", "Count"]
        vc_df = db.read_vote_count(
            self.session,
            election_id=election_id,
            jurisdiction_id=jurisdiction_id,
            fields=fields,
            aliases=aliases,
        )
        # exclude any redundant total vote types
        if len(vc_df.CountItemType.unique()) > 1:
            vc_df = vc_df[vc_df.CountItemType != "total"]
        vc_dict = vc_df.groupby(name_field).sum().to_dict()["Count"]
        return vc_dict

    def aggregate(
        self,
        election: str,
        jurisdiction: str,
        vote_type: Optional[str] = None,
        sub_unit: Optional[str] = None,
        contest: Optional[str] = None,
        contest_type: str = "Candidate",
        sub_unit_type: str = constants.default_subdivision_type,
        exclude_redundant_total: bool = True,
    ) -> pd.DataFrame:
        """Returns a dataframe of contest totals.
        if a vote type is given, restricts to that vote type; otherwise returns all vote types;
        Similarly for sub_unit and contest"""
        # using the analyzer gives us access to DB session
        empty_df_with_good_cols = pd.DataFrame(columns=["contest", "count"])
        election_id = db.name_to_id(self.session, "Election", election)
        jurisdiction_id = db.name_to_id(self.session, "ReportingUnit", jurisdiction)
        if not election_id:
            return empty_df_with_good_cols
        connection = self.session.bind.raw_connection()
        cursor = connection.cursor()

        datafile_list, err_str1 = db.data_file_list_cursor(
            cursor,
            election_id,
            reporting_unit_id=jurisdiction_id,
            by="Id",
        )
        if err_str1:
            print(err_str1)
            return empty_df_with_good_cols
        if len(datafile_list) == 0:
            print(
                f"No datafiles found for election {election} and jurisdiction {jurisdiction}"
                f"(election_id={election_id} and jurisdiction_id={jurisdiction_id})"
            )
            return empty_df_with_good_cols

        df, err_str = db.export_rollup_from_db(
            # cursor=cursor,
            session=self.session,
            top_ru=jurisdiction,
            election=election,
            sub_unit_type=sub_unit_type,
            contest_type=contest_type,
            datafile_list=datafile_list,
            by="Id",
            exclude_redundant_total=exclude_redundant_total,
            by_vote_type=True,
            contest=contest,
        )
        if err_str or df.empty:
            return empty_df_with_good_cols
        if vote_type:
            df = df[df.count_item_type == vote_type]
        if sub_unit:
            df = df[df.reporting_unit == sub_unit]
        return df

    def pres_counts_by_vote_type_and_major_subdiv(
        self, jurisdiction: str
    ) -> pd.DataFrame:
        """Not ready for prime time"""
        # TODO return dataframe with columns jurisdiction, subdivision, year, CountItemType,
        #  total votes for pres in general election
        group_cols = [
            "reporting_unit",
            "count_item_type",
        ]
        p_year = re.compile(r"\d{4}")
        elections = list(
            pd.read_sql_table("Election", self.session.bind)["Name"].unique()
        )
        df_pres = pd.DataFrame()
        pres_gen_elections = [
            el
            for el in elections
            if el.endswith(" General") and p_year.match(el[:4]) and int(el[:4]) % 4 == 0
        ]
        for el in pres_gen_elections:
            df = self.aggregate(
                el,
                jurisdiction,
                contest=f"US President ({constants.abbr[jurisdiction]})",
                sub_unit_type=self.major_subdivision_type[jurisdiction],
                exclude_redundant_total=False,
            )
            missing = [c for c in (group_cols + ["count"]) if c not in df.columns]
            if not missing:
                df = df[group_cols + ["count"]].groupby(group_cols).sum().reset_index()
                df = m.add_constant_column(df, "Election", el)
                df_pres = pd.concat([df_pres, df])
        df_pres = m.add_constant_column(df_pres, "Jurisdiction", jurisdiction).rename(
            columns={"count": "votes_for_president"}
        )
        return df_pres

    def pres_counts_by_vote_type_and_major_subdiv_all(self) -> pd.DataFrame:
        """Not ready for prime time"""
        all_df = pd.DataFrame()
        for jurisdiction in constants.abbr.keys():
            df = self.pres_counts_by_vote_type_and_major_subdiv(jurisdiction)
            df = m.add_constant_column(df, "abbr", constants.abbr[jurisdiction])
            all_df = pd.concat([all_df, df])

        return all_df

    def compare_to_results_file(
        self,
        reference: str,
        jurisdictions: Optional[Iterable[str]] = None,
        elections: Optional[Iterable[str]] = None,
        single_election: Optional[str] = None,
        single_jurisdiction: Optional[str] = None,
        report_dir: Optional[str] = None,
        significance: Optional[float] = None,
        status: Optional[str] = None,
    ) -> (
        pd.DataFrame,
        pd.DataFrame,
        pd.DataFrame,
        Optional[pd.DataFrame],
        str,
        Optional[dict],
    ):
        """session: Session, db session
        reference: str, path to tab-separated file with reference results
        report_dir: Optional[str] = None, path to directory for reports
        jurisdictions: Optional[Iterable[str]] = None, if given, restrict to jurisdictions in iterable
        elections: Optional[Iterable[str]] = None, if given, restrict to elections in iterable
        single_election: Optional[str], if given, all results assumed to be for this election;
            if not given, reference file must have Election column
        single_jurisdiction: Optional[str] = None, if given, all results assumed to be for this jurisdiction;
            if not given, reference file must have Jurisdiction column
        report_dir: Optional[str] = None, reports will be exported to this directory (if given)
        significance: Optional[float] = None, cut-off value for "significantly" wrong counts
        status: Optional[str] = None, if specified, restrict results to those with
            specified status in Status column of reference file

        if report_dir given, exports results to that directory

        Returns:
        pd.DataFrame, reference results not found
        pd.DataFrame, reference results matched in db
        pd.DataFrame, reference results found but not matched in db, with reference and db totals
        Optional[pd.DataFrame], found-but-unmatched results that are off by more than the <significance>
        str, subdirectory to which reports were written
        Optional[dict], error dictionary
        """
        err = None
        sub_dir = "No reports created"
        ref = pd.read_csv(reference, sep="\t")

        # restrict to single jurisdiction (if specified) or else to jurisdictions in list (if given)
        #  and ensure there is a Jurisdiction column
        if "Jurisdiction" in ref.columns:
            if single_jurisdiction:
                ref = ref[ref.Jurisdiction == single_jurisdiction]
            elif jurisdictions:
                ref = ref[ref.Jurisdiction.isin(jurisdictions)]
        else:
            if single_jurisdiction:
                ref = m.add_constant_column(ref, "Jurisdiction", single_jurisdiction)
            else:
                err = ui.add_new_error(
                    err,
                    "warn-test",
                    Path(reference).name,
                    "No Jurisdiction column, and no single jurisdiction specified",
                )

        # restrict to single election (if specified) or else to elections in list (if given)
        #  and ensure there is a Election column
        if "Election" in ref.columns:
            if single_election:
                ref = ref[ref.Election == single_election]
            elif elections:
                ref = ref[ref.Election.isin(elections)]
        else:
            if single_election:
                ref = m.add_constant_column(ref, "Election", single_election)
            else:
                err = ui.add_new_error(
                    err,
                    "warn-test",
                    Path(reference).name,
                    "No Election column, and no single election specified",
                )

        # restrict to specific status if given
        if status:
            if "Status" in ref.columns:
                ref = ref[ref.Status == status]
            else:
                err = ui.add_new_error(
                    err,
                    "warn-test",
                    Path(reference).name,
                    f"Status {status} was specified, but there is no 'Status' column",
                )
        if ref.empty:
            err = ui.add_new_error(
                err,
                "warn-test",
                Path(reference).name,
                f"No relevant results found in reference results file.\n",
            )
        if err:
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), None, sub_dir, err

        # if no VoteType column, use "total"; otherwise replace any null "VoteType" entries by "total"
        if not "VoteType" in ref.columns:
            ref = m.add_constant_column(ref, "VoteType", "total")
        else:
            ref["VoteType"].fillna("total", inplace=True)

        # change Count column to integer
        ref, clean_err_df = m.clean_count_cols(ref, ["Count"])
        if not clean_err_df.empty:
            err = ui.add_new_error(
                err,
                "warn-test",
                Path(reference).name,
                f"Some counts could not be interpreted as integers:\n{clean_err_df}",
            )

        # initialize dataframes to be returned
        not_found_in_db = ok = pd.DataFrame(columns=ref.columns)
        wrong = pd.DataFrame(columns=list(ref.columns) + ["Database_Count"])

        for idx, row in ref.iterrows():
            db_total = self.contest_total(
                election=row["Election"],
                jurisdiction=row["Jurisdiction"],
                contest=row["Contest"],
                reporting_unit=row["ReportingUnit"],
                vote_type=row["VoteType"],
            )

            if db_total is None:
                # contest not found
                not_found_in_db = not_found_in_db.append(row, ignore_index=True)
            elif int(db_total) == int(row["Count"]):
                ok = ok.append(row, ignore_index=True)
            else:
                wrong = wrong.append(
                    {**row, **{"Database_Count": int(db_total)}}, ignore_index=True
                )

        # rename Count to Reference Count and revise column order
        wrong.rename(columns={"Count": "Reference_Count"}, inplace=True)
        first = ["Database_Count", "Reference_Count"]
        new_order = first + [c for c in wrong.columns if c not in first]
        wrong = wrong[new_order]

        # create report of only significantly wrong values
        if significance:
            mask = (
                abs(
                    (wrong.Database_Count - wrong.Reference_Count)
                    / wrong.Reference_Count
                )
                > significance
            )
            significantly_wrong = wrong[mask]
        else:
            significantly_wrong = None

        # report
        if report_dir:
            ts = datetime.datetime.now().strftime("%m%d_%H%M")
            sub_dir = os.path.join(
                report_dir, f"compare_to_{Path(reference).stem}_{ts}"
            )
            Path(sub_dir).mkdir(exist_ok=True, parents=True)
            names = ["not_found_in_db", "ok", "wrong"]
            for df_name in names:
                eval(df_name).to_csv(
                    os.path.join(sub_dir, f"{df_name}.tsv"), sep="\t", index=None
                )
            if (
                significantly_wrong is not None
            ):  # nb: if nothing is significantly wrong, df will be empty but not None.
                significantly_wrong.to_csv(
                    os.path.join(sub_dir, f"wrong_by_at_least_{significance}.tsv"),
                    sep="\t",
                    index=None,
                )

            # report parameters
            # 'database' variable used even though syntax-checker doesn't see it
            database = self.session.bind.url
            report_str = f"database: {database}\nreference file: {reference}"
            optional_params = [
                "single_election",
                "single_jurisdiction",
                "elections",
                "jurisdictions",
                "status",
            ]
            for op in optional_params:
                if eval(op):
                    report_str = f"{report_str}\n{op}: {eval(op)}"
            open(os.path.join(sub_dir, "_parameters.txt"), "w").write(report_str)

            if not wrong.empty:
                wrong_str = (
                    f"\nSome database contest results did not match reference results. "
                    f"For details see {sub_dir}"
                )
                err = ui.add_new_error(
                    err,
                    "warn-test",
                    Path(reference).name,
                    wrong_str,
                )
            if not not_found_in_db.empty:
                nfid_str = (
                    f"\nSome expected constests not found. For details, see {sub_dir}"
                )
                err = ui.add_new_error(
                    err,
                    "warn-test",
                    Path(reference).name,
                    nfid_str,
                )
        # if no report_dir, but some results are wrong
        else:
            if not wrong.empty:
                wrong_str = f"\nSome database contest results did not match reference results.\n{wrong.to_string()}"
                err = ui.add_new_error(
                    err,
                    "warn-test",
                    Path(reference).name,
                    wrong_str,
                )
            if not not_found_in_db.empty:
                nfid_str = f"\nSome expected contests not found.\n{not_found_in_db.to_string()}"
                err = ui.add_new_error(
                    err,
                    "warn-test",
                    Path(reference).name,
                    nfid_str,
                )
        return not_found_in_db, ok, wrong, significantly_wrong, sub_dir, err


def aggregate_results(
    election: str,
    jurisdiction: str,
    dbname: Optional[str] = None,
    param_file: Optional[str] = None,
    vote_type: Optional[str] = None,
    sub_unit: Optional[str] = None,
    contest: Optional[str] = None,
    contest_type: str = "Candidate",
    sub_unit_type: str = constants.default_subdivision_type,
    exclude_redundant_total: bool = True,
) -> pd.DataFrame:
    """if a vote type is given, restricts to that vote type; otherwise returns all vote types;
    Similarly for sub_unit and contest"""
    # using the analyzer gives us access to DB session
    empty_df_with_good_cols = pd.DataFrame(columns=["contest", "count"])
    an = Analyzer(dbname=dbname, param_file=param_file)
    if not an:
        return empty_df_with_good_cols
    else:
        return an.aggregate(
            election,
            jurisdiction,
            vote_type=vote_type,
            sub_unit=sub_unit,
            contest=contest,
            contest_type=contest_type,
            sub_unit_type=sub_unit_type,
            exclude_redundant_total=exclude_redundant_total,
        )


def data_exists(
    election: str,
    jurisdiction: str,
    param_file: Optional[str] = None,
    dbname: Optional[str] = None,
) -> bool:
    """
    Required inputs:
        election: str,
        jurisdiction: str,
    Optional inputs:
        param_file: Optional[str] = None,
        dbname: Optional[str] = None,

    Returns:
        bool, True if database specified by parameters in <param_file> (or database named <dbname>, if given) has
            any election results data for the given <election> and <jurisdiction>. Otherwise false.
    """
    analyzer = Analyzer(param_file=param_file, dbname=dbname)
    return analyzer.data_exists(election, jurisdiction)


def external_data_exists(
    election: str,
    jurisdiction: str,
    param_file: Optional[str] = None,
    dbname: Optional[str] = None,
) -> bool:
    """
    Required inputs:
        election: str,
        jurisdiction: str,
    Optional inputs:
        param_file: Optional[str] = None,
        dbname: Optional[str] = None,

    Returns:
        bool, True if database specified by parameters in <param_file> (or database named <dbname>, if given) has
            any external dataset content for the given election and jurisdiction. Otherwise false.
    """
    an = Analyzer(param_file=param_file, dbname=dbname)
    if not an:
        return False

    jurisdiction_id = db.name_to_id(an.session, "ReportingUnit", jurisdiction)
    election_id = db.name_to_id(an.session, "Election", election)

    # if the database doesn't have both the jurisdiction and the election
    if (not jurisdiction_id) or (not election_id):
        # data doesn't exist
        return False

    connection = an.session.bind.raw_connection()
    cursor = connection.cursor()
    df = db.read_external(cursor, election_id, jurisdiction_id, ["Label"])
    cursor.close()

    # if no data found
    if df.empty:
        # no data exists.
        return False
    # otherwise
    else:
        # then data exists!
        return True


def load_results_df(
    session: Session,
    df: pd.DataFrame,
    necessary_constants: dict,
    juris_true_name: str,
    file_name: str,
    munger_name: str,
    path_to_jurisdiction_dir: str,
    datafile_id: int,
    election_id: int,
    rollup: bool = False,
    rollup_rut: str = constants.default_subdivision_type,
    alt_dictionary: Optional[str] = None,
) -> Optional[dict]:
    """
    Required inputs:
        session: Session,
        df: pd.DataFrame, dataframe with columns 'Count', 'Candidate_raw', 'Party_raw', etc.
        necessary_constants: dict, dictionary of constant values (e.g., if all rows of <df> are for a single Contest)
        juris_true_name: str, for error reporting
        file_name: str, for error reporting
        munger_name: str, for error reporting
        path_to_jurisdiction_dir: str, path to directory
        datafile_id: int,
        election_id: int,
    Optional inputs:
        rollup: bool = False,
        rollup_rut: str = constants.default_subdivision_type,
        alt_dictionary: Optional[str] = None,  path to file

    Munges vote counts in dataframe into the <session>'s database, using the dictionary.txt file in the
        <path_to_jurisdiction_dir> directory or, if given, the file specified by <alt_dictionary>. If
        <rollup> then results are rolled up to the ReportingUnitType <rollup_rut> if given; the default is
         <constants.default_subdivision_type>.

    Returns:
         Optional[dict], error dictionary
    """
    err = None
    working = df.copy()
    # add text column for internal CountItemType name, Id columns for all but Count, removing raw-munged
    try:
        working, new_err = m.munge_raw_to_ids(
            working,
            necessary_constants,
            path_to_jurisdiction_dir,
            file_name,
            munger_name,
            session,
            alternate_dictionary=alt_dictionary,
        )
        if new_err:
            err = ui.consolidate_errors([err, new_err])
            if ui.fatal_error(new_err):
                return err
    except Exception as exc:
        err = ui.add_new_error(
            err,
            "system",
            f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
            f"Exception while munging raw to ids: {exc}",
        )
        return err

    # # for each contest, if none or unknown candidate has total votes 0, remove rows with that contest & candidate
    nou_candidate_id = db.name_to_id(session, "Candidate", "none or unknown")
    nou_selection_ids = db.selection_ids_from_candidate_id(session, nou_candidate_id)
    unknown = (
        working[working.Selection_Id.isin(nou_selection_ids)]
        .groupby(["Contest_Id", "Selection_Id"])
        .sum()
    )
    for (contest_id, selection_id) in unknown.index:
        mask = working[["Contest_Id", "Selection_Id"]] == (contest_id, selection_id)
        working = working[~mask.all(axis=1)]

    if working.empty:
        err = ui.add_new_error(
            err,
            "jurisdiction",
            juris_true_name,
            f"No contest-selection pairs recognized via munger {munger_name}",
        )
        return err

    # rollup_dataframe results if requested
    if rollup:
        working, new_err = an.rollup_dataframe(
            session,
            working,
            "Count",
            "ReportingUnit_Id",
            "ReportingUnit_Id",
            rollup_rut,
        )
        if new_err:
            err = ui.consolidate_errors([err, new_err])
            if ui.fatal_error(new_err):
                return err

    # add_datafile_Id and Election_Id columns
    working = m.add_constant_column(working, "_datafile_Id", datafile_id)
    working = m.add_constant_column(working, "Election_Id", election_id)
    # load counts to db
    try:
        err = m.fill_vote_count(working, session, munger_name, err)
    except Exception as exc:
        err = ui.add_new_error(
            err,
            "system",
            f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
            f"Exception while filling vote count table: {exc}",
        )
        return err
    return err


def load_results_file(
    session: Session,
    munger_path: str,
    f_path: str,
    juris_true_name: str,
    datafile_id: int,
    election_id: int,
    constants: Dict[str, str],
    results_directory_path: str,
    path_to_jurisdiction_dir: str,
    rollup: bool = False,
    rollup_rut: str = constants.default_subdivision_type,
) -> Optional[dict]:
    """
    required inputs:
        session: Session, database session
        munger_path: str, path to file with munging parameters
        f_path: str, path to results file
        juris_true_name: str, name of jurisdiction (without hyphens, e.g., "American Samoa")
        datafile_id: int, Id of the file in the _datafile table in the db
        election_id: int, Id of the election in the Election table in the db
        constants: Dict[str, str], values of any elements (e.g., CountItemType) constant over the results file
        results_directory_path: str, path to root directory for results files (relative to which file paths
            are specified in the result file's .ini file)
        path_to_jurisdiction_dir: str, path to directory whose subdirectories, named for jurisdictions, contain the
            jurisdiction information and dictionary necessary for munging
    optional inputs:
        rollup: bool = False, if True, roll up results to the subdivisions specified by <rollup_rut>
        rollup_rut: str = constants.default_subdivision_type, ReportingUnitType used for rollup (typically 'county')

    Attempts to load results from results file to the database. (Does *not* require results to pass tests.)

    returns:
        Optional[dict], error dictionary
    """
    # TODO tech debt: redundant to pass results_directory_path and f_path
    munger_name = Path(munger_path).name
    file_name = Path(f_path).name
    # read parameters from munger file
    p, err = m.get_and_check_munger_params(munger_path)
    if ui.fatal_error(err):
        return err

    # transform to raw df in standard form
    df, new_err = m.file_to_raw_df(munger_path, p, f_path, results_directory_path)
    if new_err:
        err = ui.consolidate_errors([err, new_err])
        if ui.fatal_error(new_err):
            return err

    # # add columns for constant-over-file elements
    if p["constant_over_file"]:
        necessary_constants = {
            c: v for c, v in constants.items() if c in p["constant_over_file"]
        }
        df = m.add_constants_to_df(df, necessary_constants)
    else:
        necessary_constants = dict()

    # # delete any rows with items to be ignored
    df = m.remove_ignored_rows(df, munger_path)

    new_err = load_results_df(
        session,
        df,
        necessary_constants,
        juris_true_name,
        file_name,
        munger_name,
        path_to_jurisdiction_dir,
        datafile_id,
        election_id,
        rollup=rollup,
        rollup_rut=rollup_rut,
    )
    return ui.consolidate_errors([err, new_err])


def create_from_template(
    template_file: str, target_file: str, replace_dict: Dict[str, str]
):
    with open(template_file, "r") as f:
        contents = f.read()
    for k in replace_dict.keys():
        contents = contents.replace(k, replace_dict[k])
    with open(target_file, "w") as f:
        f.write(contents)


def load_or_reload_all(
    rollup: bool = False,
    dbname: Optional[str] = None,
    param_file: Optional[str] = None,
    move_files: bool = True,
    run_tests: bool = True,
    suppress_warnings: bool = False,
) -> Optional[dict]:
    """
    required inputs: (none)
    optional inputs:
        rollup: bool = False, if True, roll up results to major subdivision before storing in database
        dbname: Optional[str] = None, name of database
        param_file: Optional[str] = None, path to file with parameters for data loading
        move_files: bool = True, if True, archive files after successful load (& test, if done)
        run_tests: bool = True, if True, run tests on results in database after loading.
        suppress_warnings: bool = False, if True, do not create warnings-only files in reports_and_plots_dir

    For each election-jurisdiction pair from a <results>.ini file corresponding to a file in
        the results directory specified in the data loading parameter file, loads all result data to
        a temporary database (and, if <run_tests> is True, runs tests on loaded data). If the
        set of files in the election-jurisdiction pair all load without fatal error or failed test,
        remove any data for that election-jurisdiction pair from the database and load the new data.

    returns:
        Optional[dict], error dictionary
    """
    err = None
    dataloader = DataLoader(dbname=dbname, param_file=param_file)
    if dataloader:
        ts = datetime.datetime.now().strftime("%m%d_%H%M")
        error_and_warning_dir = os.path.join(
            dataloader.d["reports_and_plots_dir"], f"load_or_reload_all_{ts}"
        )
        # get relevant election-jurisdiction pairs
        ej_pairs = ui.election_juris_list(
            dataloader.d["ini_dir"], results_path=dataloader.d["results_dir"]
        )
        if ej_pairs:
            ej_pairs.sort()
            # process each election-jurisdiction pair
            for (election, jurisdiction) in ej_pairs:
                # if new results pass test, remove old if exists and load new
                new_err = reload_juris_election(
                    jurisdiction,
                    election,
                    error_and_warning_dir,
                    rollup=rollup,
                    dbname=dbname,
                    param_file=param_file,
                    move_files=move_files,
                    run_tests=run_tests,
                )
                if new_err:
                    err = ui.consolidate_errors([err, new_err])
        else:
            err = ui.add_new_error(
                err,
                "file",
                "All",
                "No results had corresponding file in ini_files_for_results",
            )
        # report errors to file
        err = ui.report(
            err,
            error_and_warning_dir,
            file_prefix="loading_",
            suppress_warnings=suppress_warnings,
        )
    return err


def test_and_load_multifile(
    multi_file: str,
    test_dir: Optional[str],
    dbname: Optional[str] = None,
    param_file: Optional[str] = None,
) -> Optional[dict]:
    err = None
    # TODO
    return err


def reload_juris_election(
    juris_name: str,
    election_name: str,
    report_dir,
    rollup: bool = False,
    dbname: Optional[str] = None,
    param_file: Optional[str] = None,
    move_files: bool = True,
    run_tests: bool = True,
    suppress_warnings: bool = False,
) -> Optional[dict]:
    """
    required inputs:
        juris_name: str, name of jurisdiction (without hyphens, e.g., 'District of Columbia')
        election_name: str, name of election (without hyphens, e.g., '2020 General')
        report_dir, path to directory for reporting errors, warnings and test results
    optional inputs:
        rollup: bool = False, if true, rolls up results within the to the major subdivision
        dbname: Optional[str] = None, name of database if given; otherwise database name taken from parameter file
        param_file: Optional[str] = None, path to parameter file for dataloading if given; otherwise parameter file
            path assumed to be 'run_time.ini'
        move_files: bool = True, if true, move all files to archive directory if loading (& testing, if done)
            are successful
        run_tests: bool = True, if true, run tests on loaded data
        suppress_warnings: bool = False, if true, report only errors, not warnings

    Loads and archives each results file in each direct subfolder of the results_dir
    named in ./run_time.ini -- provided there the results file is specified in a *.ini file in the
    corresponding subfolder of <content_root>/ini_files_for_results. <contest_root> is read from ./run_time.ini.

    returns:
        Optional[dict], error dictionary
    """
    # initialize dataloader
    err = None
    dl = DataLoader(dbname=dbname, param_file=param_file)

    if run_tests:
        # create temp_db (preserving live db name) and point dataloader to it
        live_db = dl.session.bind.url.database
        ts = datetime.datetime.now().strftime("%m%d_%H%M")
        temp_db = f"{live_db}_test_{ts}"

        # load all data into temp db
        new_err = dl.change_db(temp_db, db_param_file=param_file)
        err = ui.consolidate_errors([err, new_err])
        if ui.fatal_error(new_err):
            return err
        success, failure, all_tests_passed, load_err = dl.load_all(
            report_dir=report_dir,
            move_files=False,
            rollup=rollup,
            election_jurisdiction_list=[(election_name, juris_name)],
            suppress_warnings=suppress_warnings,
        )
        if load_err:
            err = ui.consolidate_errors([err, load_err])
        # if any of the data failed to load
        if failure or ui.fatal_error(load_err):
            # cleanup temp database
            db.remove_database(db_param_file=param_file, dbname=temp_db)
            return err

        # if any tests failed
        elif not all_tests_passed[f"{election_name};{juris_name}"]:
            print(
                f"{juris_name} {election_name}: No old data removed and no new data loaded because of failed tests."
            )
        else:
            # switch to live db
            new_err = dl.change_db(live_db, db_param_file=param_file)
            err = ui.consolidate_errors([err, new_err])
            if ui.fatal_error(new_err):
                return err
    else:
        temp_db = None  # for syntax-checker
        all_tests_passed = dict()
    if (not run_tests) or (
        (f"{election_name};{juris_name}" in all_tests_passed.keys())
        and (all_tests_passed[f"{election_name};{juris_name}"])
    ):
        # Remove existing data for juris-election pair from live db
        election_id = db.name_to_id(dl.session, "Election", election_name)
        juris_id = db.name_to_id(dl.session, "ReportingUnit", juris_name)
        if election_id and juris_id:
            err_str = dl.remove_data(election_id, juris_id)
            if err_str:
                err = ui.add_new_error(
                    err,
                    "warn-database",
                    f"{dl.session.bind.url.database}",
                    f"Error removing data: {err_str}",
                )

        # Load new data into live db (and move successful to archive)
        success, failure, all_tests_passed, new_err = dl.load_all(
            report_dir=report_dir,
            rollup=rollup,
            election_jurisdiction_list=[(election_name, juris_name)],
            move_files=move_files,
            run_tests=run_tests,
            suppress_warnings=suppress_warnings,
        )
        if success:
            if not all_tests_passed[f"{election_name};{juris_name}"]:
                # this should never happen, since upload was just tested. Still...
                err = ui.add_new_error(
                    err,
                    "database",
                    f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
                    f"Data loaded successfully to test db and passed tests, so old data was removed from live db.\n"
                    f"But new data loaded to live db failed some tests.",
                )
        else:
            err = ui.consolidate_errors([err, new_err])
    if run_tests:
        # cleanup temp database
        db.remove_database(db_param_file=param_file, dbname=temp_db)
    return err


def datafile_info(
    connection,
    ini_filename: str,
    results_short_name: str,
    file_name: str,
    download_date: str,
    source: str,
    note: str,
    jurisdiction_id: int,
    election_id: int,
    is_preliminary: bool,
) -> (int, Optional[dict]):
    """Inserts record into _datafile table. Returns id of datafile record"""
    err = None
    data = pd.DataFrame(
        [
            [
                results_short_name,
                file_name,
                download_date,
                source,
                note,
                jurisdiction_id,
                election_id,
                datetime.datetime.now(),
                is_preliminary,
            ]
        ],
        columns=[
            "short_name",
            "file_name",
            "download_date",
            "source",
            "note",
            "ReportingUnit_Id",
            "Election_Id",
            "created_at",
            "is_preliminary",
        ],
    )
    data = m.clean_strings(data, ["short_name"])
    try:
        err = db.insert_to_cdf_db(
            connection,
            data,
            "_datafile",
            "ini",
            ini_filename,
        )
        if ui.fatal_error(err):
            return 0, err
        else:
            # TODO tech debt not sure why we need to rename here but not elsewhere, has to do with col_map
            data.rename(columns={"short_name": "_datafile"}, inplace=True)
            datafile_id = db.append_id_to_dframe(connection, data, "_datafile",).iloc[
                0
            ]["_datafile_Id"]
    except Exception as exc:
        err = ui.add_new_error(
            err,
            "system",
            f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
            f"Error inserting record to _datafile table or retrieving _datafile_Id: {exc}",
        )
        return 0, err

    return datafile_id, err


def bad_multi_presidentials(
    dbname: str, param_file: str, compare_file: str
) -> (List[str], List[str]):
    """Compares presidential results for elections and jurisdictions listed
    in <compare_file>. Assumes <compare_file> has columns
    Election, Jurisdiction, Total Presidential"""

    results = pd.read_csv(compare_file, sep="\t")
    bad_list: List[str] = list()
    good_list: List[str] = list()
    an = Analyzer(dbname=dbname, param_file=param_file)
    for idx, r in results.iterrows():
        election_id = db.name_to_id(an.session, "Election", r["Election"])
        jurisdiction_id = db.name_to_id(an.session, "ReportingUnit", r["Jurisdiction"])
        db_counts = db.read_vote_count(
            an.session,
            election_id=election_id,
            jurisdiction_id=jurisdiction_id,
            fields=["ContestName", "Count"],
            aliases=["ContestName", "Count"],
        )
        if db_counts[db_counts.ContestName.str.contains("US President")].empty:
            bad_list.append(f"{r['Election']} {r['Jurisdiction']}: no results found")
        else:
            total = db_counts[db_counts.ContestName.str.contains("US President")].sum()[
                "Count"
            ]
            if total != r["Total_Presidential"]:
                bad_list.append(
                    f"{r['Election']} {r['Jurisdiction']}:\n\t{total}\tfrom db \n\t{r['Total_Presidential']}\t from compare file"
                )
            else:
                good_list.append(f"{r['Election']} {r['Jurisdiction']}: {total}")
    return good_list, bad_list


def export_notes_from_ini_files(
    directory: str,
    target_file: str,
    election: Optional[str] = None,
    jurisdiction: Optional[str] = None,
):
    """
    Required inputs:
        directory: str, path to directory
        target_file: str, path to file

    Optional inputs:
        election: Optional[str] = None,
        jurisdiction: Optional[str] = None,

    Creates <target_file> containing a summary of all results_notes parameter values from the .ini
        files in <directory> or its subdirectories. If <election> (resp. <jurisdiction>) is given,
        ignores all .ini files whose election (resp. jurisdiction) parameter value matches
        <election> (resp. <jurisdiction>).
    """
    df = pd.DataFrame(columns=["election", "jurisdiction", "results_note"])
    # collect notes
    try:
        paths = set()
        for root, dirs, files in os.walk(directory):
            for file in files:
                if file[-4:] == ".ini":
                    paths.update({os.path.join(root, file)})
        for p in paths:
            params, err = ui.get_parameters(
                required_keys=["election", "jurisdiction", "results_note"],
                header="election_results",
                param_file=p,
            )
            if err:
                print(f"Error reading parameters from file {p}:\n{err}")
            elif (election is None or params["election"] == election) and (
                jurisdiction is None or params["jurisdiction"] == jurisdiction
            ):
                df = df.append(
                    {
                        k: params[k]
                        for k in ["election", "jurisdiction", "results_note"]
                    },
                    ignore_index=True,
                )
    except Exception as exc:
        print(f"Exception occurred while exporting notes:\n{exc}")

    # print to file
    df = df[df["results_note"] != ""]
    df.sort_values(["election", "jurisdiction"], inplace=True)

    with open(target_file, "a") as f:
        for el in df["election"].unique():
            f.write(f"{el}\n")
            for idx, r in df[df.election == el].iterrows():
                f.write(f"{r['jurisdiction']}: {r['results_note']}\n\n")
    return


def get_major_subdivisions(
    session: Session = None,
    content_root: Optional[str] = None,
    major_subdivision_file: Optional[str] = None,
) -> (Dict[str, str], Optional[dict]):
    """
    Optional inputs:
        session: Session = None, sqlalchemy session
        content_root: Optional[str] = None, path to repository content root
        major_subdivision_file: Optional[str] = None, path to alternate file with columns 'jurisdiction',
            'major_sub_jurisdiction_type'

    Returns:
        Dict[str, str], map whose keys are all jurisdictions for which major subdivision type was found
        Optional[dict], error dictionary
    """
    err = None
    subdiv_dict = dict()
    # if file is given,
    if major_subdivision_file:
        # try to get the major subdivision type from the file
        subdiv_dict = get_major_subdiv_dict_from_file(major_subdivision_file)
        if subdiv_dict:
            return subdiv_dict, err
    elif content_root:
        # try from file in repo
        subdiv_dict = get_major_subdiv_dict_from_file(
            os.path.join(
                content_root,
                constants.subdivision_reference_file_path,
            ),
        )
        if subdiv_dict:
            return subdiv_dict, err

    # if not found in file or repo
    elif session:
        # find all jurisdiction_ids with data in db and look in db for their major subdivisions
        jurisdiction_id_list = db.jurisdiction_id_list(session)
        for jurisdiction_id in jurisdiction_id_list:
            jurisdiction = db.name_from_id(session, "ReportingUnit", jurisdiction_id)
            subdiv_dict[jurisdiction] = db.get_jurisdiction_hierarchy(
                session, jurisdiction_id
            )

    return subdiv_dict, err


def get_major_subdiv_dict_from_file(file_path: str) -> Optional[Dict[str, str]]:
    """
    Required inputs:
        file_path: str, path to file with columns 'jurisdiction', 'major_sub_jurisdiction_type'

    Returns:
        Optional[Dict[str,str]], dictionary mapping jurisdictions to subdivision types
            or, if any kind of error, None
    """
    try:
        df = pd.read_csv(file_path, sep="\t", index_col="jurisdiction")
        subdiv_dict = df["major_sub_jurisdiction_type"].to_dict()
    except Exception as exc:
        return None
    return subdiv_dict


def check_major_subdivisions(
    session: Optional[Session] = None,
    content_root: Optional[str] = None,
    major_subdivision_file: Optional[str] = None,
) -> (bool, Optional[str]):

    """
    Optional inputs:
        session: Optional[Session] = None, sqlalchemy session
        content_root: Optional[str] = None, path to repository content root
        major_subdivision_file: Optional[str] = None, path to file with columns
                'jurisdiction', 'major_sub_jurisdiction_type'

    Returns:
        bool, True if necessary major subdivisions can be found
            from <major_subdivision_file> if given,
            else from content_root if given,
            else from database session, if given
        Optional[str], error description, if there is an error
    """
    err_string = None
    ok = True
    major_subdiv_dict, err = get_major_subdivisions(
        session=session,
        content_root=content_root,
        major_subdivision_file=major_subdivision_file,
    )
    if err:
        err_string = f"No Analyzer created, because major subdivisions dictionary generated error: {err}"
        ok = False

    bad_jurisdictions = set()
    if session:
        for jurisdiction_id in db.jurisdiction_id_list(session):
            # make sure jurisdiction has subdivision type
            juris_name = db.name_from_id(session, "ReportingUnit", jurisdiction_id)
            if (
                juris_name not in major_subdiv_dict.keys()
                or major_subdiv_dict[juris_name] is None
            ):
                bad_jurisdictions.update({juris_name})
    if bad_jurisdictions:
        err_string = f"No Analyzer created, because no major subdivisions were found for these jurisdictions:\n"
        f"{sorted(list(bad_jurisdictions))}"
        ok = False
    return ok, err_string
