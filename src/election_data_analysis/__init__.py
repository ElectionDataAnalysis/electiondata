from election_data_analysis import (
    database as db,
    juris_and_munger as jm,
    user_interface as ui,
    munge as m,
)
from election_data_analysis import user_interface as ui
from election_data_analysis import munge as m
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session
from typing import List, Dict, Optional, Any, Tuple
import datetime
import os
import pandas as pd
import inspect
from pathlib import Path
import xml.etree.ElementTree as et
from election_data_analysis import analyze as a
from election_data_analysis import visualize as viz
from election_data_analysis import juris_and_munger as jm
from election_data_analysis import preparation as prep
from election_data_analysis import nist_export as nist
from election_data_analysis import externaldata as exd
import itertools
import shutil
import json

# constants
sdl_pars_req = [
    "munger_list",
    "results_file",
    "results_short_name",
    "results_download_date",
    "results_source",
    "results_note",
    "jurisdiction",
    "election",
]

# nb: jurisdiction_path is for backward compatibility
sdl_pars_opt = [
    "jurisdiction_path",
    "CandidateContest",
    "BallotMeasureContest",
    "BallotMeasureSelection",
    "Candidate",
    "Party",
    "CountItemType",
    "ReportingUnit",
    "Contest",
    "is_preliminary",
]

multi_data_loader_pars = [
    "results_dir",
    "archive_dir",
    "repository_content_root",
    "reports_and_plots_dir",
]

optional_mdl_pars = [
    "unloaded_dir",
]

prep_pars = [
    "name",
    "abbreviated_name",
    "count_of_state_house_districts",
    "count_of_state_senate_districts",
    "count_of_us_house_districts",
    "reporting_unit_type",
]

optional_prep_pars = []

analyze_pars = ["db_paramfile", "db_name"]

# classes
class SingleDataLoader:
    def __init__(
        self,
        results_dir: str,
        par_file_name: str,
        session: Session,
        mungers_path: str,
        juris_true_name: str,
        path_to_jurisdiction_dir: str,
    ):
        # adopt passed variables needed in future as attributes
        self.session = session
        self.results_dir = results_dir
        self.juris_true_name = juris_true_name
        self.par_file_name = par_file_name

        # calculate useful parameters
        self.juris_system_name = jm.system_name_from_true_name(self.juris_true_name)
        self.path_to_jurisdiction_dir = path_to_jurisdiction_dir

        # grab parameters (known to exist from __new__, so can ignore error variable)
        par_file = os.path.join(results_dir, par_file_name)
        self.d, dummy_err = ui.get_parameters(
            required_keys=sdl_pars_req,
            optional_keys=sdl_pars_opt,
            param_file=par_file,
            header="election_data_analysis",
        )

        # assign True to is_preliminary if necessary
        if "is_preliminary" not in self.d.keys() or self.d["is_preliminary"] in [
            "",
            "True",
            "true",
            None,
        ]:
            self.d["is_preliminary"] = True

        # change any blank parameters describing the results file to 'none'
        for k in self.d.keys():
            if self.d[k] == "" and k[:8] == "results_":
                self.d[k] = "none"

        # pick mungers (Note: munger_list is comma-separated list of munger names)
        self.munger = dict()
        self.munger_err = dict()
        # TODO document
        self.mungers_dir = mungers_path
        self.munger_list = [x.strip() for x in self.d["munger_list"].split(",")]

    def track_results(self) -> (dict, Optional[dict]):
        """insert a record for the _datafile, recording any error string <e>.
        Return Id of _datafile.Id and Election.Id"""
        err = None
        filename = self.d["results_file"]
        jurisdiction_id = db.name_to_id(
            self.session, "ReportingUnit", self.d["jurisdiction"]
        )
        if jurisdiction_id is None:
            err = ui.add_new_error(
                err,
                "ini",
                self.par_file_name,
                f"No ReportingUnit named {self.d['jurisdiction']} found in database",
            )
            return [0, 0], err
        election_id = db.name_to_id(self.session, "Election", self.d["election"])
        if election_id is None:
            err = ui.add_new_error(
                err,
                "ini",
                self.par_file_name,
                f"No election named {self.d['election']} found in database",
            )
            return [0, 0], err
        data = pd.DataFrame(
            [
                [
                    self.d["results_short_name"],
                    filename,
                    self.d["results_download_date"],
                    self.d["results_source"],
                    self.d["results_note"],
                    jurisdiction_id,
                    election_id,
                    datetime.datetime.now(),
                    self.d["is_preliminary"],
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
                self.session.bind, data, "_datafile", "ini", self.par_file_name
            )
            if ui.fatal_error(err):
                return [0, 0], err
            else:
                col_map = {"short_name": "short_name"}
                datafile_id = db.append_id_to_dframe(
                    self.session.bind, data, "_datafile", col_map=col_map
                ).iloc[0]["_datafile_Id"]
        except Exception as exc:
            err = ui.add_new_error(
                err,
                "system",
                f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
                f"Error inserting record to _datafile table or retrieving _datafile_Id: {exc}",
            )
            return [0, 0], err

        return {"_datafile_Id": datafile_id, "Election_Id": election_id}, err

    def load_results(
        self, rollup: bool = False, rollup_rut: Optional[str] = None
    ) -> dict:
        """Load results, returning error (or None, if load successful)"""
        err = None
        print(f'\n\nProcessing {self.d["results_file"]}')

        # Enter datafile info to db and collect _datafile_Id and Election_Id
        results_info, new_err = self.track_results()
        if new_err:
            err = ui.consolidate_errors([err, new_err])
            return err

        else:
            constants = self.collect_constants_from_ini()

            # load results to db
            for mu in self.munger_list:
                print(f"\twith munger {mu}")
                f_path = os.path.join(self.results_dir, self.d["results_file"])
                mu_path = os.path.join(self.mungers_dir, f"{mu}.munger")
                new_err = load_results_file(
                    self.session,
                    mu_path,
                    f_path,
                    self.juris_true_name,
                    results_info,
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
        """collect constant elements from .ini file.
        Omits constants that have no content"""
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
        """lists all values for the element found in the file"""
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
                    munger_path = os.path.join(self.mungers_dir, f"{mu}.munger")
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


class DataLoader:
    def __new__(cls, param_file: str = "run_time.ini", dbname: Optional[str] = None):
        """Checks if parameter file exists and is correct. If not, does
        not create DataLoader object."""

        d, err = ui.get_parameters(
            required_keys=multi_data_loader_pars,
            optional_keys=optional_mdl_pars,
            param_file=param_file,
            header="election_data_analysis",
        )
        if err:
            print(f"DataLoader object not created. Error:\n{err}")
            return None

        return super().__new__(cls)

    def __init__(self, param_file="run_time.ini", dbname: Optional[str] = None):
        # grab parameters
        self.d, self.parameter_err = ui.get_parameters(
            required_keys=multi_data_loader_pars,
            optional_keys=optional_mdl_pars,
            param_file=param_file,
            header="election_data_analysis",
        )

        # define parameters derived from run_time.ini
        self.d["ini_dir"] = os.path.join(
            self.d["repository_content_root"], "ini_files_for_results"
        )
        self.d["mungers_dir"] = os.path.join(
            self.d["repository_content_root"], "mungers"
        )

        # create db if it does not already exist and have right tables
        err = db.create_db_if_not_ok(db_param_file=param_file, dbname=dbname)

        # connect to db
        self.engine = None  # will be set in connect_to_db
        self.session = None  # will be set in connect_to_db
        self.connect_to_db(err=err, dbname=dbname, db_param_file=param_file)

    def connect_to_db(
        self,
        dbname: Optional[str] = None,
        err: Optional[dict] = None,
        db_param_file: str = "run_time.ini",
    ):
        new_err = None
        try:
            self.engine, new_err = db.sql_alchemy_connect(
                param_file=db_param_file, dbname=dbname
            )
            Session = sessionmaker(bind=self.engine)
            self.session = Session()
        except Exception as e:
            print(f"Cannot connect to database. Exiting. Exception:\n{e}")
            quit()
        if new_err:
            print("Unexpected error connecting to database.")
            err = ui.consolidate_errors([err, new_err])
            ui.report(err, self.d["reports_and_plots_dir"], file_prefix="dataloading_")
            print("Exiting")
            quit()
        else:
            return

    def change_db(self, new_db_name: str, db_param_file: str = "run_time.ini"):
        """Changes the database into which the data is loaded, including reconnecting"""
        self.d["dbname"] = new_db_name
        self.session.close()
        self.connect_to_db(dbname=new_db_name, db_param_file=db_param_file)
        db.create_db_if_not_ok(dbname=new_db_name)
        return

    def change_dir(self, dir_param: str, new_dir: str):
        # TODO technical debt: error handling
        self.d[dir_param] = new_dir
        return

    def load_one_from_ini(
        self,
        ini_path: str,
        path_to_jurisdiction_dir: str,
        juris_true_name: str,
        rollup: bool = False,
    ) -> (Optional[SingleDataLoader], Optional[dict]):
        """Load a single results file specified by the parameters in <ini_path>.
        Returns SingleDataLoader object (and error)"""
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
            rollup_rut = db.get_major_subdiv_type(
                self.session,
                sdl.d["jurisdiction"],
                file_path=os.path.join(
                    self.d["repository_content_root"],
                    "jurisdictions",
                    "000_major_subjurisdiction_types.txt",
                ),
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
    ) -> (List[str], List[str], str, Optional[dict]):
        """Looks within ini_files_for_results/<jurisdiction> for
        all ini files matching  given election and jurisdiction.
        For each, attempts to load file if it exists; reports missing data files.
        Returns
        * list of successfully-loaded files,
        * list of files that failed to load
        * latest download date for successfully-loaded files
        * error report with outright error for results files whose loading failed.
        If <report_missing_files> is True, includes warning for missing files"""
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
                    header="election_data_analysis",
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
    ) -> (Dict[str, List[str]], Dict[str, List[str]], Optional[dict]):
        """Processes all results (or all results corresponding to pairs in
        ej_list if given) in DataLoader's results directory using
        .ini files from repository.
        If load is successful for all files for a single election-jurisdicction pair,
        then add records for total vote counts whereever necessary.
        By default, loads (or updates) the info from the jurisdiction files
        into the db first. By default, moves files to the DataLoader's archive directory.
        Returns a post-reporting error dictionary, and a dictionary of
        successfully-loaded files (by election-jurisdiction pair).
        (Note: errors initializing loading process (e.g., results file not found) do *not* generate
        <success> = False, though those errors are reported in <err>"""
        # initialize
        err = None
        success = dict()
        failure = dict()
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
            for juris in jurisdictions:
                # check jurisdiction
                juris_path = os.path.join(
                    self.d["repository_content_root"],
                    "jurisdictions",
                    jm.system_name_from_true_name(juris),
                )
                new_err = jm.ensure_jurisdiction_dir(juris_path)
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
                        continue
                print(f"Loading/updating jurisdiction {juris} to {self.session.bind}")
                try:
                    new_err = jm.load_or_update_juris_to_db(
                        self.session,
                        os.path.join(
                            self.d["repository_content_root"],
                            "jurisdictions",
                            jm.system_name_from_true_name(juris),
                        ),
                        juris,
                    )
                    if new_err:
                        err = ui.consolidate_errors([err, new_err])
                except Exception as exc:
                    err = ui.add_new_error(
                        err, "jurisdiction", juris, f"Exception during loading: {exc}"
                    )
        else:
            print("No jurisdictions loaded because load_jurisdictions==False")

        for jurisdiction in jurisdictions:
            juris_err = None
            for election in elections[jurisdiction]:
                # load the relevant files
                (
                    success_list,
                    failure_list,
                    latest_download_date,
                    new_err,
                ) = self.load_ej_pair(
                    election,
                    jurisdiction,
                    rollup=rollup,
                    report_missing_files=report_missing_files,
                )
                if new_err:
                    juris_err = ui.consolidate_errors([juris_err, new_err])
                success[f"{election};{jurisdiction}"] = success_list
                failure[f"{election};{jurisdiction}"] = failure_list

                # if all files loaded successfully
                if not failure_list:
                    # add totals
                    self.add_totals_if_missing(election, jurisdiction)

            if move_files:
                # if all existing files referenced in any results.ini
                # for the jurisdiction
                # -- for any election -- loaded correctly
                if not ui.fatal_error(juris_err):
                    juris_system_name = jm.system_name_from_true_name(jurisdiction)
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
                                f"{juris_system_name}_{latest_download_date}",
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
                key_list=[
                    "munger",
                    "warn-munger",
                    "jurisdiction",
                    "warn-jurisdiction",
                    "file",
                    "warn-file",
                    "ini",
                    "warn-ini",
                    "warn-database",
                    "database",
                ],
            )
        #  report munger, jurisdiction and file errors & warnings

        # report remaining errors
        ui.report(err, report_dir, file_prefix="system")

        # keep all election-juris pairs in success report, but remove empty failure reports
        failure = {k: v for k, v in failure.items() if v}

        return success, failure, err

    def remove_data(
        self,
        election_id: int,
        juris_id: int,
    ) -> Optional[str]:
        """Remove from the db all data for the given <election_id> in the given <juris>"""
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
        for idx in df_list:
            db.remove_vote_counts(connection, cursor, idx)
        return None

    def add_totals_if_missing(self, election, jurisdiction) -> Optional[dict]:
        """for each contest, add 'total' vote type wherever it's missing
        returning any error"""
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
            "CountItemType_Id",
            "OtherCountItemType",
            "Count",
        ]
        aliases = [
            "Contest_Id",
            "Selection_Id",
            "ReportingUnit_Id",
            "Election_Id",
            "_datafile_Id",
            "CountItemType_Id",
            "OtherCountItemType",
            "Count",
        ]
        df = db.read_vote_count(
            self.session, election_id, jurisdiction_id, fields, aliases
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
                err = db.insert_to_cdf_db(
                    self.session.bind,
                    m_df,
                    "VoteCount",
                    "system",
                    f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
                )
            except Exception as exc:
                err = ui.add_new_error(
                    err,
                    "system",
                    f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
                    f"Unexpected exception while adding totals to existing db for {election} - {jurisdiction}",
                )
        return err

    def load_data_from_db_dump(self, dbname, dump_file: str) -> Optional[str]:
        """Create a database from a file dumped from another database (but only if db does not
        already exist). Return error string"""
        connection = self.session.bind.raw_connection()
        cursor = connection.cursor()
        err_str = db.create_database(
            connection, cursor, dbname=dbname, delete_existing=False
        )

        cursor.close()
        connection.close()

        if not err_str:
            # read contents of dump into db
            err_str = db.restore_to_db(dbname, dump_file, self.engine.url)
        return err_str

    def load_single_external_data_file(
            self,
            data_file: str,
            source: str,
            year: str,
            note: str,
            order_within_category: Optional[Dict[str,int]] = None,
            replace_existing: bool = False,  # TODO
    ) -> Optional[dict]:
        df = pd.read_csv(data_file)
        err = self.load_single_external_data_set(
            df,
            source,
            year,
            note,
            order_within_category=order_within_category,
            replace_existing=replace_existing,
        )
        return err

    def load_single_external_data_set(
        self,
        df: pd.DataFrame,
        source: str,
        year: str,
        note: str,
        replace_existing: bool = False,  # TODO
    ) -> Optional[dict]:
        """<data_file> has to be in particular form:
        csv
        columns: Category, Label, OrderWithinCategory, ReportingUnit, Value.
        Choices of "Category and "Label"
        will show up in analyze.display_options text.
        ReportingUnit assumed to follow name convention internal to db.
        order_within_category dictionary determines order within analyze.display_options"""
        err = None
        # TODO check columns of df
        # TODO error handling
        working = df.copy()
        working["Source"] = source
        working["Year"] = year
        working["Note"] = note

        # put info into ExternalDataSet table and retrieve Id
        eds = working[["Category","Label","OrderWithinCategory","Source","Year","Note"]].drop_duplicates().copy()
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
        eds_col_map = {c:c for c in eds.columns}
        working = db.append_id_to_dframe(self.session.bind,working,"ExternalDataSet",col_map=eds_col_map)
        ru = pd.read_sql_table("ReportingUnit", self.session.bind).rename(
            columns={"Id": "ReportingUnit_Id"}
        )
        working = working.merge(
            ru[["ReportingUnit_Id", "Name"]], how="left", left_on="ReportingUnit", right_on="Name"
        )
        ed = working[working.ReportingUnit_Id.notnull()][["Value","ReportingUnit_Id","ExternalDataSet_Id"]]
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
    def load_acs5_data(
        self, census_year: int, election: str
    ) -> Optional[dict]:
        """Download census.gov American Community Survey data by county
        for the given year;
        upload data to db; associate all datasets for the year to election-juris
        pairs for any juris that has nontrivial join."""
        err = None
        df = pd.DataFrame()
        for category in exd.acs5_columns.keys():
            columns_to_get = exd.acs5_columns[category].keys()
            working = exd.get_raw_acs5_data(columns_to_get, census_year)
            working = exd.combine_and_rename_columns(working, exd.acs_5_label_summands)
            working = exd.normalize(working, exd.acs_5_label_summands.keys())
            working["Category"] = category
        df = pd.concat(df, working)
        self.load_single_external_data_set()
        # TODO
        return err

    def temp_reload_existing_acs5_data(self, census_file: str) -> Optional[dict]:
        """Transitional function for taking data from Version 1.0 system to later version (June 2021)
        load data from census file exported from old db (e.g. census_no_ids.csv)"""
        cdf = pd.read_csv(census_file,index_col=None)

        els = pd.read_sql_table("Election",self.session.bind)
        els = els[els.Name != "none or unknown"]
        els["ElectionYear"] = els.Name.str[0:4]
        els["ElectionYear"] = pd.to_numeric(els["ElectionYear"])

        data_df = els.merge(cdf,how="left",on="ElectionYear").drop("Id",axis=1)

        for y in [2016,2018]:
            working = data_df[data_df.Year == y]

            self.load_single_external_data_set(
                working,
                "American Community Survey 5",
                y,
                "",
            )

        col_map = {c:c for c in ["Category","Label","Year"]}
        df_appended = db.append_id_to_dframe(
            self.session.bind,data_df,"ExternalDataSet",col_map=col_map)
        join_df = df_appended.merge(
            els,on="Name"
        ).rename(
            columns={"Id":"Election_Id"}
        )[["Election_Id","ExternalDataSet_Id"]]
        err = db.insert_to_cdf_db(
            self.session.bind,
            join_df,
            "ElectionExternalDataSetJoin",
            "database",
            "join data not loaded"
        )
        return err

def check_par_file_elements(
    ini_d: dict,
    mungers_path: str,
    ini_file_name: str,
) -> Optional[dict]:
    """<d> is the dictionary of parameters pulled from the ini file"""
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
    par_file_name: str,
    session: Session,
    mungers_path: str,
    juris_true_name: str,
    path_to_jurisdiction_dir: str,
) -> (Optional[SingleDataLoader], Optional[dict]):
    """Return SDL if it could be successfully initialized, and
    error dictionary (including munger errors noted in SDL initialization)"""

    # test parameters
    par_file = os.path.join(results_dir, par_file_name)
    sdl = None
    d, err = ui.get_parameters(
        required_keys=sdl_pars_req,
        optional_keys=sdl_pars_opt,
        param_file=par_file,
        header="election_data_analysis",
    )
    if err:
        return sdl, err

    # check consistency of munger and .ini file regarding elements to be read from ini file
    new_err_2 = check_par_file_elements(d, mungers_path, par_file_name)
    if new_err_2:
        err = ui.consolidate_errors([err, new_err_2])
        sdl = None
    if not ui.fatal_error(new_err_2):
        sdl = SingleDataLoader(
            results_dir,
            par_file_name,
            session,
            mungers_path,
            juris_true_name,
            path_to_jurisdiction_dir,
        )
        err = ui.consolidate_errors([err, sdl.munger_err])
    # check download date
    try:
        datetime.datetime.strptime(d["results_download_date"], "%Y-%m-%d")
        err_str = None
    except TypeError:
        err_str = f"No download date found"
    except ValueError:
        err_str = f"Date could not be parsed. Expected format is 'YYYY-MM-DD', actual is {d['results_download_date']}"
    if err_str:
        err = ui.add_new_error(err, "ini", par_file_name, err_str)
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
            (prep_param_file, prep_pars),
            (run_time_param_file, ["repository_content_root", "reports_and_plots_dir"]),
        ]:
            try:
                d, parameter_err = ui.get_parameters(
                    required_keys=required,
                    param_file=param_file,
                    header="election_data_analysis",
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
        target_dir: Optional[str] = None,
        templates: Optional[str] = None,
    ):
        """Create starter files in <target_dir>. If no <target_dir> is given, put the standard
        jurisdiction files into a subdirectory of the jurisdictions directory in the repo, and put
        the starter dictionary in the current directory.
        """
        # create and fill jurisdiction directory
        # TODO Feature: allow other districts to be set in paramfile
        print(f"\nStarting {inspect.currentframe().f_code.co_name}")
        error = jm.ensure_jurisdiction_dir(self.d["jurisdiction_path"])
        # add default entries
        project_root = Path(__file__).absolute().parents[1]
        # default templates are from repo
        if not templates:
            templates = os.path.join(
                project_root,
                "election_data_analysis",
                "juris_and_munger",
                "jurisdiction_templates",
            )
        for element in ["Party", "Election"]:
            new_err = prep.add_defaults(self.d["jurisdiction_path"], templates, element)
            if new_err:
                error = ui.consolidate_errors([error, new_err])

        # add all standard Offices/RUs/CandidateContests
        asc_err = self.add_standard_contests()

        # Feature create starter dictionary.txt with cdf_internal name
        #  used as placeholder for raw_identifier_value
        dict_err = self.starter_dictionary(target_dir=target_dir)

        error = ui.consolidate_errors([error, asc_err, dict_err])
        ui.report(
            error, self.d["reports_and_plots_dir"], file_prefix=f"prep_{self.d['name']}"
        )
        return error

    def add_primaries_to_dict(self) -> Optional[dict]:
        """Return error dictionary"""
        print("\nStarting add_primaries_to_dict")
        error = None

        primaries = {}
        # read CandidateContest.txt, Party.txt and dictionary.txt
        cc = prep.get_element(self.d["jurisdiction_path"], "CandidateContest")
        p = prep.get_element(self.d["jurisdiction_path"], "Party")
        d = prep.get_element(self.d["jurisdiction_path"], "dictionary")
        # for each CandidateContest line in dictionary.txt with cdf_identifier in CandidateContest.txt
        # and for each Party line in dictionary.txt with cdf_identifier in Party.txt
        # append corresponding line in dictionary.txt
        party_d = d[
            (d["cdf_element"] == "Party")
            & (d["cdf_internal_name"].isin(p["Name"].tolist()))
        ]
        contest_d = d[
            (d["cdf_element"] == "CandidateContest")
            & (d["cdf_internal_name"].isin(cc["Name"].tolist()))
        ]
        for i, p in party_d.iterrows():
            primaries[p["raw_identifier_value"]] = contest_d.copy().rename(
                columns={
                    "cdf_internal_name": "contest_internal",
                    "raw_identifier_value": "contest_raw",
                }
            )
            primaries[p["raw_identifier_value"]]["cdf_internal_name"] = primaries[
                p["raw_identifier_value"]
            ].apply(
                lambda row: prep.primary(
                    row, p["cdf_internal_name"], "contest_internal"
                ),
                axis=1,
            )
            primaries[p["raw_identifier_value"]]["raw_identifier_value"] = primaries[
                p["raw_identifier_value"]
            ].apply(
                lambda row: prep.primary(row, p["raw_identifier_value"], "contest_raw"),
                axis=1,
            )

        if primaries:
            df_list = [
                df[["cdf_element", "cdf_internal_name", "raw_identifier_value"]]
                for df in primaries.values()
            ]
            df_list.append(d)
            new_dictionary = pd.concat(df_list)
        else:
            new_dictionary = d
        new_err = prep.write_element(
            self.d["jurisdiction_path"], "dictionary", new_dictionary
        )
        ui.consolidate_errors([error, new_err])
        ui.report(
            error, self.d["reports_and_plots_dir"], file_prefix=f"prep_{self.d['name']}"
        )
        return error

    def add_standard_contests(
        self, juriswide_contests: list = None, other_districts: dict = None
    ) -> dict:
        """If <juriswide_contest> is None, use standard list hard-coded.
        Returns error dictionary"""
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

        w_office = prep.get_element(self.d["jurisdiction_path"], "Office")
        w_ru = prep.get_element(self.d["jurisdiction_path"], "ReportingUnit")
        w_cc = prep.get_element(self.d["jurisdiction_path"], "CandidateContest")
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
            juriswide_contests = [
                f"US President ({abbr})",
                f"{abbr} Governor",
                f"US Senate {abbr}",
                f"{abbr} Attorney General",
                f"{abbr} Lieutenant Governor",
                f"{abbr} Treasurer",
                f"{abbr} Secretary of State",
            ]
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
        new_err = prep.write_element(
            self.d["jurisdiction_path"], "Office", w_office.drop_duplicates()
        )
        if new_err:
            err = ui.consolidate_errors([err, new_err])
            if ui.fatal_error(new_err):
                return err

        new_err = prep.write_element(
            self.d["jurisdiction_path"], "ReportingUnit", w_ru.drop_duplicates()
        )
        if new_err:
            err = ui.consolidate_errors([err, new_err])
            if ui.fatal_error(new_err):
                return err
        new_err = prep.write_element(
            self.d["jurisdiction_path"], "CandidateContest", w_cc.drop_duplicates()
        )
        if new_err:
            err = ui.consolidate_errors([err, new_err])
            if ui.fatal_error(new_err):
                return err
        return err

    def add_primaries_to_candidate_contest(self) -> Optional[str]:
        print(f"\nStarting {inspect.currentframe().f_code.co_name}")

        primaries = {}
        err_str = None

        # get contests that are not already primaries
        contests = prep.get_element(self.d["jurisdiction_path"], "CandidateContest")
        # TODO might have to check for '' as well as nulls
        non_p_contests = contests[contests["PrimaryParty"].isnull()]
        if non_p_contests.empty:
            err_str = "CandidateContest.txt is missing or has no non-primary contests. No primary contests added."
            return err_str

        # get parties
        parties = prep.get_element(self.d["jurisdiction_path"], "Party")
        if parties.empty:
            if err_str:
                err_str += (
                    "\n Party.txt is missing or empty. No primary contests added."
                )
            else:
                err_str = "\n Party.txt is missing or empty. No primary contests added."
            return err_str

        for i, party in parties.iterrows():
            p = party["Name"]
            primaries[p] = non_p_contests.copy()
            primaries[p]["Name"] = non_p_contests.apply(
                lambda row: prep.primary(row, p, "Name"), axis=1
            )
            primaries[p]["PrimaryParty"] = p

        all_primaries = [primaries[p] for p in parties.Name.unique()]
        new_err = prep.write_element(
            self.d["jurisdiction_path"],
            "CandidateContest",
            pd.concat([contests] + all_primaries),
        )
        err_str = ui.consolidate_errors([err_str, new_err])
        return err_str

    def starter_dictionary(
        self, include_existing=True, target_dir: Optional[str] = None
    ) -> dict:
        """Creates a starter file for dictionary.txt, assuming raw_identifiers are the same as cdf_internal names.
        Puts file in the current directory. Returns error dictionary"""
        w = dict()
        elements = [
            "BallotMeasureContest",
            "Candidate",
            "CandidateContest",
            "Election",
            "Party",
            "ReportingUnit",
        ]
        old = prep.get_element(self.d["jurisdiction_path"], "dictionary")
        if not include_existing:
            old.drop()
        for element in elements:
            w[element] = prep.get_element(self.d["jurisdiction_path"], element)
            name_field = db.get_name_field(element)
            w[element] = m.add_constant_column(w[element], "cdf_element", element)
            w[element].rename(columns={name_field: "cdf_internal_name"}, inplace=True)
            w[element]["raw_identifier_value"] = w[element]["cdf_internal_name"]

        starter_file_name = f'{self.d["abbreviated_name"]}_starter_dictionary.txt'
        starter = pd.concat(
            [
                w[element][["cdf_element", "cdf_internal_name", "raw_identifier_value"]]
                for element in elements
            ]
        ).drop_duplicates()
        if target_dir:
            ssd_str = starter_dict_dir = target_dir
        else:
            ssd_str = "current directory (not in jurisdiction directory)"
            starter_dict_dir = "."
        err = prep.write_element(
            starter_dict_dir, "dictionary", starter, file_name=starter_file_name
        )
        print(f"Starter dictionary created in {ssd_str}:\n{starter_file_name}")
        return err

    def add_sub_county_rus(
        self,
        par_file_name: str,
        sub_ru_type: str = "precinct",
        county_type="county",
    ) -> Optional[dict]:
        err_list = list()
        dl = DataLoader()
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
            m_path = os.path.join(sdl.mungers_dir, f"{mu}.munger")
            mu_d, new_err = m.get_and_check_munger_params(m_path)
            # get ReportingUnit formula
            ru_formula = ""
            headers = [
                x
                for x in m.req_munger_parameters["munge_field_types"]
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
                jd_df = prep.get_element(self.d["jurisdiction_path"], "dictionary")
                ru_df = prep.get_element(self.d["jurisdiction_path"], "ReportingUnit")
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
                new_err = prep.write_element(
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
                new_err = prep.write_element(
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

    def make_test_file(self, election: str):
        juris_true_name = self.d["name"]
        juris_abbr = self.d["abbreviated_name"]
        tests_dir = os.path.join(
            Path(self.d["mungers_dir"]).parents[1],
            "tests",
            "specific_result_file_tests",
        )
        juris_test_dir = os.path.join(tests_dir, self.d["system_name"])
        sample_test_dir = os.path.join(tests_dir, "20xx_test_templates")
        election_str = jm.system_name_from_true_name(election)
        test_file_name = f"test_{self.d['system_name']}_{election_str}.py"
        new_test_file = os.path.join(juris_test_dir, test_file_name)
        if not os.path.isdir(juris_test_dir):
            os.mkdir(juris_test_dir)

        if not os.path.isfile(new_test_file):
            test_replace = {
                f'jurisdiction = "North Carolina"\nabbr = "NC"': f'jurisdiction = "{juris_true_name}"\nabbr = "{juris_abbr}"',
                f'single_county = "North Carolina;Bertie County"': f'single_county = "{juris_true_name}; "',
            }
            create_from_template(
                os.path.join(sample_test_dir, f"donttest_template_{election_str}.py"),
                new_test_file,
                test_replace,
            )
            return

    def make_ini_file(
        self,
        ini_name: str,
        munger_name_list: str,
        is_preliminary: bool = False,
    ):
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
                os.path.join(inis_dir, "template.ini"),
                new_ini_file,
                ini_replace,
            )
        return

    def make_munger_file(self, munger_name: str):
        new_munger_file = os.path.join(self.d["mungers_dir"], f"{munger_name}.munger")
        if not os.path.isfile(new_munger_file):
            munger_replace = dict()
            create_from_template(
                os.path.join(self.d["mungers_dir"], "000_template.munger"),
                new_munger_file,
                munger_replace,
            )
        return

    def __init__(
        self,
        prep_param_file: str = "jurisdiction_prep.ini",
        run_time_param_file: str = "run_time.ini",
        target_dir: Optional[str] = None,
    ):
        self.d = dict()
        # get parameters from jurisdiction_prep.ini and run_time.ini
        for param_file, required in [
            (prep_param_file, prep_pars),
            (run_time_param_file, ["repository_content_root", "reports_and_plots_dir"]),
        ]:
            d, parameter_err = ui.get_parameters(
                required_keys=required,
                param_file=param_file,
                header="election_data_analysis",
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
    output_directory: str,
    munger_list: str,
    jurisdiction: str,
    election: str,
    download_date: str = "1900-01-01",
    source: str = "unknown",
    results_note: str = "none",
    extension: Optional[str] = None,
):
    """Utility to create parameter files for multiple files.
    Makes a parameter file for each (non-.ini,non .*) file in <dir>,
    once all other necessary parameters are specified.
    If <extension> is given, makes parameter file for each file with the given extension.
    Writes .ini files to <output_directory"""
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
            f"[election_data_analysis]\nresults_file={juris_system_name}/{f}\n"
            f"munger_list={munger_list}\njurisdiction={jurisdiction}\nelection={election}\n"
            f"results_short_name={jurisdiction}_{f}\nresults_download_date={download_date}\n"
            f"results_source={source}\nresults_note={results_note}\n"
        )
        ini_file_name = ".".join(f.split(".")[:-1]) + ".ini"
        with open(os.path.join(output_directory, ini_file_name), "w") as p:
            p.write(par_text)
    return


class Analyzer:
    def __new__(cls, param_file=None, dbname=None):
        """Checks if parameter file exists and is correct. If not, does
        not create DataLoader object."""
        try:
            if not param_file:
                param_file = "run_time.ini"
            d, postgres_param_err = ui.get_parameters(
                required_keys=["dbname"],
                param_file=param_file,
                header="postgresql",
            )
            d, eda_err = ui.get_parameters(
                required_keys=["reports_and_plots_dir"],
                param_file=param_file,
                header="election_data_analysis",
            )
            if eda_err:
                print(eda_err)
                if ui.fatal_error(eda_err):
                    return None
        except FileNotFoundError:
            print(
                f"Parameter file '{param_file}' not found. Ensure that it is located"
                f" in the current directory ({os.getcwd()}).\nAnalyzer object not created."
            )
            return None

        if postgres_param_err or eda_err:
            print("Parameter file missing requirements.")
            print(f"postgres: {postgres_param_err}")
            print(f"election_data_analysis: {eda_err}")
            print("Analyzer object not created.")
            return None

        return super().__new__(cls)

    def __init__(self, param_file=None, dbname=None):
        if not param_file:
            param_file = "run_time.ini"

        # read reports_and_plots_dir from param_file
        d, error = ui.get_parameters(
            required_keys=["reports_and_plots_dir", "repository_content_root"],
            param_file=param_file,
            header="election_data_analysis",
        )
        self.reports_and_plots_dir = d["reports_and_plots_dir"]
        self.repository_content_root = d["repository_content_root"]

        # create session
        eng, err = db.sql_alchemy_connect(param_file, dbname=dbname)
        Session = sessionmaker(bind=eng)
        self.session = Session()

    # `verbose` param is not used but may be necessary. See github issue #524 for details
    def display_options(
        self, input_str: str, verbose: bool = True, filters: list = None
    ):
        """<input_str> is one of: 'election', 'jurisdiction', 'contest_type', 'contest',
        'category' or 'count'"""
        try:
            filters_mapped = ui.get_contest_type_mappings(filters)
            results = ui.get_filtered_input_options(
                self.session, input_str, filters_mapped
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
    ) -> Optional[list]:
        """Used to create a scatter plot based on selected inputs. The fig_type parameter
        is used when the user wants to actually create the visualization; this uses plotly
        so any image extension that is supported by plotly is usable here. Currently supports
        html, png, jpeg, webp, svg, pdf, and eps. Note that some filetypes may need plotly-orca
        installed as well."""
        jurisdiction_id = db.name_to_id(self.session, "ReportingUnit", jurisdiction)
        (
            subdivision_type_id,
            other_subdivision_type,
        ) = db.get_major_subdiv_id_and_othertext(self.session, jurisdiction_id)
        h_election_id = db.name_to_id(self.session, "Election", h_election)
        v_election_id = db.name_to_id(self.session, "Election", v_election)
        # *_type is either candidates or contests or parties
        h_type, h_count_item_type = self.split_category_input(h_category)
        v_type, v_count_item_type = self.split_category_input(v_category)
        h_runoff = h_count.endswith("Runoff")
        v_runoff = v_count.endswith("Runoff")
        h_count = h_count.split(" - ")[0].strip()
        v_count = v_count.split(" - ")[0].strip()

        agg_results = a.create_scatter(
            self.session,
            jurisdiction_id,
            subdivision_type_id,
            other_subdivision_type,
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
    ) -> list:
        """contest_type is an election district type, e.g.,
        state, congressional, state-senate, state-house, territory, etc.
        Complete list is given by the keys of <db.contest_type_mapping>"""
        election_id = db.name_to_id(self.session, "Election", election)
        jurisdiction_id = db.name_to_id(self.session, "ReportingUnit", jurisdiction)
        # for now, bar charts can only handle jurisdictions where county is one level
        # down from the jurisdiction
        subdivision_type_id, other_subdivision_type = db.get_jurisdiction_hierarchy(
            self.session, jurisdiction_id
        )
        # bar chart always at one level below top reporting unit
        agg_results = a.create_bar(
            self.session,
            jurisdiction_id,
            subdivision_type_id,
            other_subdivision_type,
            contest_type,
            contest,
            election_id,
            False,
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
        elif input_str == "Census data":
            return "census", "total"

    def export_outlier_data(
        self,
        election: str,
        jurisdiction: str,
        contest: str = None,
    ) -> list:
        """contest_type is one of state, congressional, state-senate, state-house"""
        election_id = db.name_to_id(self.session, "Election", election)
        jurisdiction_id = db.name_to_id(self.session, "ReportingUnit", jurisdiction)
        subdivision_type_id, other_subdivision_type = db.get_jurisdiction_hierarchy(
            self.session, jurisdiction_id
        )
        # bar chart always at one level below top reporting unit
        agg_results = a.create_bar(
            self.session,
            jurisdiction_id,
            subdivision_type_id,
            other_subdivision_type,
            None,
            contest,
            election_id,
            True,
        )
        return agg_results

    def top_counts(
        self, election: str, rollup_unit: str, sub_unit: str, by_vote_type: bool
    ) -> Optional[str]:
        rollup_unit_id = db.name_to_id(self.session, "ReportingUnit", rollup_unit)
        sub_unit_id = db.name_to_id(self.session, "ReportingUnitType", sub_unit)
        sub_rutype_othertext = ""
        if sub_unit_id is None:
            sub_rutype_othertext = sub_unit
        election_id = db.name_to_id(self.session, "Election", election)
        err = a.create_rollup(
            self.session,
            self.reports_and_plots_dir,
            top_ru_id=rollup_unit_id,
            sub_rutype_id=sub_unit_id,
            election_id=election_id,
            by_vote_type=by_vote_type,
            sub_rutype_othertext=sub_rutype_othertext,
        )
        return err

    def export_nist_v1_json(self, election: str, jurisdiction: str) -> dict:
        election_id = db.name_to_id(self.session, "Election", election)
        jurisdiction_id = db.name_to_id(self.session, "ReportingUnit", jurisdiction)

        election_report = dict()

        election_report["Contest"] = a.nist_candidate_contest(
            self.session, election_id, jurisdiction_id
        )
        election_report["GpUnit"] = a.nist_reporting_unit(
            self.session, election_id, jurisdiction_id
        )
        election_report["Party"] = a.nist_party(
            self.session, election_id, jurisdiction_id
        )
        election_report["Election"] = a.nist_election(
            self.session, election_id, jurisdiction_id
        )
        election_report["Office"] = a.nist_office(
            self.session, election_id, jurisdiction_id
        )
        election_report["Candidate"] = a.nist_candidate(
            self.session, election_id, jurisdiction_id
        )

        return election_report

    def export_nist_v1(
        self,
        election: str,
        jurisdiction: str,
    ) -> str:
        """exports NIST v1 json string"""
        json_string = json.dumps(self.export_nist_v1_json(election, jurisdiction))
        return json_string

    def export_nist(
        self,
        election: str,
        jurisdiction: str,
        major_subdivision: Optional[str] = None,
    ) -> str:
        """exports NIST v2 xml string"""
        xml_string = et.tostring(
            nist.nist_v2_xml_export_tree(
                self.session,
                election,
                jurisdiction,
                major_subdivision=major_subdivision,
                issuer=nist.default_issuer,
                issuer_abbreviation=nist.default_issuer_abbreviation,
                status=nist.default_status,
                vendor_application_id=nist.default_vendor_application_id,
            ).getroot(),
            encoding=m.default_encoding,
            method="xml",
        )
        return xml_string

    def diff_in_diff(
        self,
        election: str,
    ) -> (pd.DataFrame, list):
        """for each jurisdiction in the election that has more than just 'total',
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
            "county",
            "district_type",
            "party",
            "contest_pair",
            "min_count_by_contest",
            "vote_type_pair",
            "abs_diff_in_diff",
        ]
        # TODO tech debt works for candidate contests only

        # TODO: tech debt code treats 'other' like county or any county-like ru-type,
        #  which might cause trouble if there is a non-standard major subdivision type

        election_id = db.name_to_id(self.session, "Election", election)

        # loop through state-like jurisdictions with multiple vote types
        vts = db.vote_types_by_juris(self.session, election)

        contests_df = db.contest_families_by_juris(
            self.session,
            election,
        )
        # loop through states, etc.
        for state in vts.keys():
            state_rows = list()
            # get vote-types, contest roll-up by vote type and contest-by-election-district types
            data_file_list, _ = db.data_file_list(self.session, election_id)
            vote_types = {x for x in vts[state] if x != "total"}
            district_types = contests_df[contests_df["jurisdiction"] == state]
            state_id = db.name_to_id(self.session, "ReportingUnit", state)

            # find major subdivision
            major_sub_ru_type_name = db.get_major_subdiv_type(
                self.session,
                state,
                file_path=os.path.join(self.repository_content_root),
            )

            # get dataframe of results, adding column for political party
            res, _ = db.export_rollup_from_db(
                self.session,
                state,
                election,
                major_sub_ru_type_name,
                "Candidate",  # TODO extend to ballotmeasure contests too
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
                        (contests_df.jurisdiction == state)
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
                                                state,
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
            state_with_hyphens = jm.system_name_from_true_name(state)
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
        """given an election, a reporting unit -- not necessarily a whole jurisdiction--
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
        reportingunit_id: int,
    ) -> dict:
        """Returns dictionary of vote counts by element (summing over everything else
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
            election_id,
            reportingunit_id,
            fields,
            aliases,
        )
        # exclude any redundant total vote types
        if len(vc_df.CountItemType.unique()) > 1:
            vc_df = vc_df[vc_df.CountItemType != "total"]
        vc_dict = vc_df.groupby(name_field).sum("Count").to_dict()["Count"]
        return vc_dict


def aggregate_results(
    election: str,
    jurisdiction: str,
    dbname: Optional[str] = None,
    vote_type: Optional[str] = None,
    sub_unit: Optional[str] = None,
    contest: Optional[str] = None,
    contest_type: str = "Candidate",
    sub_unit_type: str = "county",
    exclude_redundant_total: bool = True,
) -> pd.DataFrame:
    """if a vote type is given, restricts to that vote type; otherwise returns all vote types;
    Similarly for sub_unit and contest"""
    # using the analyzer gives us access to DB session
    empty_df_with_good_cols = pd.DataFrame(columns=["contest", "count"])
    an = Analyzer(dbname=dbname)
    if not an:
        return empty_df_with_good_cols
    election_id = db.name_to_id(an.session, "Election", election)
    jurisdiction_id = db.name_to_id(an.session, "ReportingUnit", jurisdiction)
    if not election_id:
        return empty_df_with_good_cols
    connection = an.session.bind.raw_connection()
    cursor = connection.cursor()

    datafile_list, e = db.data_file_list_cursor(
        cursor,
        election_id,
        reporting_unit_id=jurisdiction_id,
        by="Id",
    )
    if e:
        print(e)
        return empty_df_with_good_cols
    if len(datafile_list) == 0:
        print(
            f"No datafiles found for election {election} and jurisdiction {jurisdiction}"
            f"(election_id={election_id} and jurisdiction_id={jurisdiction_id})"
        )
        return empty_df_with_good_cols

    df, err_str = db.export_rollup_from_db(
        # cursor=cursor,
        session=an.session,
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


def data_exists(
    election: str,
    jurisdiction: str,
    p_path: Optional[str] = None,
    dbname: Optional[str] = None,
) -> bool:
    an = Analyzer(param_file=p_path, dbname=dbname)
    if not an:
        return False

    election_id = db.name_to_id(an.session, "Election", election)
    reporting_unit_id = db.name_to_id(an.session, "ReportingUnit", jurisdiction)

    # if the database doesn't have the election or the reporting unit
    if not election_id or not reporting_unit_id:
        # data doesn't exist
        return False

    # read all contests with records in the VoteCount table
    df = db.read_vote_count(
        an.session, election_id, reporting_unit_id, ["ContestName"], ["contest_name"]
    )
    # if no contest found
    if df.empty:
        # no data exists.
        return False
    # otherwise
    else:
        # then data exists!
        return True


def census_data_exists(
    election: str,
    jurisdiction: str,
    p_path: Optional[str] = None,
    dbname: Optional[str] = None,
) -> bool:
    an = Analyzer(param_file=p_path, dbname=dbname)
    if not an:
        return False

    jurisdiction_id = db.name_to_id(an.session, "ReportingUnit", jurisdiction)
    election_id = db.name_to_id(an.session, "Election", election)

    # if the database doesn't have the reporting unit
    if not jurisdiction_id:
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


def check_totals_match_vote_types(
    election: str,
    jurisdiction: str,
    sub_unit_type="county",
    dbname=None,
) -> bool:
    """Interesting if there are both total and other vote types;
    otherwise trivially true"""
    an = Analyzer(dbname=dbname)
    active = db.active_vote_types(an.session, election, jurisdiction)
    if len(active) > 1 and "total" in active:
        # pull type 'total' only
        df_candidate = aggregate_results(
            election,
            jurisdiction,
            contest_type="Candidate",
            vote_type="total",
            sub_unit_type=sub_unit_type,
            exclude_redundant_total=False,
            dbname=dbname,
        )
        df_ballot = aggregate_results(
            election,
            jurisdiction,
            contest_type="BallotMeasure",
            vote_type="total",
            sub_unit_type=sub_unit_type,
            exclude_redundant_total=False,
            dbname=dbname,
        )
        df_total_type_only = pd.concat([df_candidate, df_ballot])

        # pull all types but total
        df_candidate = aggregate_results(
            election,
            jurisdiction,
            contest_type="Candidate",
            sub_unit_type=sub_unit_type,
            exclude_redundant_total=True,
            dbname=dbname,
        )
        df_ballot = aggregate_results(
            election,
            jurisdiction,
            contest_type="BallotMeasure",
            exclude_redundant_total=True,
            sub_unit_type=sub_unit_type,
            dbname=dbname,
        )
        df_sum_nontotal_types = pd.concat([df_candidate, df_ballot])
        return df_total_type_only["count"].sum() == df_sum_nontotal_types["count"].sum()
    else:
        return True


def contest_total(
    election: str,
    jurisdiction: str,
    contest: str,
    dbname: Optional[str] = None,
    vote_type: Optional[str] = None,
    county: Optional[str] = None,
    sub_unit_type: str = "county",
    contest_type: Optional[str] = "Candidate",
) -> int:
    df = aggregate_results(
        election=election,
        jurisdiction=jurisdiction,
        dbname=dbname,
        vote_type=vote_type,
        sub_unit=county,
        sub_unit_type=sub_unit_type,
        contest=contest,
        contest_type=contest_type,
    )
    return df["count"].sum()


def count_type_total(
    election: str,
    jurisdiction: str,
    contest: str,
    count_item_type: str,
    sub_unit_type: str = "county",
    dbname: Optional[str] = None,
) -> int:
    df_candidate = aggregate_results(
        election=election,
        jurisdiction=jurisdiction,
        contest=contest,
        contest_type="Candidate",
        vote_type=count_item_type,
        sub_unit_type=sub_unit_type,
        dbname=dbname,
    )
    df_ballot = aggregate_results(
        election=election,
        jurisdiction=jurisdiction,
        contest=contest,
        contest_type="BallotMeasure",
        vote_type=count_item_type,
        sub_unit_type=sub_unit_type,
        dbname=dbname,
    )
    df = pd.concat([df_candidate, df_ballot])
    if df.empty:
        return 0
    else:
        return df["count"].sum()


def check_count_types_standard(
    election: str, jurisdiction: str, dbname: Optional[str] = None
) -> bool:
    an = Analyzer(dbname=dbname)
    election_id = db.name_to_id(an.session, "Election", election)
    reporting_unit_id = db.name_to_id(an.session, "ReportingUnit", jurisdiction)
    standard_ct_list = list(
        db.read_vote_count(
            an.session,
            election_id,
            reporting_unit_id,
            ["CountItemType"],
            ["CountItemType"],
        )["CountItemType"].unique()
    )

    # don't want type "other"
    if "other" in standard_ct_list:
        standard_ct_list.remove("other")
    active = db.active_vote_types(an.session, election, jurisdiction)
    for vt in active:
        # if even one fails, count types are not standard
        if vt not in standard_ct_list:
            return False
    # if nothing failed, count types are standard
    return True


def get_contest_with_unknown_candidates(
    election: str, jurisdiction: str, dbname: Optional[str] = None
) -> List[str]:
    an = Analyzer(dbname=dbname)
    if not an:
        return [f"Failure to connect to database"]
    election_id = db.name_to_id(an.session, "Election", election)
    if not election_id:
        return [f"Election {election} not found"]
    jurisdiction_id = db.name_to_id(an.session, "ReportingUnit", jurisdiction)
    if not jurisdiction_id:
        return [f"Jurisdiction {jurisdiction} not found"]

    contests = db.get_contest_with_unknown(an.session, election_id, jurisdiction_id)
    return contests


def load_results_file(
    session: Session,
    munger_path: str,
    f_path: str,
    juris_true_name: str,
    election_datafile_ids: dict,
    constants: Dict[str, str],
    results_directory_path: str,
    path_to_jurisdiction_dir: str,
    rollup: bool = False,
    rollup_rut: str = "county",
) -> Optional[dict]:

    # TODO tech debt: redundant to pass results_directory_path and f_path
    munger_name = Path(munger_path).name
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

    # # add Id columns for all but Count, removing raw-munged
    try:
        df, new_err = m.munge_raw_to_ids(
            df,
            necessary_constants,
            path_to_jurisdiction_dir,
            munger_name,
            juris_true_name,
            session,
            p["file_type"],
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
        df[df.Selection_Id.isin(nou_selection_ids)]
        .groupby(["Contest_Id", "Selection_Id"])
        .sum()
    )
    for (contest_id, selection_id) in unknown.index:
        mask = df[["Contest_Id", "Selection_Id"]] == (contest_id, selection_id)
        df = df[~mask.all(axis=1)]

    if df.empty:
        err = ui.add_new_error(
            err,
            "jurisdiction",
            juris_true_name,
            f"No contest-selection pairs recognized via munger {munger_name} from {Path(f_path).name}",
        )
        return err

    # rollup_dataframe results if requested
    if rollup:
        df, new_err = a.rollup_dataframe(
            session, df, "Count", "ReportingUnit_Id", "ReportingUnit_Id", rollup_rut
        )
        if new_err:
            err = ui.consolidate_errors([err, new_err])
            if ui.fatal_error(new_err):
                return err

    # add_datafile_Id and Election_Id columns
    for c in ["_datafile_Id", "Election_Id"]:
        df = m.add_constant_column(df, c, election_datafile_ids[c])
    # load counts to db
    try:
        err = m.fill_vote_count(df, session, munger_name, err)
    except Exception as exc:
        err = ui.add_new_error(
            err,
            "system",
            f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
            f"Exception while filling vote count table: {exc}",
        )
        return err
    return err


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
    test_dir: Optional[str] = None, rollup: bool = False
) -> Optional[dict]:
    err = None
    dataloader = DataLoader()
    if dataloader:
        ts = datetime.datetime.now().strftime("%m%d_%H%M")
        error_and_warning_dir = os.path.join(
            dataloader.d["reports_and_plots_dir"], f"load_or_reload_all_{ts}"
        )
        # if no test directory given, use tests from repo
        if not test_dir:
            test_dir = os.path.join(
                Path(dataloader.d["repository_content_root"]).parent,
                "tests",
                "specific_result_file_tests",
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
                new_err = ui.reload_juris_election(
                    jurisdiction,
                    election,
                    test_dir,
                    error_and_warning_dir,
                    rollup=rollup,
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
        ui.report(err, error_and_warning_dir, file_prefix=f"loading_")
    else:
        current_directory = os.getcwd()
        print(
            f"No dataloader created; check that {current_directory}/run_time.ini exists "
            f"and has all necessary parameters."
        )
    return err
