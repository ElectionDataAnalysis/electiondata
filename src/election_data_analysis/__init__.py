from election_data_analysis import database as db, juris_and_munger as jm, user_interface as ui, munge as m
from election_data_analysis import user_interface as ui
from election_data_analysis import munge as m
from sqlalchemy.orm import sessionmaker
from typing import List, Dict, Optional
import datetime
import os
import pandas as pd
import ntpath
import inspect
from pathlib import Path
from election_data_analysis import analyze as a
from election_data_analysis import visualize as viz
from election_data_analysis import juris_and_munger as jm
from election_data_analysis import preparation as prep

# constants
# from election_data_analysis.munge import get_and_check_munger_params, to_standard_count_frame, munge_source_to_raw, \
    # munge_raw_to_ids, fill_vote_count
default_encoding = "utf_8"

sdl_pars_req = [
    "munger_name",
    "results_file",
    "results_short_name",
    "results_download_date",
    "results_source",
    "results_note",
    "top_reporting_unit",
    "election",
]

# nb: jurisdiction_path is for backward compatibility
sdl_pars_opt = [
    "jurisdiction_path",
    "jurisdiction_directory",
    "aux_data_dir",
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
    "jurisdictions_dir",
    "mungers_dir",
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
class DataLoader:
    def __new__(cls):
        """Checks if parameter file exists and is correct. If not, does
        not create DataLoader object."""
        d, err = ui.get_parameters(
            required_keys=multi_data_loader_pars,
            optional_keys=optional_mdl_pars,
            param_file="run_time.ini",
            header="election_data_analysis",
        )
        if err:
            print("DataLoader object not created.")
            ui.report(err)
            return None

        return super().__new__(cls)

    def __init__(self):
        # grab parameters
        self.d, self.parameter_err = ui.get_parameters(
            required_keys=multi_data_loader_pars,
            optional_keys=optional_mdl_pars,
            param_file="run_time.ini",
            header="election_data_analysis",
        )

        # create db if it does not already exist and have right tables
        err = db.create_db_if_not_ok()

        # connect to db
        self.connect_to_db(err=err)

    def connect_to_db(self, dbname: Optional[str] = None, err: Optional[dict] = None):
        new_err = None
        try:
            self.engine, new_err = db.sql_alchemy_connect(dbname=dbname)
            Session = sessionmaker(bind=self.engine)
            self.session = Session()
        except Exception as e:
            print("Cannot connect to database. Exiting.")
            quit()
        if new_err:
            print("Unexpected error connecting to database.")
            err = ui.consolidate_errors([err, new_err])
            ui.report(err)
            print("Exiting")
            quit()
        else:
            return

    def change_db(self, new_db_name: str):
        """Changes the database into which the data is loaded, including reconnecting"""
        self.d["dbname"] = new_db_name
        self.session.close()
        self.connect_to_db(dbname=new_db_name)
        db.create_db_if_not_ok(dbname=new_db_name)
        return

    def change_dir(self, dir_param: str, new_dir: str):
        # TODO technical debt: error handling
        self.d[dir_param] = new_dir
        return

    def load_all(
        self,
        load_jurisdictions: bool = True,
        move_files: bool = True,
        election_jurisdiction_list: Optional[list] = None,
    ) -> (Optional[dict], bool):
        """Processes all .ini files in the DataLoader's results directory.
        By default, loads (or reloads) the info from the jurisdiction files
        into the db first. By default, moves files to the DataLoader's archive directory.
        Returns a post-reporting error dictionary, and a flag to indicate whether all loaded successfully.
        (Note: errors initializing loading process (e.g., results file not found) do *not* generate
        <success> = False, though those errors are reported in <err>"""
        # initialize error dictionary and success flag
        err = None
        success = True

        # set locations for error reporting
        # TODO get rid of mungers_path variable, use self.d directly
        mungers_path = self.d["mungers_dir"]

        # define directory for archiving successfully loaded files (and storing warnings)
        db_param, new_err = ui.get_parameters(
            required_keys=["dbname"], param_file="run_time.ini", header="postgresql"
        )
        if new_err:
            err = ui.consolidate_errors([err, new_err])
        if ui.fatal_error(new_err):
            err = ui.report(err)
            return err, False

        # specify directories for archiving and reporting warnings
        success_dir = os.path.join(self.d["archive_dir"], db_param["dbname"])
        loc_dict = {
            "munger": self.d["results_dir"],
            "jurisdiction": self.d["results_dir"],
            "warn-munger": self.d["results_dir"],
            "warn-jurisdiction": self.d["results_dir"],
        }

        # list .ini files and pull their jurisdiction_paths
        par_files = [f for f in os.listdir(self.d["results_dir"]) if f[-4:] == ".ini"]

        # no .ini files found, return error
        if not par_files:
            err = ui.add_new_error(
                err,
                "file",
                self.d["results_dir"],
                f"No <results>.ini files found in directory. No results files will be processed.",
            )
            err = ui.report(err)
            return err, False

        params = dict()
        juris_directory = dict()

        # For each par_file get params or throw error
        good_par_files = list()
        for f in par_files:
            # grab parameters
            par_file = os.path.join(self.d["results_dir"], f)
            params[f], new_err = ui.get_parameters(
                required_keys=sdl_pars_req,
                optional_keys=sdl_pars_opt,
                param_file=par_file,
                header="election_data_analysis",
            )
            if new_err:
                err = ui.consolidate_errors([err, new_err])
            if not ui.fatal_error(new_err):
                ###########
                # for backwards compatibility
                if not params[f]["jurisdiction_directory"]:
                    params[f]["jurisdiction_directory"] = Path(
                        params[f]["jurisdiction_path"]
                    ).name
                ###########
                if election_jurisdiction_list:
                    if (
                        params[f]["election"],
                        params[f]["top_reporting_unit"],
                    ) in election_jurisdiction_list:
                        good_par_files.append(f)
                else:
                    good_par_files.append(f)
                juris_directory[f] = params[f]["jurisdiction_directory"]

        # group .ini files by jurisdiction_directory name
        jurisdiction_dirs = list({juris_directory[f] for f in good_par_files})
        jurisdiction_dirs.sort()

        # for each jurisdiction, create Jurisdiction or throw error
        good_jurisdictions = list()
        juris = dict()
        for jp in jurisdiction_dirs:
            # create and load jurisdiction or throw error
            juris[jp], new_err = ui.pick_juris_from_filesystem(
                juris_path=os.path.join(self.d["jurisdictions_dir"], jp),
                err=None,
                check_files=load_jurisdictions,
            )
            if new_err:
                err = ui.consolidate_errors([err, new_err])

            # if no fatal errors thrown, continue to process jp
            if not ui.fatal_error(new_err):
                # if asked to load the jurisdiction, load it.
                if load_jurisdictions:
                    print(f"Loading jurisdiction from {jp} to {self.session.bind}")
                    try:
                        new_err = juris[jp].load_juris_to_db(
                            self.session,
                        )
                        if new_err:
                            err = ui.consolidate_errors([err, new_err])
                    except Exception as exc:
                        err = ui.add_new_error(
                            err,
                            "jurisdiction",
                            jp,
                            f"Exception during loading: {exc}"
                        )
                    if not ui.fatal_error(new_err):
                        good_jurisdictions.append(jp)
                # if not asked to load jurisdiction, assume it's loaded
                else:
                    print(
                        f"Jurisdiction {juris[jp].name} assumed to be loaded to database already"
                    )
                    good_jurisdictions.append(jp)
            else:
                err = ui.consolidate_errors([err, new_err])
                return err, False

        # process all good parameter files with good jurisdictions
        for jp in good_jurisdictions:
            good_files = [f for f in good_par_files if juris_directory[f] == jp]
            print(f"Processing results files specified in {good_files}")
            for f in good_files:
                sdl, new_err = check_and_init_singledataloader(
                    self.d["results_dir"],
                    f,
                    self.session,
                    mungers_path,
                    juris[jp],
                )
                if new_err:
                    err = ui.consolidate_errors([err, new_err])

                # if fatal error, print warning
                if ui.fatal_error(new_err):
                    print(f"Fatal error before loading; data not loaded from {f}")
                # if no fatal error from SDL initialization, continue
                else:
                    # try to load data
                    load_error = sdl.load_results()
                    if load_error:
                        err = ui.consolidate_errors([err, load_error])

                    # if move_files == True and no fatal load error,
                    if move_files and not ui.fatal_error(load_error):
                        # archive files
                        ui.archive_from_param_file(
                            f, self.d["results_dir"], success_dir
                        )
                        print(
                            f"\tArchived {f} and its results file after successful load "
                            f"via mungers {sdl.d['munger_name']}.\n"
                        )
                    # if there was a fatal load error
                    elif ui.fatal_error(load_error):
                        print(f"\tFatal loading errors. {f} and its results file not loaded (and not archived)")
                        success = False

                    # if move_files is false and there is no fatal error
                    else:
                        print(
                            f"{f} and its results file loaded successfully via mungers {sdl.d['munger_name']}."
                        )

                #  report munger, jurisdiction and file errors & warnings
                err = ui.report(
                    err,
                    loc_dict=loc_dict,
                    key_list=[
                        "munger",
                        "jurisdiction",
                        "file",
                        "warn-munger",
                        "warn-jurisdiction",
                        "warn-file",
                    ],
                    file_prefix=f"{f[:-4]}_",
                )
        # report remaining errors
        loc_dict = {
            "munger": self.d["results_dir"],
            "jurisdiction": self.d["results_dir"],
            "warn-munger": self.d["results_dir"],
            "warn-jurisdiction": self.d["results_dir"],
        }
        ui.report(err, loc_dict)
        return err, success

    def remove_data(self, election_id: int, juris_id: int, active_confirm: bool) -> Optional[str]:
        """Remove from the db all data for the given <election_id> in the given <juris>"""
        # get connection & cursor
        connection = self.session.bind.raw_connection()
        cursor = connection.cursor()

        # find all datafile ids matching the given election and jurisdiction
        df_list, err_str = db.data_file_list(
            cursor, election_id, reporting_unit_id=juris_id
        )
        if err_str:
            return err_str

        # remove data from all those datafiles
        for idx in df_list:
            db.remove_vote_counts(connection, cursor, idx, active_confirm)
        return None


class SingleDataLoader:
    def __init__(
        self,
        results_dir: str,
        par_file_name: str,
        session,
        mungers_path: str,
        juris: jm.Jurisdiction,
    ):
        # adopt passed variables needed in future as attributes
        self.session = session
        self.results_dir = results_dir
        self.juris = juris
        self.par_file_name = par_file_name

        # grab parameters (known to exist from __new__, so can ignore error variable)
        par_file = os.path.join(results_dir, par_file_name)
        self.d, dummy_err = ui.get_parameters(
            required_keys=sdl_pars_req,
            optional_keys=sdl_pars_opt,
            param_file=par_file,
            header="election_data_analysis",
        )

        # assign None to aux_data_dir if necessary
        if "aux_data_dir" not in self.d.keys() or self.d["aux_data_dir"] in [
            "",
            "None",
            "none",
        ]:
            self.d["aux_data_dir"] = None

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

        # pick mungers (Note: munger_name is comma-separated list of munger names)
        self.munger = dict()
        self.munger_err = dict()
        # TODO document
        self.mungers_dir = mungers_path
        self.munger_list = [x.strip() for x in self.d["munger_name"].split(",")]
        # TODO check mungers for consistency?

    def track_results(self) -> (dict, Optional[str]):
        """insert a record for the _datafile, recording any error string <e>.
        Return Id of _datafile.Id and Election.Id"""
        filename = self.d["results_file"]
        top_reporting_unit_id = db.name_to_id(
            self.session, "ReportingUnit", self.d["top_reporting_unit"]
        )
        if top_reporting_unit_id is None:
            e = f"No ReportingUnit named {self.d['top_reporting_unit']} found in database"
            return [0, 0], e
        election_id = db.name_to_id(self.session, "Election", self.d["election"])
        if election_id is None:
            e = f"No election named {self.d['election']} found in database"
            return [0, 0], e
        data = pd.DataFrame(
            [
                [
                    self.d["results_short_name"],
                    filename,
                    self.d["results_download_date"],
                    self.d["results_source"],
                    self.d["results_note"],
                    top_reporting_unit_id,
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
            e = db.insert_to_cdf_db(self.session.bind, data, "_datafile")
            if e:
                return [0, 0], e
            else:
                col_map = {"short_name": "short_name"}
                datafile_id = db.append_id_to_dframe(
                    self.session.bind, data, "_datafile", col_map=col_map
                ).iloc[0]["_datafile_Id"]
        except Exception as exc:
            return (
                [0, 0],
                f"Error inserting record to _datafile table or retrieving _datafile_Id: {exc}",
            )
        return {"_datafile_Id": datafile_id, "Election_Id": election_id}, e

    def load_results(self) -> dict:
        """Load results, returning error (or None, if load successful)"""
        err = None
        print(f'\n\nProcessing {self.d["results_file"]}')

        # Enter datafile info to db and collect _datafile_Id and Election_Id
        results_info, err_str = self.track_results()
        if err_str:
            err = ui.add_new_error(
                err,
                "system",
                "SingleDataLoader.load_results",
                f"Error inserting _datafile record:\n{err_str}" f" ",
            )
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
                    self.juris,
                    results_info,
                    constants,
                    self.results_dir,
                )
                if new_err:
                    err = ui.consolidate_errors([err, new_err])
        return err

    def collect_constants_from_ini(self) -> dict:
        """collect constant elements from .ini file"""
        constants = dict()
        for k in ["Party",
                  "ReportingUnit",
                  "CountItemType",
                  "CandidateContest",
                  "BallotMeasureContest",
                  "BallotMeasureSelection",
                  "Candidate",
                  ]:
            # if element was given in .ini file
            if self.d[k] is not None:
                constants[k] = self.d[k]
        return constants


def check_par_file_elements(
    ini_d: dict,
    mungers_path: str,
    ini_file_name: str,
) -> Optional[dict]:
    """<d> is the dictionary of parameters pulled from the ini file"""
    err = None

    # for each munger,
    for mu in ini_d["munger_name"].split(","):
        # check any constant_over_file elements are defined in .ini file
        munger_file = os.path.join(mungers_path,f"{mu}.munger")
        params, p_err = ui.get_parameters(
            required_keys=list(), param_file=munger_file, optional_keys=["constant_over_file"], header="format",
        )
        if p_err:
            err = ui.consolidate_errors([err, p_err])
        elif params["constant_over_file"]:
            bad_constants = list()
            for c in params["constant_over_file"].split(","):
                if c not in ini_d.keys() or ini_d[c] is None:
                    bad_constants.append(c)
            if bad_constants:
                err = ui.add_new_error(err, "ini", ini_file_name, f"Munger requires constants to be defined:\n{bad_constants}")

    return err


def check_and_init_singledataloader(
    results_dir: str,
    par_file_name: str,
    session,
    mungers_path: str,
    juris: jm.Jurisdiction,
) -> (SingleDataLoader, Optional[dict]):
    """Return SDL if it could be successfully initialized, and
    error dictionary (including munger errors noted in SDL initialization)"""
    # test parameters
    par_file = os.path.join(results_dir, par_file_name)
    d, err = ui.get_parameters(
        required_keys=sdl_pars_req,
        optional_keys=sdl_pars_opt,
        param_file=par_file,
        header="election_data_analysis",
    )
    if err:
        sdl = None
        return sdl, err

    # check consistency of munger and .ini file regarding elements to be read from ini file
    new_err_2 = check_par_file_elements(d, mungers_path, par_file_name)
    if new_err_2:
        err = ui.consolidate_errors([err, new_err_2])
    if not ui.fatal_error(new_err_2):
        sdl = SingleDataLoader(
            results_dir,
            par_file_name,
            session,
            mungers_path,
            juris,
        )
        err = ui.consolidate_errors([err, sdl.munger_err])
    return sdl, err


class JurisdictionPrepper:
    def __new__(cls):
        """Checks if parameter file exists and is correct. If not, does
        not create JurisdictionPrepper object."""
        for param_file, required in [
            ("jurisdiction_prep.ini",prep_pars),
            ("run_time.ini", ["jurisdictions_dir","mungers_dir"]),
        ]:
            try:
                d, parameter_err = ui.get_parameters(
                    required_keys=required,
                    param_file=param_file,
                    header="election_data_analysis",
                )
            except FileNotFoundError as e:
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

    def new_juris_files(self):
        """<juris_path> identifies the directory where the files will live.
        <abbr> is the two-letter abbreviation for state/district/territory.
        <state_house>, etc., gives the number of districts;
        <other_districts> is a dictionary of other district names, types & counts, e.g.,
        {'Circuit Court':{'ReportingUnitType':'judicial','count':5}}
        """
        # TODO Feature: allow other districts to be set in paramfile
        print(f"\nStarting {inspect.currentframe().f_code.co_name}")
        error = jm.ensure_jurisdiction_dir(self.d["jurisdiction_path"])
        # add default entries
        project_root = Path(__file__).absolute().parents[1]
        templates = os.path.join(
            project_root, "juris_and_munger", "jurisdiction_templates"
        )
        for element in ["Party", "Election"]:
            new_err = prep.add_defaults(self.d["jurisdiction_path"], templates, element)
            if new_err:
                error = ui.consolidate_errors([error, new_err])

        # add all standard Offices/RUs/CandidateContests
        asc_err = self.add_standard_contests()

        # Feature create starter dictionary.txt with cdf_internal name
        #  used as placeholder for raw_identifier_value
        dict_err = self.starter_dictionary()

        error = ui.consolidate_errors([error, asc_err, dict_err])
        ui.report(error)
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
        ui.report(error)
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
                err_str += "\n Party.txt is missing or empty. No primary contests added."
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

    def starter_dictionary(self, include_existing=True) -> dict:
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
        err = prep.write_element(
            ".", "dictionary", starter, file_name=starter_file_name
        )
        print(
            f"Starter dictionary created in current directory (not in jurisdiction directory):\n{starter_file_name}"
        )
        return err

    def __init__(self):
        self.d = dict()
        # get parameters from jurisdiction_prep.ini and run_time.ini
        for param_file, required in [
            ("jurisdiction_prep.ini",prep_pars),
            ("run_time.ini", ["jurisdictions_dir","mungers_dir"]),
        ]:
            d, parameter_err = ui.get_parameters(
                required_keys=required,
                param_file=param_file,
                header="election_data_analysis",
            )
            self.d.update(d)
        # calculate full jurisdiction path from other info
        self.d["jurisdiction_path"] = os.path.join(self.d["jurisdictions_dir"],self.d["name"].replace(" ","-"))

        self.state_house = int(self.d["count_of_state_house_districts"])
        self.state_senate = int(self.d["count_of_state_senate_districts"])
        self.congressional = int(self.d["count_of_us_house_districts"])


def make_par_files(
    dir: str,
    munger_name: str,
    jurisdiction_path: str,
    top_ru: str,
    election: str,
    download_date: str = "1900-01-01",
    source: str = "unknown",
    results_note: str = "none",
):
    """Utility to create parameter files for multiple files. Makes a parameter file for each (non-.ini,non .*) file in <dir>,
    once all other necessary parameters are specified."""
    data_file_list = [f for f in os.listdir(dir) if (f[-4:] != ".ini") & (f[0] != ".")]
    for f in data_file_list:
        par_text = (
            f"[election_data_analysis]\nresults_file={f}\njurisdiction_path={jurisdiction_path}\n"
            f"munger_name={munger_name}\ntop_reporting_unit={top_ru}\nelection={election}\n"
            f"results_short_name={top_ru}_{f}\nresults_download_date={download_date}\n"
            f"results_source={source}\nresults_note={results_note}\n"
        )
        par_name = ".".join(f.split(".")[:-1]) + ".ini"
        with open(os.path.join(dir, par_name), "w") as p:
            p.write(par_text)
    return


class Analyzer:
    def __new__(self, param_file=None, dbname=None):
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
                required_keys=["rollup_directory"],
                param_file=param_file,
                header="election_data_analysis",
            )
        except FileNotFoundError as e:
            print(
                "Parameter file 'run_time.ini' not found. Ensure that it is located"
                " in the current directory. Analyzer object not created."
            )
            return None

        if postgres_param_err or eda_err:
            print("Parameter file missing requirements.")
            print(f"postgres: {postgres_param_err}")
            print(f"election_data_analysis: {eda_err}")
            print("Analyzer object not created.")
            return None

        return super().__new__(self)

    def __init__(self, param_file=None, dbname=None):
        if not param_file:
            param_file = "run_time.ini"

        # read rollup_directory from param_file
        d, error = ui.get_parameters(
            required_keys=["rollup_directory"],
            param_file=param_file,
            header="election_data_analysis",
        )
        self.rollup_directory = d["rollup_directory"]

        # create session
        eng, err = db.sql_alchemy_connect(param_file, dbname=dbname)
        Session = sessionmaker(bind=eng)
        self.session = Session()

    def display_options(self, input: str, verbose: bool = False, filters: list = None):
        if not verbose:
            results = db.get_input_options(self.session, input, False)
        else:
            if not filters:
                df = pd.DataFrame(db.get_input_options(self.session, input, True))
                results = db.package_display_results(df)
            else:
                try:
                    filters_mapped = ui.get_contest_type_mappings(filters)
                    results = db.get_filtered_input_options(
                        self.session, input, filters_mapped
                    )
                except:
                    results = None
        if results:
            return results
        return None

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
        subdivision_type_id = db.get_jurisdiction_hierarchy(
            self.session, jurisdiction_id
        )
        h_election_id = db.name_to_id(self.session, "Election", h_election)
        v_election_id = db.name_to_id(self.session, "Election", v_election)
        # *_type is either candidates or contests or parties
        h_type, h_count_item_type = self.split_category_input(h_category)
        v_type, v_count_item_type = self.split_category_input(v_category)
        h_count = h_count.split(" - ")[0].strip()
        v_count = v_count.split(" - ")[0].strip()

        agg_results = a.create_scatter(
            self.session,
            jurisdiction_id,
            subdivision_type_id,
            h_election_id,
            h_count_item_type,
            h_count,
            h_type,
            v_election_id,
            v_count_item_type,
            v_count,
            v_type,
        )
        if fig_type and agg_results:
            viz.plot("scatter", agg_results, fig_type, self.rollup_directory)
        return agg_results

    def bar(
        self,
        election: str,
        jurisdiction: str,
        contest_type: str = None,
        contest: str = None,
        fig_type: str = None,
    ) -> list:
        """contest_type is one of state, congressional, state-senate, state-house"""
        election_id = db.name_to_id(self.session, "Election", election)
        jurisdiction_id = db.name_to_id(self.session, "ReportingUnit", jurisdiction)
        # for now, bar charts can only handle jurisdictions where county is one level
        # down from the jurisdiction
        most_granular_id = db.name_to_id(self.session, "ReportingUnitType", "county")
        subdivision_type_id = db.get_jurisdiction_hierarchy(
            self.session, jurisdiction_id
        )
        # bar chart always at one level below top reporting unit
        agg_results = a.create_bar(
            self.session,
            jurisdiction_id,
            subdivision_type_id,
            contest_type,
            contest,
            election_id,
            False,
        )
        if fig_type and agg_results:
            for agg_result in agg_results:
                viz.plot("bar", agg_result, fig_type, self.rollup_directory)
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
        subdivision_type_id = db.get_jurisdiction_hierarchy(
            self.session, jurisdiction_id
        )
        # bar chart always at one level below top reporting unit
        agg_results = a.create_bar(
            self.session,
            jurisdiction_id,
            subdivision_type_id,
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
        election_id = db.name_to_id(self.session, "Election", election)
        err = a.create_rollup(
            self.session,
            self.rollup_directory,
            top_ru_id=rollup_unit_id,
            sub_rutype_id=sub_unit_id,
            election_id=election_id,
            by_vote_type=by_vote_type,
        )
        return err


def get_filename(path: str) -> str:
    head, tail = ntpath.split(path)
    return tail or ntpath.basename(head)


def aggregate_results(
        election,
        jurisdiction,
        dbname: Optional[str] = None,
        vote_type: Optional[str] = None,
        sub_unit: Optional[str] = None,
        contest: Optional[str] = None,
        contest_type: str = "Candidate",
        sub_unit_type: str = "county",
        exclude_redundant_total: bool = True,
):
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

    datafile_list, e = db.data_file_list(
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
        cursor=cursor,
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


def data_exists(election, jurisdiction, p_path=None, dbname=None):
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
    df = db.read_vote_count(an.session, election_id, reporting_unit_id, ["Name"],["contest_name"])
    # if no contest found
    if df.empty:
        # no data exists.
        return False
    # otherwise
    else:
        # then data exists!
        return True


def census_data_exists(election, jurisdiction, p_path=None, dbname=None):
    an = Analyzer(param_file=p_path, dbname=dbname)
    if not an:
        return False

    reporting_unit_id = db.name_to_id(an.session, "ReportingUnit", jurisdiction)

    # if the database doesn't have the reporting unit
    if not reporting_unit_id:
        # data doesn't exist
        return False

    connection = an.session.bind.raw_connection()
    cursor = connection.cursor()
    df = db.read_external(cursor, int(election[0:4]), reporting_unit_id, ["Label"])
    cursor.close()
    
    # if no contest found
    if df.empty:
        # no data exists.
        return False
    # otherwise
    else:
        # then data exists!
        return True


def check_totals_match_vote_types(
        election, 
        jurisdiction, 
        sub_unit_type="county",
        dbname=None,
):
    """Interesting if there are both total and other vote types;
    otherwise trivially true"""
    an = Analyzer(dbname=dbname)
    active = db.active_vote_types(an.session, election, jurisdiction)
    if len(active) > 1 and 'total' in active:
        # pull type 'total' only
        df_candidate = aggregate_results(
            election, 
            jurisdiction, 
            contest_type="Candidate",
            vote_type='total',
            sub_unit_type=sub_unit_type,
            exclude_redundant_total=False,
            dbname=dbname,
        )
        df_ballot = aggregate_results(
            election, 
            jurisdiction, 
            contest_type="BallotMeasure",
            vote_type='total',
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
        election,
        jurisdiction,
        contest,
        dbname: Optional[str] = None,
        vote_type: Optional[str] = None,
        county: Optional[str] = None,
        sub_unit_type: str = "county",
        contest_type: Optional[str] = "Candidate"
):
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


def count_type_total(election, jurisdiction, contest, count_item_type, sub_unit_type="county", dbname=None):
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


def check_count_types_standard(election, jurisdiction, dbname=None):
    an = Analyzer(dbname=dbname)
    standard_ct_list = db.get_input_options(an.session,'count_item_type',False)
    # don't want type "other"
    standard_ct_list.remove("other")
    active = db.active_vote_types(an.session, election, jurisdiction)
    for vt in active:
        # if even one fails, count types are not standard
        if vt not in standard_ct_list:
            return False
    # if nothing failed, count types are standard
    return True


def get_contest_with_unknown_candidates(election, jurisdiction, dbname=None) -> List[str]:
    an = Analyzer(dbname=dbname)
    if not an:
        return [f"Failure to connect to database"]
    election_id = db.name_to_id(an.session, "Election", election)
    if not election_id:
        return[f"Election {election} not found"]
    jurisdiction_id = db.name_to_id(an.session, "ReportingUnit", jurisdiction)
    if not jurisdiction_id:
        return[f"Jurisdiction {jurisdiction} not found"]

    contests = db.get_contest_with_unknown(an.session, election_id, jurisdiction_id)
    return contests


def load_results_file(
        session,
        munger_path: str,
        f_path: str,
        juris: jm.Jurisdiction,
        election_datafile_ids: dict,
        constants: Dict[str, str],
        results_directory_path,
) -> Optional[dict]:

    # TODO tech debt: redundant to pass results_directory_path and f_path
    munger_name = Path(munger_path).name
    # read parameters from munger file
    p, err = m.get_and_check_munger_params(munger_path)
    if ui.fatal_error(err):
        return err

    # read data into standard count format dataframe
    #  append "_SOURCE" to all non-Count column names
    #  (to avoid confilcts if e.g., source has col names 'Party')
    try:
        df, original_string_columns, err = m.to_standard_count_frame(
            f_path, munger_path, p, constants, suffix="_SOURCE",
        )
        if ui.fatal_error(err):
            return err

    except Exception as exc:
        err = ui.add_new_error(
            err,
            "system",
            f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
            f"Exception while converting data to standard form: {exc}"
        )
        return err

    # transform source to completely munged (possibly with foreign keys if there is aux data)
    # # add raw-munged column for each element, removing old
    try:
        df, new_err = m.munge_source_to_raw(
            df,
            munger_path,
            p,
            original_string_columns,
            "_SOURCE",
            results_directory_path,
        )
    except Exception as exc:
        err = ui.add_new_error(
            err,
            "system",
            f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
            f"Exception while munging source to raw: {exc}"
        )
        return err

    # # add columns for constant-over-file elements
    for element in constants.keys():
        df = m.add_constant_column(
            df,
            element,
            constants[element],
        )

    # # delete any rows with items to be ignored
    ig, new_err = ui.get_parameters(
        header="ignore",
        required_keys=[],
        optional_keys=[
            "Candidate",
            "CandidateContest",
            "BallotMeasureSelection",
            "BallotMeasureContest",
            "Party",
        ],
        param_file=munger_path,
    )
    if new_err:
        pass  # errors ignored, as header is not required
    real_ig_keys = [x for x in ig.keys() if ig[x] is not None]
    for element in real_ig_keys:
        value_list = ig[element].split(",")
        mask = ~df[f"{element}_raw"].isin(value_list)
        df = df[mask]

    # # add Id columns for all but Count, removing raw-munged
    try:
        df, new_err = m.munge_raw_to_ids(df, constants, juris, munger_name, session)
        if new_err:
            err = ui.consolidate_errors([err, new_err])
            if ui.fatal_error(new_err):
                return err
    except Exception as exc:
        err = ui.add_new_error(
            err,
            "system",
            f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
            f"Exception while munging raw to ids: {exc}"
        )
        return err

    # # for each contest, if none or unknown candidate has total votes 0, remove rows with that contest & candidate
    nou_candidate_id = db.name_to_id(session, "Candidate", "none or unknown")
    nou_selection_ids = db.selection_ids_from_candidate_id(session, nou_candidate_id)
    unknown = df[df.Selection_Id.isin(nou_selection_ids)].groupby(["Contest_Id","Selection_Id"]).sum()
    for (contest_id, selection_id) in unknown.index:
        mask = df[['Contest_Id', 'Selection_Id']] == (contest_id, selection_id)
        df = df[~mask.all(axis=1)]

    # add_datafile_Id and Election_Id columns
    for c in ["_datafile_Id", "Election_Id"]:
        df = m.add_constant_column(df, c, election_datafile_ids[c])
    # load counts to db
    try:
        err = m.fill_vote_count(df, session, err)
    except Exception as exc:
        err = ui.add_new_error(
            err,
            "system",
            f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
            f"Exception while filling vote count table: {exc}"
        )
        return err
    return err
