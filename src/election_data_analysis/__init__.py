from election_data_analysis import database as db
from election_data_analysis import user_interface as ui
from election_data_analysis import munge as m
from sqlalchemy.orm import sessionmaker
from typing import List, Optional
import datetime
import os
import pandas as pd
import ntpath
import inspect
from pathlib import Path
from election_data_analysis import analyze as a
from election_data_analysis import visualize as v
from election_data_analysis import juris_and_munger as jm
from election_data_analysis import preparation as prep

# constants
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
]

multi_data_loader_pars = [
    "results_dir",
    "archive_dir",
    "jurisdictions_dir",
    "mungers_dir",
]

prep_pars = [
    "mungers_dir",
    "jurisdiction_path",
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
        d, err = ui.get_runtime_parameters(
            required_keys=multi_data_loader_pars,
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
        self.d, self.parameter_err = ui.get_runtime_parameters(
            required_keys=multi_data_loader_pars,
            param_file="run_time.ini",
            header="election_data_analysis",
        )

        # create db if it does not already exist and have right tables
        ok, err = db.test_connection()
        if not ok:
            db.create_new_db()

        # connect to db
        try:
            self.engine, new_err = db.sql_alchemy_connect()
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

    def load_all(
        self, load_jurisdictions: bool = True, move_files: bool = False
    ) -> dict:
        """returns an error dictionary"""
        # initialize error dictionary
        err = None

        # set locations for error reporting
        # TODO get rid of mungers_path variable, use self.d directly
        mungers_path = self.d["mungers_dir"]

        # define directory for archiving successfully loaded files (and storing warnings)
        db_param, new_err = ui.get_runtime_parameters(
            required_keys=["dbname"], param_file="run_time.ini", header="postgresql"
        )
        if new_err:
            err = ui.consolidate_errors([err, new_err])
        if ui.fatal_error(new_err):
            err = ui.report(err)
            return err

        # specify directories for archiving and reporting warnings
        success_dir = os.path.join(self.d["archive_dir"], db_param["dbname"])
        loc_dict = {
            "munger": self.d["results_dir"],
            "jurisdiction": self.d["results_dir"],
            "warn-munger": success_dir,
            "warn-jurisdiction": success_dir,
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
            return err

        params = dict()
        juris_directory = dict()

        # For each par_file get params or throw error
        good_par_files = list()
        for f in par_files:
            # grab parameters
            par_file = os.path.join(self.d["results_dir"], f)
            params[f], new_err = ui.get_runtime_parameters(
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
                good_par_files.append(f)
                juris_directory[f] = params[f]["jurisdiction_directory"]

        # group .ini files by jurisdiction_directory name
        jurisdiction_dirs = {juris_directory[f] for f in good_par_files}

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
                    new_err = juris[jp].load_juris_to_db(
                        self.session,
                    )
                    if new_err:
                        err = ui.consolidate_errors([err, new_err])
                    if not ui.fatal_error(new_err):
                        good_jurisdictions.append(jp)
                # if not asked to load jurisdiction, assume it's loaded
                else:
                    print(
                        f"Jurisdiction {juris[jp].name} assumed to be loaded to database already"
                    )
                    good_jurisdictions.append(jp)

        # process all good parameter files with good jurisdictions
        for jp in good_jurisdictions:
            good_files = [f for f in good_par_files if juris_directory[f] == jp]
            print(f"Processing results files {good_files}")
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

                # if no fatal error from SDL initialization, continue
                if not ui.fatal_error(new_err):
                    # try to load data
                    load_error = sdl.load_results()
                    if load_error:
                        err = ui.consolidate_errors([err, load_error])

                    # if no fatal load error, archive files

                    if move_files and not ui.fatal_error(load_error):
                        ui.archive(f, self.d["results_dir"], success_dir)
                        ui.archive(
                            sdl.d["results_file"], self.d["results_dir"], success_dir
                        )
                        print(
                            f"\tArchived {f} and its results file after successful load.\n"
                        )
                    elif move_files and ui.fatal_error(load_error):
                        print(f"\t{f} and its results file not archived due to errors")

                    else:
                        print(f"{f} and its results file loaded successfully.")
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
            "warn-munger": success_dir,
            "warn-jurisdiction": success_dir,
        }
        ui.report(err, loc_dict)
        return err


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

        # grab parameters (known to exist from __new__, so can ignore error variable)
        par_file = os.path.join(results_dir, par_file_name)
        self.d, dummy_err = ui.get_runtime_parameters(
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

        # change any blank parameters describing the results file to 'none'
        for k in self.d.keys():
            if self.d[k] == "" and k[:8] == "results_":
                self.d[k] = "none"

        # pick mungers (Note: munger_name is comma-separated list of munger names)
        self.munger = dict()
        self.munger_err = dict()
        # TODO document
        self.munger_list = [x.strip() for x in self.d["munger_name"].split(",")]

        # initialize each munger (or collect error)
        m_err = dict()
        for mu in self.munger_list:
            self.munger[mu], m_err[mu] = jm.check_and_init_munger(
                os.path.join(mungers_path, mu)
            )

        self.munger_err = ui.consolidate_errors([m_err[mu] for mu in self.munger_list])

    def track_results(self) -> (List[int], Optional[str]):
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
            ],
        )
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
            return [0, 0], f"Error inserting record to _datafile table or retrieving _datafile_Id: {exc}"
        return [datafile_id, election_id], e

    def load_results(self) -> dict:
        """Load results, returning error (or None, if load successful)"""
        err = None
        print(f'\n\nProcessing {self.d["results_file"]}')
        results_info, e = self.track_results()
        if e:
            err = ui.add_new_error(
                err,
                "system",
                "SingleDataLoader.load_results",
                f"Error inserting _datafile record:\n{e}"
                f" "
            )
            return err

        else:
            if self.d["aux_data_dir"] is None:
                aux_data_path = None
            else:
                aux_data_path = os.path.join(self.results_dir, self.d["aux_data_dir"])
            for mu in self.munger_list:
                f_path = os.path.join(self.results_dir, self.d["results_file"])
                new_err = ui.new_datafile(
                    self.session,
                    self.munger[mu],
                    f_path,
                    self.juris,
                    results_info=results_info,
                    aux_data_path=aux_data_path,
                )
                if new_err:
                    err = ui.consolidate_errors([err, new_err])
        return err


def check_aux_data_setup(
    params, aux_data_dir_parent, mungers_path, par_file_name
) -> dict:

    err = None
    # if aux_data_dir is given
    if "aux_data_dir" in params.keys() and params["aux_data_dir"] is not None:
        # check that it is a bona fide subdirectory of the results directory
        if not os.path.isdir(os.path.join(aux_data_dir_parent, params["aux_data_dir"])):
            # TODO test this error
            err = ui.add_new_error(
                err,
                "ini",
                par_file_name,
                f"Specified aux_data_dir ({params['aux_data_dir']}) is not a subdirectory of {aux_data_dir_parent}",
            )
            sdl = None
            return sdl, err
    # and if aux_data_dir is not given
    else:
        # check that no munger expects an aux_data_dir
        for m_name in params["munger_name"].split(","):
            if os.path.isfile(os.path.join(mungers_path, m_name, "aux_meta.txt")):
                err = ui.add_new_error(
                    err,
                    "ini",
                    par_file_name,
                    f"Munger {m_name} has an aux_meta.txt file, "
                    f"indicating that an auxiliary data directory is expected, but "
                    f"no aux_data_dir is given",
                )
    return err


def check_and_init_singledataloader(
    results_dir: str,
    par_file_name: str,
    session,
    mungers_path: str,
    juris: jm.Jurisdiction,
) -> (SingleDataLoader, dict):
    """Return SDL if it could be successfully initialized, and
    error dictionary (including munger errors noted in SDL initialization)"""
    # test parameters
    par_file = os.path.join(results_dir, par_file_name)
    d, err = ui.get_runtime_parameters(
        required_keys=sdl_pars_req,
        optional_keys=sdl_pars_opt,
        param_file=par_file,
        header="election_data_analysis",
    )
    if err:
        sdl = None
        return sdl, err

    # check consistency of munger and .ini file regarding aux data
    new_err = check_aux_data_setup(d, results_dir, mungers_path, par_file_name)

    if ui.fatal_error(new_err):
        err = ui.consolidate_errors([err, new_err])
        sdl = None
        return sdl, err

    ##################
    # for backward compatibility
    if ("jurisdiction_directory" not in d.keys()) and (
        "jurisdiction_path" not in d.keys()
    ):
        sdl = None
        err = ui.add_new_error(
            dict(),
            "ini",
            par_file_name,
            f"Neither jurisdiction_directory nor jurisdiction_path specified",
        )
        sdl = None
        return sdl, err
    ######################

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
        param_file = "jurisdiction_prep.ini"
        try:
            d, parameter_err = ui.get_runtime_parameters(
                required_keys=prep_pars,
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

    def add_primaries_to_dict(self) -> dict:
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

    def add_primaries_to_candidate_contest(self):

        print(f"\nStarting {inspect.currentframe().f_code.co_name}")

        primaries = {}
        error = None

        # get contests that are not already primaries
        contests = prep.get_element(self.d["jurisdiction_path"], "CandidateContest")
        # TODO might have to check for '' as well as nulls
        non_p_contests = contests[contests["PrimaryParty"].isnull()]
        if non_p_contests.empty:
            error = "CandidateContest.txt is missing or has no non-primary contests. No primary contests added."
            return error

        # get parties
        parties = prep.get_element(self.d["jurisdiction_path"], "Party")
        if parties.empty:
            if error:
                error += "\n Party.txt is missing or empty. No primary contests added."
            else:
                error = "\n Party.txt is missing or empty. No primary contests added."
            return error

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
        error = ui.consolidate_errors([error, new_err])
        return error

    def add_sub_county_rus_from_results_file(
        self,
        results_file_path,
        munger_path,
        aux_data_path=None,
        sub_ru_type: str = "precinct",
        error: dict = None,
    ) -> dict:
        """Assumes precincts (or other sub-county reporting units)
        are munged from row of the results file.
        Adds corresponding rows to ReportingUnit.txt and dictionary.txt
        using internal County name correctly"""

        munger_name = Path(munger_path).name

        # read data from file (appending _SOURCE)
        wr, munger, new_err = ui.read_results(
            results_file_path=results_file_path,
            munger_path=munger_path,
            aux_data_path=aux_data_path,
            error=None,
        )
        if new_err:
            error = ui.consolidate_errors([error, new_err])
            if ui.fatal_error(new_err):
                return error

        if wr.empty:
            error = ui.add_new_error(
                error,
                "file",
                Path(results_file_path).name,
                f"No results read via munger {Path(munger_path).name}",
            )
            return error

        # clean the dataframe read from the results
        wr, err_df = m.generic_clean(wr)
        # reduce <wr> in size
        fields = [
            f"{field}_SOURCE"
            for field in munger.cdf_elements.loc["ReportingUnit", "fields"]
        ]
        # TODO generalize to mungers with aux_data
        bad_fields = [f for f in fields if f not in wr.columns]
        if bad_fields:
            error = ui.add_new_error(
                error,
                "munger",
                munger.name,
                f"\n(ignore _SOURCE suffix below)\n"
                f"ReportingUnit formula fields not found in file ({Path(results_file_path).name}): "
                f"{bad_fields}\n"
                f"file fields are:\n{wr.columns}",
            )
            return error
        wr = wr[fields].drop_duplicates()

        # get rid of all-blank rows
        wr = wr[(wr != "").any(axis=1)]
        if wr.empty:
            error = ui.add_new_error(
                error,
                "file",
                Path(results_file_path).name,
                f"No relevant information read from results file.",
            )
            return error

        # get formulas from munger
        ru_formula = munger.cdf_elements.loc["ReportingUnit", "raw_identifier_formula"]
        try:
            # text up to first ; is the County -- the part following is the sub_ru
            ru_parts = ru_formula.split(";")
            county_formula = ru_parts[0]
            sub_list = ru_parts[1:]
            sub_ru_formula = ";".join(sub_list)

        except ValueError:
            error = ui.add_new_error(
                error,
                "munger",
                munger.name,
                f"ReportingUnit formula has wrong format for adding sub-county units. "
                f"If counties and sub-county units can be identified by this munger, "
                f"they should be separated by ;",
            )
            return error

        # add columns for county and sub_ru
        wr, error = m.add_column_from_formula(
            wr, county_formula, "County_raw", error, munger_name, suffix="_SOURCE"
        )
        wr, error = m.add_column_from_formula(
            wr, sub_ru_formula, "Sub_County_raw", error, munger_name, suffix="_SOURCE"
        )

        # add column for county internal name
        ru_dict_old = prep.get_element(self.d["jurisdiction_path"], "dictionary")
        ru_dict_new = ru_dict_old[ru_dict_old.cdf_element == "ReportingUnit"]
        wr = wr.merge(
            ru_dict_new,
            how="left",
            left_on="County_raw",
            right_on="raw_identifier_value",
        ).rename(columns={"cdf_internal_name": "County_internal"})

        # add required new columns
        wr = m.add_constant_column(wr, "ReportingUnitType", sub_ru_type)
        wr = m.add_constant_column(wr, "cdf_element", "ReportingUnit")
        wr["Name"] = wr.apply(
            lambda x: f'{x["County_internal"]};{x["Sub_County_raw"]}', axis=1
        )
        wr["raw_identifier_value"] = wr.apply(
            lambda x: f'{x["County_raw"]};{x["Sub_County_raw"]}', axis=1
        )

        # add info to ReportingUnit.txt
        ru_add = wr[["Name", "ReportingUnitType"]]
        ru_old = prep.get_element(self.d["jurisdiction_path"], "ReportingUnit")
        new_err = prep.write_element(
            self.d["jurisdiction_path"], "ReportingUnit", pd.concat([ru_old, ru_add])
        )
        if new_err:
            error = ui.consolidate_errors([error, new_err])
            if ui.fatal_error(new_err):
                return error

        # add info to dictionary
        wr.rename(columns={"Name": "cdf_internal_name"}, inplace=True)
        dict_add = wr[["cdf_element", "cdf_internal_name", "raw_identifier_value"]]
        new_err = prep.write_element(
            self.d["jurisdiction_path"],
            "dictionary",
            pd.concat([ru_dict_old, dict_add]),
        )
        error = ui.consolidate_errors([error, new_err])
        return error

    def add_sub_county_rus_from_multi_results_file(
        self, dir: str, error: dict = None, sub_ru_type: str = "precinct"
    ) -> dict:
        """For each .ini file in <dir>, finds specified results file.
        For each results file, adds all elements in <elements> to <element>.txt and, naively, to <dictionary.txt>
        for each file in <dir> named (with munger) in a .ini file in the directory"""
        print(f"\nStarting {inspect.currentframe().f_code.co_name}")

        environment_d, new_err = ui.get_runtime_parameters(
            required_keys=["mungers_dir"],
            param_file="run_time.ini",
            header="election_data_analysis",
            err=None,
        )
        if new_err:
            error = ui.consolidate_errors([error, new_err])
            if ui.fatal_error(new_err):
                return error

        for par_file_name in [x for x in os.listdir(dir) if x[-4:] == ".ini"]:
            par_file = os.path.join(dir, par_file_name)
            d, new_err = ui.get_runtime_parameters(
                required_keys=sdl_pars_req,
                param_file=par_file,
                header="election_data_analysis",
                err=None,
                optional_keys=sdl_pars_opt,
            )
            if new_err:
                error = ui.consolidate_errors([error, new_err])
                if ui.fatal_error(new_err):
                    return error

            # set aux_data_path
            if "aux_data_dir" in d.keys() and d["aux_data_dir"] is not None:
                aux_data_path = os.path.join(dir, d["aux_data_dir"])
            else:
                aux_data_path = None

            for m_name in d["munger_name"].split(","):
                new_err = self.add_sub_county_rus_from_results_file(
                    error=None,
                    sub_ru_type=sub_ru_type,
                    results_file_path=os.path.join(dir, d["results_file"]),
                    munger_path=os.path.join(environment_d["mungers_dir"], m_name),
                    aux_data_path=aux_data_path,
                )
                if new_err:
                    error = ui.consolidate_errors([error, new_err])
        ui.report(error)
        return error

    def add_elements_from_multi_results_file(
        self, elements: iter, dir: str, error: dict
    ) -> dict:
        """Adds all elements in <elements> to <element>.txt and, naively, to <dictionary.txt>
        for each file in <dir> named (with munger) in a .ini file in the directory"""

        print(f"\nStarting {inspect.currentframe().f_code.co_name}")
        # get path to mungers directory
        environment_d, new_err = ui.get_runtime_parameters(
            required_keys=["mungers_dir"],
            param_file="run_time.ini",
            header="election_data_analysis",
            err=None,
        )
        if new_err:
            ui.consolidate_errors([error, new_err])

        for par_file_name in [x for x in os.listdir(dir) if x[-4:] == ".ini"]:
            # pull parameters for results file
            par_file = os.path.join(dir, par_file_name)
            d, new_err = ui.get_runtime_parameters(
                required_keys=["results_file", "munger_name"],
                header="election_data_analysis",
                optional_keys=["aux_data_dir"],
                param_file=par_file,
                err=None,
            )
            if new_err:
                error = ui.consolidate_errors([error, new_err])

            if not ui.fatal_error(error):
                if d["aux_data_dir"] is None:
                    aux_data_path = None
                else:
                    aux_data_path = os.path.join(dir, d["aux_data_dir"])
                # loop through mungers in the "munger_name" list
                for m_name in d["munger_name"].split(","):
                    # add elements
                    new_err = self.add_elements_from_results_file(
                        elements=elements,
                        results_file_path=os.path.join(dir, d["results_file"]),
                        munger_path=os.path.join(environment_d["mungers_dir"], m_name),
                        aux_data_path=aux_data_path,
                        error=None,
                    )
                    error = ui.consolidate_errors([error, new_err])
        ui.report(error)
        return error

    def add_elements_from_results_file(
        self,
        elements: iter,
        results_file_path: str,
        munger_path: str,
        aux_data_path: str = None,
        error: dict = None,
    ) -> dict:
        """For a single munger, add lines in dictionary.txt and <element>.txt
        corresponding to munged names not already in dictionary
        or not already in <element>.txt for each <element> in <elements>"""

        # read data from file (appending _SOURCE)
        wr, mu, new_err = ui.read_results(
            results_file_path=results_file_path,
            munger_path=munger_path,
            aux_data_path=aux_data_path,
            error=dict(),
        )
        if new_err:
            error = ui.consolidate_errors([error, new_err])
            if ui.fatal_error(new_err):
                return error

        for element in elements:
            name_field = db.get_name_field(element)
            # append <element>_raw column
            w_new, new_err = m.add_munged_column(
                wr,
                mu,
                element,
                error,
                mode=mu.cdf_elements.loc[element, "source"],
                inplace=False,
            )
            if new_err:
                error = ui.consolidate_errors([error, new_err])
                if ui.fatal_error(new_err):
                    return error

            # get set of name_field values from results file
            names_from_results = w_new[f"{element}_raw"].unique()

            # delete any named '""' and warn user
            if "\"\"" in names_from_results:
                names_from_results.remove("\"\"")
                error = ui.add_new_error(
                    error,
                    "warn-file",
                    results_file_path,
                    f"An {element} named '\"\"' was found in the file and ignored. If you want it in {element}.txt "
                    f"or dictionary.txt, you will have to add it by hand."
                )

            # change any double double-quotes to single quotes; remove enclosing double-quotes
            names_from_results = [x.replace("\"\"","'").strip("\"") for x in names_from_results]


            # find <element>_raw values not in dictionary.txt.raw_identifier_value;
            #  add corresponding lines to dictionary.txt
            wd = prep.get_element(self.d["jurisdiction_path"], "dictionary")
            old_raw = wd[wd.cdf_element == element]["raw_identifier_value"].to_list()
            new_raw = [x for x in names_from_results if x not in old_raw]
            new_raw_df = pd.DataFrame(
                [[element, x, x] for x in new_raw],
                columns=["cdf_element", "cdf_internal_name", "raw_identifier_value"],
            )
            wd = pd.concat([wd, new_raw_df]).drop_duplicates()
            new_err = prep.write_element(self.d["jurisdiction_path"], "dictionary", wd)
            if new_err:
                error = ui.consolidate_errors([error, new_err])
                if ui.fatal_error(new_err):
                    return error

            # find cdf_internal_names that are not in <element>.txt and add them to <element>.txt
            we = prep.get_element(self.d["jurisdiction_path"], element)
            old_internal = we[name_field].to_list()
            new_internal = [
                x
                for x in wd[wd.cdf_element == element]["cdf_internal_name"]
                if x not in old_internal
            ]

            new_internal_df = pd.DataFrame(
                [[x] for x in new_internal], columns=[name_field]
            )
            we = pd.concat([we, new_internal_df]).drop_duplicates()
            new_err = prep.write_element(self.d["jurisdiction_path"], element, we)
            if new_err:
                ui.consolidate_errors([error, new_err])
                if ui.fatal_error(new_err):
                    return error
            # if <element>.txt has columns other than <name_field>, notify user
            if we.shape[1] > 1 and not new_internal_df.empty:
                error = ui.add_new_error(
                    error,
                    "warn-jurisdiction",
                    Path(self.d["jurisdiction_path"]).name,
                    f"Check {element}.txt for new rows missing data in some fields.",
                )
        return error

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
        self.d, self.parameter_err = ui.get_runtime_parameters(
            required_keys=prep_pars,
            optional_keys=optional_prep_pars,
            param_file="jurisdiction_prep.ini",
            header="election_data_analysis",
            err=None,
        )
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
    def __new__(self, param_file=None):
        """Checks if parameter file exists and is correct. If not, does
        not create DataLoader object."""
        try:
            if not param_file:
                param_file = "run_time.ini"
            d, parameter_err = ui.get_runtime_parameters(
                required_keys=["dbname"],
                param_file=param_file,
                header="postgresql",
            )
        except FileNotFoundError as e:
            print(
                "Parameter file 'run_time.ini' not found. Ensure that it is located"
                " in the current directory. Analyzer object not created."
            )
            return None

        if parameter_err:
            print("Parameter file missing requirements.")
            print(parameter_err)
            print("Analyzer object not created.")
            return None

        return super().__new__(self)

    def __init__(self, param_file=None):
        if param_file:
            self.param_file = param_file
        else:
            self.param_file = "run_time.ini"
        eng, err = db.sql_alchemy_connect(self.param_file)
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
                    results = db.get_filtered_input_options(self.session, input, filters)
                except:
                    results = None
        if results:
            return results
        return None

    def scatter(
        self,
        jurisdiction: str,
        subdivision_type: str,
        h_election: str,
        h_category: str,
        h_count: str,  # horizontal axis params
        v_election: str,
        v_category: str,
        v_count: str,  # vertical axis params
        fig_type: str = None,
    ) -> list:
        """Used to create a scatter plot based on selected inputs. The fig_type parameter
        is used when the user wants to actually create the visualization; this uses plotly
        so any image extension that is supported by plotly is usable here. Currently supports
        html, png, jpeg, webp, svg, pdf, and eps. Note that some filetypes may need plotly-orca
        installed as well."""
        d, error = ui.get_runtime_parameters(
            required_keys=["rollup_directory"],
            param_file=self.param_file,
            header="election_data_analysis",
        )
        if error:
            print("Parameter file missing requirements.")
            print(error)
            print("Data not created.")
            return
        jurisdiction_id = db.name_to_id(self.session, "ReportingUnit", jurisdiction)
        subdivision_type_id = db.name_to_id(
            self.session, "ReportingUnitType", subdivision_type
        )
        h_election_id = db.name_to_id(self.session, "Election", h_election)
        v_election_id = db.name_to_id(self.session, "Election", v_election)
        # *_type is either candidates or contests
        h_count_item_type, h_type = self.split_category_input(h_category)
        v_count_item_type, v_type = self.split_category_input(v_category)
        if h_count == "All Candidates" or h_count == "All Contests":
            h_count_id = -1
        elif h_type == "candidates":
            h_count_id = db.name_to_id(self.session, "Candidate", h_count)
        elif h_type == "contests":
            h_count_id = db.name_to_id(self.session, "CandidateContest", h_count)
        if v_count == "All Candidates" or v_count == "All Contests":
            v_count_id = -1
        elif v_type == "candidates":
            v_count_id = db.name_to_id(self.session, "Candidate", v_count)
        elif v_type == "contests":
            v_count_id = db.name_to_id(self.session, "CandidateContest", v_count)
        agg_results = a.create_scatter(
            self.session,
            jurisdiction_id,
            subdivision_type_id,
            h_election_id,
            h_count_item_type,
            h_count_id,
            h_type,
            v_election_id,
            v_count_item_type,
            v_count_id,
            v_type,
        )
        if fig_type and agg_results:
            v.plot("scatter", agg_results, fig_type, d["rollup_directory"])
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
        d, error = ui.get_runtime_parameters(
            required_keys=["rollup_directory"],
            param_file=self.param_file,
            header="election_data_analysis",
        )
        if error:
            print("Parameter file missing requirements.")
            print(error)
            print("Data not created.")
            return
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
                v.plot("bar", agg_result, fig_type, d["rollup_directory"])
        return agg_results

    def split_category_input(self, input_str: str):
        """Helper function. Takes an input from the front end that is the cartesian
        product of the CountItemType and {'Candidate', 'Contest'}. So something like:
        Total Candidates or Absentee Contests. Cleans this and returns
        something usable for the system to identify what the user is asking for."""
        count_item_types = self.display_options("count_item_type")
        count_item_type = [
            count_type for count_type in count_item_types if count_type in input_str
        ][0]
        selection_type = input_str[len(count_item_type) + 1 :]
        return count_item_type, selection_type

    def export_outlier_data(
        self,
        election: str,
        jurisdiction: str,
        contest: str = None,
    ) -> list:
        """contest_type is one of state, congressional, state-senate, state-house"""
        d, error = ui.get_runtime_parameters(
            required_keys=["rollup_directory"],
            param_file=self.param_file,
            header="election_data_analysis",
        )
        if error:
            print("Parameter file missing requirements.")
            print(error)
            print("Data not created.")
            return
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
    ) -> str:
        d, error = ui.get_runtime_parameters(
            required_keys=["rollup_directory"],
            param_file=self.param_file,
            header="election_data_analysis",
        )
        if error:
            print("Parameter file missing requirements.")
            print(error)
            print("Data not created.")
            return
        else:
            rollup_unit_id = db.name_to_id(self.session, "ReportingUnit", rollup_unit)
            sub_unit_id = db.name_to_id(self.session, "ReportingUnitType", sub_unit)
            election_id = db.name_to_id(self.session, "Election", election)
            err = a.create_rollup(
                self.session,
                d["rollup_directory"],
                top_ru_id=rollup_unit_id,
                sub_rutype_id=sub_unit_id,
                election_id=election_id,
                by_vote_type=by_vote_type,
            )
            return err


def get_filename(path: str) -> str:
    head, tail = ntpath.split(path)
    return tail or ntpath.basename(head)
