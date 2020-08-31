from election_anomaly import database as db
from election_anomaly import user_interface as ui
from election_anomaly import munge as m
from sqlalchemy.orm import sessionmaker
import datetime
from pathlib import Path
import os
import pandas as pd
from pprint import pprint
import sys
import ntpath
from election_anomaly import analyze as a
from election_anomaly import visualize as v
from election_anomaly import juris_and_munger as jm
from election_anomaly import preparation as prep

# constants
single_data_loader_pars = [
    "jurisdiction_path",
    "munger_name",
    "results_file",
    "results_short_name",
    "results_download_date",
    "results_source",
    "results_note",
    "top_reporting_unit",
    "election",
    "aux_data_dir",
]

multi_data_loader_pars = [
    "project_root",
    "db_paramfile",
    "db_name",
    "results_dir",
    "archive_dir",
]

prep_pars = [
    "project_root",
    "jurisdiction_path",
    "name",
    "abbreviated_name",
    "count_of_state_house_districts",
    "count_of_state_senate_districts",
    "count_of_us_house_districts",
    "reporting_unit_type",
]

optional_prep_pars = ["results_file", "munger_name"]

analyze_pars = ["db_paramfile", "db_name"]

# classes
class DataLoader:
    def __new__(self):
        """Checks if parameter file exists and is correct. If not, does
        not create DataLoader object."""
        try:
            d, parameter_err = ui.get_runtime_parameters(
                multi_data_loader_pars, param_file="multi.par"
            )
        except FileNotFoundError as e:
            print(
                "Parameter file multi.par not found. Ensure that it is located"
                " in the current directory. DataLoader object not created."
            )
            return None

        if parameter_err:
            print("Parameter file missing requirements.")
            print(parameter_err)
            print("DataLoader object not created.")
            return None

        return super().__new__(self)

    def __init__(self):
        # grab parameters
        self.d, self.parameter_err = ui.get_runtime_parameters(
            multi_data_loader_pars, param_file="multi.par"
        )

        # prepare to track files loaded, dictionary of dictionaries, keys are parameter file paths
        self.tracker = dict()

        # create db if it does not already exist
        error = db.establish_connection(
            paramfile=self.d["db_paramfile"], db_name=self.d["db_name"]
        )
        if error:
            db.create_new_db(
                self.d["project_root"], self.d["db_paramfile"], self.d["db_name"]
            )

        # connect to db
        try:
            self.engine = db.sql_alchemy_connect(
                paramfile=self.d["db_paramfile"], db_name=self.d["db_name"]
            )
            Session = sessionmaker(bind=self.engine)
            self.session = Session()
        except Exception as e:
            print("Cannot connect to database. Exiting.")
            quit()

    def load_all(self, load_jurisdictions: bool = True) -> dict:
        """returns a dictionary of any files that threw an error"""
        mungers_path = os.path.join(self.d["project_root"], "mungers")

        # list .par files and pull their jurisdiction_paths
        par_files = [f for f in os.listdir(self.d["results_dir"]) if f[-4:] == ".par"]
        params = dict()
        param_err = dict()
        juris_path = dict()
        for f in par_files:
            # grab parameters
            par_file = os.path.join(self.d["results_dir"], f)
            params[f], param_err[f] = ui.get_runtime_parameters(
                single_data_loader_pars,
                optional_keys=["aux_data_dir"],
                param_file=par_file,
            )
            juris_path[f] = params[f]["jurisdiction_path"]
            # update file_tracker
            self.tracker[f] = params[f]
            self.tracker[f]["status"] = "not loaded"
            if param_err[f]:
                self.tracker["parameter_error"] = param_err[f]

        err = dict()
        # group .par files by jurisdiction_path
        jurisdiction_paths = {juris_path[f] for f in par_files}
        files = dict()
        for jp in jurisdiction_paths:
            if load_jurisdictions:
                juris, juris_err = ui.pick_juris_from_filesystem(
                    jp, self.d["project_root"], check_files=True
                )
                if juris is None:
                    err["jurisdiction_error"] = juris_err
                    return err
                print(f"Loading jurisdiction from {jp} to {self.session.bind}")
                if juris:
                    juris_load_err = juris.load_juris_to_db(
                        self.session, self.d["project_root"]
                    )
                    if juris_load_err:
                        err["juris_load_error"] = juris_load_err
                        return err
            # process all files from the given jurisdiction
            files[jp] = [f for f in par_files if juris_path[f] == jp]
            print(f"Processing results files {files[jp]}")
            for f in files[jp]:
                sdl = SingleDataLoader(
                    self.d["results_dir"],
                    f,
                    self.d["project_root"],
                    self.session,
                    mungers_path,
                    juris,
                )
                errors = sdl.check_errors()
                if errors == (None, None):
                    self.tracker[f]["status"] = "loading initialized"
                    # try to load data
                    load_error = sdl.load_results()
                    self.move_loaded_results_file(sdl, f, load_error)
                    if load_error:
                        err[f] = load_error
                        self.tracker[f]["load_error"] = load_error
                    else:
                        self.tracker[f]["status"] = "loaded"
                else:
                    self.tracker[f]["status"] = "loading not initialized"
                    print("Error(s) before data loading:")
                    if sdl.parameter_err:
                        print(f"Parameter error: {sdl.parameter_err}\n")
                        self.tracker[f][
                            "SingleDataLoader_parameter_error"
                        ] = sdl.parameter_err
                    if sdl.munger_err:
                        print(f"Munger error: {sdl.munger_err}")
                        self.tracker[f]["munger_error"] = sdl.munger_err
        return err

    def move_loaded_results_file(self, sdl, f: str, load_error: dict):
        warnings = []
        errors = []
        for mu in load_error.keys():
            if load_error[mu]:
                warn_err_keys = [k for k in load_error[mu].keys()].copy()
            else:
                warn_err_keys = []
            for k in warn_err_keys:
                msg = load_error[mu].pop(k)
                if "warning" in k or "Warning" in k:
                    print(f"Warning ({mu}): {msg}")
                    warnings.append(msg)
                elif "error" in k:
                    print(f"Error ({mu}): {msg}")
                    errors.append(msg)
        if errors:
            self.tracker[f]["status"] = "loading failed"
            err_str = "\n\t".join(errors)
            if warnings:
                ws = "\n\t".join(warnings)
                warn_str = f"\nWarnings:\n{ws}"
            else:
                warn_str = None
            # save errors in current directory
            warn_file = os.path.join(self.d["results_dir"], f"{f[:-4]}.errors")
            with open(warn_file, "w") as wf:
                wf.write(f"Errors:\n\t{err_str}\n\nWarnings:\n\t{warn_str}")
            print(
                f"Fatal errors found. Results not loaded; file not moved. See {f[:-4]}.errors"
            )

        else:
            # move results file and its parameter file to a subfolder of the archive directory
            #  named for the db
            self.tracker[f]["status"] = "loaded"
            new_dir = os.path.join(self.d["archive_dir"], self.d["db_name"])
            ui.archive(f, self.d["results_dir"], new_dir)
            ui.archive(sdl.d["results_file"], self.d["results_dir"], new_dir)
            print_str = f"\tArchived {f} and its results file."
            if warnings:
                # save warnings in archive directory
                warn_file = os.path.join(new_dir, f"{f[:-4]}.warn")
                with open(warn_file, "w") as wf:
                    wf.write("\n".join(warnings))
                print_str += f" See warnings in {f[:-4]}."
            print(print_str)
        return


class SingleDataLoader:
    def __init__(
        self, results_dir, par_file_name, project_root, session, munger_path, juris
    ):
        # adopt passed variables needed in future as attributes
        self.project_root = project_root
        self.session = session
        self.results_dir = results_dir
        self.juris = juris
        # grab parameters
        par_file = os.path.join(results_dir, par_file_name)
        self.d, self.parameter_err = ui.get_runtime_parameters(
            single_data_loader_pars, optional_keys=["aux_data_dir"], param_file=par_file
        )

        # change any blank parameters to 'none'
        for k in self.d.keys():
            if self.d[k] == "" and k[:8] == "results_":
                self.d[k] = "none"

        # convert comma-separated list to python list
        # TODO document
        self.munger_list = [x.strip() for x in self.d["munger_name"].split(",")]

        # set aux_data_dir to None if appropriate
        if self.d["aux_data_dir"] in ["None", ""]:
            self.d["aux_data_dir"] = None

        # pick mungers
        self.munger = dict()
        self.munger_err = dict()
        for mu in self.munger_list:
            self.munger[mu], self.munger_err[mu] = ui.pick_munger(
                mungers_dir=munger_path, project_root=project_root, munger_name=mu
            )
        # if no munger throws an error:
        if all([x is None for x in self.munger_err.values()]):
            self.munger_err = None

    def check_errors(self):
        return self.parameter_err, self.munger_err

    def track_results(self):
        filename = self.d["results_file"]
        top_reporting_unit_id = db.name_to_id(
            self.session, "ReportingUnit", self.d["top_reporting_unit"]
        )
        election_id = db.name_to_id(self.session, "Election", self.d["election"])

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
        e = db.insert_to_cdf_db(self.session.bind, data, "_datafile")
        if e:
            return [0, 0], e
        else:
            col_map = {"short_name": "short_name"}
            datafile_id = db.append_id_to_dframe(
                self.session.bind, data, "_datafile", col_map=col_map
            ).iloc[0]["_datafile_Id"]
        return [datafile_id, election_id], e

    def load_results(self) -> dict:
        print(f'Processing {self.d["results_file"]}')
        results_info, e = self.track_results()
        if e:
            err = {"database": e}
        else:
            err = dict()
            for mu in self.munger_list:
                f_path = os.path.join(self.results_dir, self.d["results_file"])
                emu = ui.new_datafile(
                    self.session,
                    self.munger[mu],
                    f_path,
                    self.project_root,
                    self.juris,
                    results_info=results_info,
                    aux_data_dir=self.d["aux_data_dir"],
                )
                if emu != dict():
                    err[mu] = emu
        if err == dict():
            err = None
        return err


class JurisdictionPrepper:
    def __new__(cls):
        """Checks if parameter file exists and is correct. If not, does
        not create JurisdictionPrepper object."""
        param_file = "jurisdiction_prep.par"
        try:
            d, parameter_err = ui.get_runtime_parameters(
                prep_pars, param_file="jurisdiction_prep.par"
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

    def new_juris_files(self, other_districts: dict = None):
        """<juris_path> identifies the directory where the files will live.
        <abbr> is the two-letter abbreviation for state/district/territory.
        <state_house>, etc., gives the number of districts;
        <other_districts> is a dictionary of other district names, types & counts, e.g.,
        {'Circuit Court':{'ReportingUnitType':'judicial','count':5}}
        """
        # TODO Feature: allow other districts to be set in paramfile
        error = dict()
        # create directory if it doesn't exist
        jm.ensure_jurisdiction_dir(
            self.d["jurisdiction_path"], self.d["project_root"], ignore_empty=True
        )

        # add default entries
        templates = os.path.join(
            self.d["project_root"], "templates/jurisdiction_templates"
        )
        for element in ["Party", "Election"]:
            prep.add_defaults(self.d["jurisdiction_path"], templates, element)

        # add all standard Offices/RUs/CandidateContests
        self.add_standard_contests()

        # Feature create starter dictionary.txt with cdf_internal name
        #  used as placeholder for raw_identifier_value
        e = self.starter_dictionary()
        if e:
            ui.add_erro(error, "dictionary", e)
        return error

    def add_primaries_to_dict(self) -> str:
        error = None
        # TODO add real error handling
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
        prep.write_element(self.d["jurisdiction_path"], "dictionary", new_dictionary)
        return error

    def add_standard_contests(
        self, juriswide_contests: list = None, other_districts: dict = None
    ):
        """If <juriswide_contest> is None, use standard list hard-coded."""
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
            ]
        # append jurisdiction-wide offices
        jw_off = pd.DataFrame(
            [[x, self.d["name"]] for x in juriswide_contests], columns=cols_off
        )
        w_office = w_office.append(jw_off, ignore_index=True)

        # append jurisdiction-wide contests
        jw_cc = pd.DataFrame(
            [[x, 1, x, ""] for x in juriswide_contests], columns=cols_cc
        )
        w_cc = w_cc.append(jw_cc, ignore_index=True)

        prep.write_element(
            self.d["jurisdiction_path"], "Office", w_office.drop_duplicates()
        )
        prep.write_element(
            self.d["jurisdiction_path"], "ReportingUnit", w_ru.drop_duplicates()
        )
        prep.write_element(
            self.d["jurisdiction_path"], "CandidateContest", w_cc.drop_duplicates()
        )
        return

    def add_primaries_to_candidate_contest(self):
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
        prep.write_element(
            self.d["jurisdiction_path"],
            "CandidateContest",
            pd.concat([contests] + all_primaries),
        )
        return error

    def add_sub_county_rus_from_results_file(
        self,
        error: dict = None,
        sub_ru_type: str = "precinct",
        results_file_path=None,
        munger_name=None,
        **kwargs,
    ) -> dict:
        """Assumes precincts (or other sub-county reporting units)
        are munged from row of the results file.
        Adds corresponding rows to ReportingUnit.txt and dictionary.txt
        using internal County name correctly"""

        # get parameters from arguments; otherwise from self.d; otherwise throw error
        kwargs, missing = ui.get_params_to_read_results(
            self.d, results_file_path, munger_name
        )
        if missing:
            ui.add_error(
                error,
                "datafile",
                f"Parameters missing: {missing}. Results file cannot be processed.",
            )
            return error

        # read data from file (appending _SOURCE)
        wr, munger, error = ui.read_results(kwargs, error)

        if wr.empty:
            ui.add_error(
                error, "datafile", f"No results read from file. Parameters: {kwargs}"
            )
            return error

        # reduce <wr> in size
        fields = [
            f"{field}_SOURCE"
            for field in munger.cdf_elements.loc["ReportingUnit", "fields"]
        ]
        wr = wr[fields].drop_duplicates()

        # get rid of all-blank rows
        wr = wr[(wr != "").any(axis=1)]
        if wr.empty:
            ui.add_error(
                error,
                "datafile",
                f"No relevant information read from file. Parameters: {kwargs}",
            )
            return error

        # get formulas from munger
        ru_formula = munger.cdf_elements.loc["ReportingUnit", "raw_identifier_formula"]
        try:
            [county_formula, sub_ru_formula] = ru_formula.split(";")
        except ValueError:
            ui.add_error(
                error,
                "munge_error",
                f"ReportingUnit formula in munger {munger.name} has wrong format (should have two parts separated by ;)",
            )
            return error

        # add columns for county and sub_ru
        wr, error = m.add_column_from_formula(
            wr, county_formula, "County_raw", error, suffix="_SOURCE"
        )
        wr, error = m.add_column_from_formula(
            wr, sub_ru_formula, "Sub_County_raw", error, suffix="_SOURCE"
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
        prep.write_element(
            self.d["jurisdiction_path"], "ReportingUnit", pd.concat([ru_old, ru_add])
        )

        # add info to dictionary
        wr.rename(columns={"Name": "cdf_internal_name"}, inplace=True)
        dict_add = wr[["cdf_element", "cdf_internal_name", "raw_identifier_value"]]
        prep.write_element(
            self.d["jurisdiction_path"],
            "dictionary",
            pd.concat([ru_dict_old, dict_add]),
        )
        return error

    def add_sub_county_rus_from_multi_results_file(
        self, dir: str, error: dict = None, sub_ru_type: str = "precinct"
    ) -> dict:
        """Adds all elements in <elements> to <element>.txt and, naively, to <dictionary.txt>
        for each file in <dir> named (with munger) in a .par file in the directory"""
        if not error:
            error = dict()
        for par_file_name in [x for x in os.listdir(dir) if x[-4:] == ".par"]:
            par_file = os.path.join(dir, par_file_name)
            file_dict, missing_params = ui.get_runtime_parameters(
                ["results_file", "munger_name"],
                optional_keys=["aux_data_dir"],
                param_file=par_file,
            )
            file_dict["sub_ru_type"] = sub_ru_type
            file_dict["results_file_path"] = os.path.join(
                dir, file_dict["results_file"]
            )
            if missing_params:
                ui.add_error(
                    error,
                    "parameter_file",
                    f"Parameters missing from {par_file_name}:{missing_params}",
                )
            else:
                error = self.add_sub_county_rus_from_results_file(error, **file_dict)
        return error

    def add_elements_from_multi_results_file(
        self, elements: iter, dir: str, error: dict
    ):
        """Adds all elements in <elements> to <element>.txt and, naively, to <dictionary.txt>
        for each file in <dir> named (with munger) in a .par file in the directory"""
        for par_file_name in [x for x in os.listdir(dir) if x[-4:] == ".par"]:
            par_file = os.path.join(dir, par_file_name)
            file_dict, missing_params = ui.get_runtime_parameters(
                ["results_file", "munger_name"],
                optional_keys=["aux_data_dir"],
                param_file=par_file,
            )
            file_dict["results_file_path"] = os.path.join(
                dir, file_dict["results_file"]
            )
            if missing_params:
                ui.add_error(
                    error,
                    "parameter_file",
                    f"Parameters missing from {par_file_name}:{missing_params}",
                )
            else:
                error = self.add_elements_from_results_file(
                    elements, error, **file_dict
                )
        return error

    def add_elements_from_results_file(
        self,
        elements: iter,
        error: dict,
        results_file_path=None,
        munger_name=None,
        **kwargs,
    ) -> dict:
        """Add lines in dictionary.txt and <element>.txt corresponding to munged names not already in dictionary
        or not already in <element>.txt for each <element> in <elements>"""

        # get parameters from arguments; otherwise from self.d; otherwise throw error
        # get parameters from arguments; otherwise from self.d; otherwise throw error
        kwargs, missing = ui.get_params_to_read_results(
            self.d, results_file_path, munger_name
        )
        if missing:
            ui.add_error(
                error,
                "datafile",
                f"Parameters missing: {missing}. Results file cannot be processed.",
            )
            return error

        # read data from file (appending _SOURCE)
        wr, mu, error = ui.read_results(kwargs, error)

        for element in elements:
            name_field = db.get_name_field(element)
            # append <element>_raw
            wr, error = m.add_munged_column(
                wr,
                mu,
                element,
                error,
                mode=mu.cdf_elements.loc[element, "source"],
                inplace=False,
            )
            if error:
                return error
            # find <element>_raw values not in dictionary.txt.raw_identifier_value;
            #  add corresponding lines to dictionary.txt
            wd = prep.get_element(self.d["jurisdiction_path"], "dictionary")
            old_raw = wd[wd.cdf_element == element]["raw_identifier_value"].to_list()
            new_raw = [x for x in wr[f"{element}_raw"] if x not in old_raw]
            new_raw_df = pd.DataFrame(
                [[element, x, x] for x in new_raw],
                columns=["cdf_element", "cdf_internal_name", "raw_identifier_value"],
            )
            wd = pd.concat([wd, new_raw_df]).drop_duplicates()
            prep.write_element(self.d["jurisdiction_path"], "dictionary", wd)

            # find cdf_internal_names that are not in <element>.txt and add them to <element>.txt
            we = prep.get_element(self.d["jurisdiction_path"], element)
            old_internal = we[name_field].to_list()
            new_internal = [
                x
                for x in wd[wd.cdf_element == element]["cdf_internal_name"]
                if x not in old_internal
            ]
            # TODO guide user to check dictionary for bad stuff before running this
            #  e.g., primary contests already in dictionary cause a problem.
            new_internal_df = pd.DataFrame(
                [[x] for x in new_internal], columns=[name_field]
            )
            we = pd.concat([we, new_internal_df]).drop_duplicates()
            prep.write_element(self.d["jurisdiction_path"], element, we)
            # if <element>.txt has columns other than <name_field>, notify user
            if we.shape[1] > 1 and not new_internal_df.empty:
                ui.add_error(
                    error,
                    "preparation",
                    f"New rows added to {element}.txt, but data may be missing from some fields in those rows.",
                )
        return error

    def starter_dictionary(self, include_existing=True) -> str:
        """Creates a starter file for dictionary.txt, assuming raw_identifiers are the same as cdf_internal names.
        Puts file in the current directory"""
        w = dict()
        elements = [
            "BallotMeasureContest",
            "Candidate",
            "CandidateContest",
            "Election",
            "Office",
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
            prep_pars,
            optional_keys=optional_prep_pars,
            param_file="jurisdiction_prep" ".par",
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
    aux_data_dir: str = "",
):
    """Utility to create parameter files for multiple files. Makes a parameter file for each (non-.par,non .*) file in <dir>,
    once all other necessary parameters are specified."""
    data_file_list = [f for f in os.listdir(dir) if (f[-4:] != ".par") & (f[0] != ".")]
    for f in data_file_list:
        par_text = f"[election_anomaly]\nresults_file={f}\njurisdiction_path={jurisdiction_path}\nmunger_name={munger_name}\ntop_reporting_unit={top_ru}\nelection={election}\nresults_short_name={top_ru}_{f}\nresults_download_date={download_date}\nresults_source={source}\nresults_note={results_note}\naux_data_dir={aux_data_dir}\n"
        par_name = ".".join(f.split(".")[:-1]) + ".par"
        with open(os.path.join(dir, par_name), "w") as p:
            p.write(par_text)
    return


class Analyzer:
    def __new__(self):
        """Checks if parameter file exists and is correct. If not, does
        not create DataLoader object."""
        try:
            d, parameter_err = ui.get_runtime_parameters(
                ["db_paramfile", "db_name", "results_file"]
            )
        except FileNotFoundError as e:
            print(
                "Parameter file not found. Ensure that it is located"
                " in the current directory. Analyzer object not created."
            )
            return None

        if parameter_err:
            print("Parameter file missing requirements.")
            print(parameter_err)
            print("Analyzer object not created.")
            return None

        return super().__new__(self)

    def __init__(self):
        self.d, self.parameter_err = ui.get_runtime_parameters(
            ["db_paramfile", "db_name", "results_file"]
        )
        self.d["results_file_short"] = get_filename(self.d["results_file"])

        eng = db.sql_alchemy_connect(
            paramfile=self.d["db_paramfile"], db_name=self.d["db_name"]
        )
        Session = sessionmaker(bind=eng)
        self.session = Session()

    def display_options(self, input: str, verbose: bool=False, filters: list=None):
        if not verbose:
            results = db.get_input_options(self.session, input, False)
        else:
            if not filters:
                df = pd.DataFrame(db.get_input_options(self.session, input, True))
                results = db.package_display_results(df)
            else:
                results = db.get_filtered_input_options(self.session, input, filters)
        if results:
            return results
        return None

    def top_counts_by_vote_type(self, election: str, rollup_unit: str, sub_unit: str) -> str:
        d, error = ui.get_runtime_parameters(
            ["rollup_directory"], param_file="multi.par"
        )
        if error:
            err_str = (
                f"Parameter file missing requirements.\n{error}\nNo results exported"
            )
            print(err_str)
        else:
            connection = self.session.bind.raw_connection()
            cursor = connection.cursor()
            rollup_unit_id = db.name_to_id(self.session, "ReportingUnit", rollup_unit)
            sub_unit_id = db.name_to_id(self.session, "ReportingUnitType", sub_unit)
            election_id = db.name_to_id(self.session, "Election", election)
            err_str = a.create_rollup(
                cursor, d["rollup_directory"], rollup_unit_id, sub_unit_id, election_id
            )
            connection.close()
        return err_str

    def top_counts(self, rollup_unit: str , sub_unit: str):
        d, error = ui.get_runtime_parameters(["rollup_directory"])
        if error:
            print("Parameter file missing requirements.")
            print(error)
            print("Data not created.")
            return
        else:
            rollup_unit_id = db.name_to_id(self.session, "ReportingUnit", rollup_unit)
            sub_unit_id = db.name_to_id(self.session, "ReportingUnitType", sub_unit)
            results_info = db.get_datafile_info(
                self.session, self.d["results_file_short"]
            )
            rollup = a.create_rollup(
                self.session,
                d["rollup_directory"],
                top_ru_id=rollup_unit_id,
                sub_rutype_id=sub_unit_id,
                sub_rutype_othertext="",
                datafile_id_list=results_info[0],
                election_id=results_info[1],
                by_vote_type=False,
            )
            return

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
            ["rollup_directory"], param_file="analyze.par"
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
        h_count_item_type, h_type = self.split_category_input(h_category)
        v_count_item_type, v_type = self.split_category_input(v_category)
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
        if fig_type:
            v.plot("scatter", agg_results, fig_type, d["rollup_directory"])
        return agg_results

    def bar(
        self,
        jurisdiction: str, 
        contest_type: str = None, 
        contest: str = None, 
        fig_type: str = None
    ) -> list:
        """contest_type is one of state, congressional, state-senate, state-house"""
        d, error = ui.get_runtime_parameters(
            ["rollup_directory", "sub_reporting_unit_type"], param_file="analyze.par"
        )
        if error:
            print("Parameter file missing requirements.")
            print(error)
            print("Data not created.")
            return
        jurisdiction_id = db.name_to_id(self.session, "ReportingUnit", jurisdiction)
        most_granular_id = db.name_to_id(
            self.session, "ReportingUnitType", d["sub_reporting_unit_type"]
        )
        hierarchy = db.get_jurisdiction_hierarchy(
            self.session, jurisdiction_id, most_granular_id
        )
        results_info = db.get_datafile_info(self.session, self.d["results_file_short"])
        # bar chart always at one level below top reporting unit
        agg_results = a.create_bar(
            self.session,
            jurisdiction_id,
            hierarchy[1],
            contest_type,
            contest,
            results_info[1],
            False,
        )
        if fig_type:
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

    def export_outlier_data(self, jurisdiction: str, contest: str=None):
        """contest_type is one of state, congressional, state-senate, state-house"""
        d, error = ui.get_runtime_parameters(
            ["rollup_directory", "sub_reporting_unit_type"], param_file="analyze.par"
        )
        if error:
            print("Parameter file missing requirements.")
            print(error)
            print("Data not created.")
            return
        jurisdiction_id = db.name_to_id(self.session, "ReportingUnit", jurisdiction)
        most_granular_id = db.name_to_id(
            self.session, "ReportingUnitType", d["sub_reporting_unit_type"]
        )
        hierarchy = db.get_jurisdiction_hierarchy(
            self.session, jurisdiction_id, most_granular_id
        )
        results_info = db.get_datafile_info(self.session, self.d["results_file_short"])
        # bar chart always at one level below top reporting unit
        agg_results = a.create_bar(
            self.session,
            jurisdiction_id,
            hierarchy[1],
            None,
            contest,
            results_info[1],
            True,
        )
        return agg_results


def get_filename(path: str) -> str:
    head, tail = ntpath.split(path)
    return tail or ntpath.basename(head)
