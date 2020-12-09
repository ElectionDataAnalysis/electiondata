from election_data_analysis import user_interface as ui
from election_data_analysis import juris_and_munger as jm
from election_data_analysis import munge as m
from election_data_analysis import database as db
import os
import re
import inspect
from pathlib import Path
import pandas as pd
from typing import List, Dict, Optional, Any

# TODO remove the below
"""
xls-multi mungers
../src/mungers/az_ppp/format.config:file_type=xls-multi
    New type xls: Cleaned-2020PPECanvasReportBook.xlsx 
../src/mungers/oh_pri/format.config:file_type=xls-multi
    New type xls: use master sheet republicancountyall2020.xlsx and similar for dem, lib
../src/mungers/mt_pri/format.config:file_type=xls-multi
../src/mungers/wi_pri/format.config:file_type=xls-multi
../src/mungers/oh_gen/format.config:file_type=xls-multi
../src/mungers/mt_gen/format.config:file_type=xls-multi
../src/mungers/phila_16g/format.config:file_type=xls-multi
(
"""


def count_columns_by_name(
        format_file_path: str,
        results_dir: dict,
        state: str,
        d_ini: dict,
        mu: str,
) -> Optional[str]:
    """returns comma-separated list of count columns"""
    # get file_type, count_columns, count_columns_by_name and header_row_count
    d_mu, mu_err = ui.get_parameters(
        required_keys=["file_type"],
        optional_keys=[
            "count_columns",
            "header_row_count",
            "field_name_row",
            "count_of_top_lines_to_skip",
            "count_columns",
            "count_columns_by_name",
        ],
        header="format",
        param_file=format_file_path
    )
    if d_mu["count_columns"] is not None:
        pass
    # if we can get column names from file
    elif (
            d_mu["file_type"] in ["csv", "txt", "txt-semicolon-separated"]
            and
            int(d_mu["header_row_count"]) > 0
    ):
        if d_mu["file_type"] == "csv":
            sep = ","
        elif d_mu["file_type"] == "txt":
            sep = "\t"
        elif d_mu["file_type"] == "txt-semicolon-separated":
            sep = ";"

        # read the header rows from the dataframe
        rf_path = os.path.join(
            results_dir[d_ini["election"]],
            d_ini["results_file"],
        )
        if d_mu["count_of_top_lines_to_skip"] is None:
            skiprows = 0
        else:
            skiprows = int(d_mu["count_of_top_lines_to_skip"])
        try:
            df = pd.read_csv(
                rf_path,
                sep=sep,
                nrows=int(d_mu["header_row_count"]),
                skiprows=skiprows,
            )
            # print(f"File processed: {rf_path}")
            # capture the names of the count_columns
            count_cols_by_name_list = [
                df.columns[int(x)]
                for x in d_mu["count_columns"].split(",")
                if int(x) < df.shape[1]
            ]
            return ",".join(count_cols_by_name_list)
        except FileNotFoundError:
            print(f"File not found: {rf_path}")
    # if we get count col names from munger
    elif d_mu["count_columns_by_name"] is not None:
        return d_mu["count_columns_by_name"]
    else:
        print(f"Count columns needed but not found for munger {mu}")
        return None


def count_fields(
        ini_dir: str,
        mungers_dir: str,
        results_dir: dict,
        err: Optional[dict] = None,
) -> dict:
    # loop through .ini files; build dictionary of `count_fields` lists from `count_columns` lists.
    cf = dict()
    for state in os.listdir(ini_dir):
        state_dir = os.path.join(ini_dir, state)
        if os.path.isdir(state_dir):
            for f in [x for x in os.listdir(state_dir) if x[-4:] == ".ini"]:
                # read necessary info from .ini file
                d_ini, err = ui.get_parameters(
                    required_keys=["results_file", "munger_name", "election"],
                    header="election_data_analysis",
                    param_file=os.path.join(state_dir, f),
                    err=err,
                )
                # loop through mungers mentioned
                for mu in d_ini["munger_name"].split(","):
                    # get count columns by name
                    count_cols_by_name_str = count_columns_by_name(
                        os.path.join(mungers_dir, mu, "format.config"),
                        results_dir,
                        state,
                        d_ini,
                        mu,
                    )

                    # create/update count_fields for mu
                    if count_cols_by_name_str is not None:
                        if mu in cf.keys():
                            if count_cols_by_name_str not in cf[mu]:
                                cf[mu].append(count_cols_by_name_str)
                        else:
                            cf[mu] = [count_cols_by_name_str]
    # check for problematic ones
    for mu in cf.keys():
        if len(cf[mu]) > 1:
            print(f"Munger {mu} has more than one set of count fields")
    return cf


def create_munger_files(
        ini_dir,
        old_mungers_dir,
        new_mungers_dir,
        results_dir,
        munger_list = None,
) -> dict:
    err = None

    count_field_dict = count_fields(ini_dir, old_mungers_dir, results_dir)
    if not os.path.isdir(new_mungers_dir):
        os.mkdir(new_mungers_directory)

    if munger_list:
        mungers_to_make = [x for x in os.listdir(old_mungers_dir) if x in munger_list]
    else:
        mungers_to_make = os.listdir(old_mungers_dir)
    for mu in mungers_to_make:
        mu_dir = os.path.join(old_mungers_dir, mu)
        # for each genuine munger
        if os.path.isdir(mu_dir):

            # initialize dict to hold info to be written
            new_sections = dict()

            # get contents of format.config
            d, err = ui.get_parameters(
                required_keys=jm.munger_pars_req,
                optional_keys=jm.munger_pars_opt,
                header="format",
                param_file=os.path.join(mu_dir, "format.config"),
                err=err,
            )
            # get contents of cdf_elements
            cdf_elements_df = pd.read_csv(os.path.join(mu_dir, "cdf_elements.txt"), sep="\t")

            # initialize format header
            new_sections["format"] = ["[format]"]
            # copy simple parameters
            for param in ["encoding", "thousands_separator"]:
                if d[param] is not None:
                    new_sections["format"].append(f"{param}={d[param]}")

            # set file_type and related params
            if d["file_type"] == "csv":
                new_sections["format"].append(f"file_type=flat_text")
                new_sections["format"].append(f"sep=,")
            elif d["file_type"] == "txt":
                new_sections["format"].append(f"file_type=flat_text")
                new_sections["format"].append(f"sep=\t")
            elif d["file_type"] == "txt-semicolon-separated":
                new_sections["format"].append(f"file_type=flat_text")
                new_sections["format"].append(f"sep=;")
            elif d["file_type"] == "xls":
                new_sections["format"].append(f"file_type=excel")
            elif d["file_type"] == "xls-multi":
                new_sections["format"].append(f"file_type=excel")
                new_sections["format"].append(f"sheets_to_skip={d['sheets_to_skip']}")
            elif d["file_type"] in ["json-nested", "xml"]:
                new_sections["format"].append(f"file_type={d['file_type']}")

            # set count_locations and related params
            if d["count_columns"] is not None:
                new_sections["format"].append(f"count_locations=by_column_number")
                new_sections["format"].append(f"count_column_numbers={d['count_columns']}")
            elif mu in count_field_dict.keys():
                new_sections["format"].append(f"count_locations=by_field_name")
                new_sections["format"].append(f"count_fields={count_field_dict[mu]}")
            elif d["field_names_if_no_field_name_row"] is not None:
                new_sections["format"].append(f"count_locations=by_column_number")
                new_sections["format"].append(f"count_column_numbers={d['count_columns']}")

            # set string_location and related params 
            str_locations = []
            if {"row", "xml"}.intersection(set(cdf_elements_df["source"].unique())):
                str_locations.append('from_field_values')
            if {"column"}.issubset(set(cdf_elements_df["source"].unique())):
                str_locations.append("in_count_headers")
            if {"ini"}.issubset(set(cdf_elements_df["source"].unique())):
                str_locations.append("constant_over_file")
            if (d["file_type"] == "xls-multi"
                    and
                    cdf_elements_df["raw_identifier_formula"].str.contains("constant_line").any()):
                str_locations.append("constant_over_sheet")
            str_l_str = ",".join(set(str_locations))
            new_sections["format"].append(f"string_locations={str_l_str}")

            # set rows to skip
            if d["count_of_top_lines_to_skip"]:
                new_sections["format"].append(f"rows_to_skip={d['count_of_top_lines_to_skip']}")

            # note if all rows of a flat file contain only data (not field names)
            if d["field_names_if_no_field_name_row"]:
                new_sections["format"].append("missing=field_names")

            # create other parameter sections as needed
            if "from_field_values" in str_locations:
                # initialize the section
                new_sections["from_field_values"] = ["[from_field_values]"]
                # fill the section
                for idx, r in cdf_elements_df.iterrows():
                    if r["source"] in ["row", "xml"]:
                        new_sections["from_field_values"].append(f"{r['name']}={r['raw_identifier_formula']}")
            if "in_count_headers" in str_locations:
                # initialize the section
                new_sections["in_count_headers"] = ["[in_count_headers]"]
                # fill the section
                for idx, r in cdf_elements_df.iterrows():
                    if r["source"] == "column":
                        # TODO replace, e.g., <0> by <header_0>
                        p = re.compile(r"<(\d)>")
                        form = re.sub(p, r"<header_\1>", r['raw_identifier_formula'])
                        new_sections["in_count_headers"].append(f"{r['name']}={form}")
            if "constant_over_file" in str_locations:
                # create the corresponding parameter
                constant_list = list(
                    cdf_elements_df[cdf_elements_df["source"] == "ini"]["name"].unique()
                )
                cl = ",".join(constant_list)
                new_sections["format"].append(f"constant_over_file={cl}")
            if "constant_over_sheet" in str_locations:
                # initialize the section
                new_sections["constant_over_sheet"] = ["[constant_over_sheet]"]
                # fill the section
                if mu == "wi_pri":
                    new_sections["constant_over_sheet"] = ["CandidateContest=<row_4><row_5>"]

            # write to new munger file
            section_strings = ["\n".join(new_sections[k]) for k in new_sections.keys()]
            file_string = "\n\n".join(section_strings)
            new_mu_file_path = os.path.join(new_mungers_dir, f"{mu}.munger")
            with open(new_mu_file_path, "w") as f:
                f.write(file_string)
    return err


def create_ini_files(
        ini_directory,
        ini_list: Optional[List[str]] = None,
) -> Optional[dict]:
    err = None

    if not os.path.isdir(ini_directory):
        return {"ini_directory": "not found"}

    contest_name_pattern = re.compile(r"\nContest=(.+)\n")
    contest_type_pattern = re.compile(r"\ncontest_type=(\w+)\s*\n")
    for state in [x for x in os.listdir(ini_directory) if "." not in x]:
        state_directory = os.path.join(ini_directory,state)
        if ini_list:
            inis_to_do = [x for x in os.listdir(state_directory) if x in ini_list]
        else:
            inis_to_do = os.listdir(state_directory)
        for ini in inis_to_do:
            # for each genuine ini
            if ini[-4:] == ".ini":
                ini_path = os.path.join(state_directory, ini)
                # get contents of *.ini
                with open(ini_path, "r") as fi:
                    old_contents = fi.read()

                try:
                # get contest type
                    contest_type = contest_type_pattern.findall(old_contents)[0]
                    contest_name = contest_name_pattern.findall(old_contents)[0]

                    new_contents = old_contents.replace(
                        f"\ncontest_type={contest_type}",""
                    ).replace(
                        f"Contest={contest_name}",f"{contest_type}Contest={contest_name}"
                    )

                    # write to new ini file
                    with open(ini_path, "w") as f:
                        f.write(new_contents)
                except:
                    pass
    return err


req_munger_params: Dict[str, str] = {
    "file_type": "string",
    "count_locations": "string",
    "string_locations": "list-of-strings",
}

opt_munger_params: Dict[str, str] = {
    "sheets_to_read_names": "list-of-strings",
    "sheets_to_skip": "list-of-strings",
    "sheets_to_read_numbers": "list-of-integers",
    "sheets_to_skip_numbers": "list-of-integers",
    "rows_to_skip": "integer",
    "flat_file_delimiter": "string",
    "quoting": "string",
    "thousands_separator": "string",
    "encoding": "string",
    "count_fields_by_name": "list-of-strings",
    "count_field_name_row": "int",  # TODO allow multi-rows here?
    "count_column_numbers": "list-of-integers",
    "count_header_row_numbers": "list-of-integers",
    "string_field_names": "list-of-strings",
    "string_field_name_row": "int",
    "auxiliary_data_location": "string",
    "all_rows": "string",
    "constant_over_file": "list-of-strings",
}

munger_dependent_reqs: Dict[str, Dict[str, List[str]]] = {
    "file_type": {"flat_text": ["flat_file_delimiter"]},
    "count_locations": {
        "by_field_name": ["count_fields_by_name"],
        "by_column_number": ["count_column_numbers"],
    },
}

req_munger_param_values: Dict[str, List[str]] = {
    "string_locations": ["from_field_values", "in_count_headers", "constant_over_file", "constant_over_sheet", "auxiliary_data"],
    "count_locations": ["by_field_name", "by_column_number"],
    "file_type": ["excel", "json-nested", "xml", "flat_text"],
}

string_location_reqs: Dict[str, List[str]] = {
    "from_field_values": ["string_field_names"],
    "in_count_headers": ["count_header_row_numbers"],
    "auxiliary_data": ["auxiliary_data_directory"],
    "constant_over_file": [],
    "constant_over_sheet": ["constant_over_sheet"],
}

all_munge_elements = [
    "BallotMeasureContest",
    "CandidateContest",
    "BallotMeasureSelection",
    "Candidate",
    "Party",
    "ReportingUnit",
    "CountItemType",
]


def munge_raw_to_ids(
        df: pd.DataFrame,
        constants: dict,
        juris: jm.Jurisdiction,
        munger_name: str,
        session,
) -> (pd.DataFrame, Optional[dict]):

    err = None
    working = df.copy()

    # add Contest_Id column and contest_type column
    if "CandidateContest" in constants.keys():
        working = m.add_constant_column(
            working,
            "Contest_Id",
            db.name_to_id(session, "Contest", constants["CandidateContest"])
        )
        working = m.add_constant_column(
            working,
            "contest_type",
            "Candidate"
        )
        working.drop("CandidateContest", axis=1, inplace=True)
    elif "BallotMeasureContest" in constants.keys():
        working = m.add_constant_column(
            working,
            "Contest_Id",
            db.name_to_id(session, "Contest", constants["BallotMeasureContest"])
        )
        working.drop("BallotMeasureContest", axis=1, inplace=True)
        working = m.add_constant_column(
            working,
            "contest_type",
            "BallotMeasure"
        )
    else:
        try:
            working, err = m.add_contest_id(working, juris, err, session)
        except Exception as exc:
            err = ui.add_new_error(
                err,
                "system",
                "munge.raw_elements_to_cdf",
                f"Unexpected exception while adding Contest_Id: {exc}",
            )
            return err
        if ui.fatal_error(err):
            return working, err

    # add all other _Ids/Other except Selection_Id
    # # for constants
    other_constants = [t for t in constants.keys() if t[-7:] != "Contest" and (t[-9:] != "Selection")]
    for element in other_constants:
        # CountItemType is the only enumeration
        if element == "CountItemType":
            enum_df = pd.read_sql_table(element, session.bind)
            one_line = pd.DataFrame([[constants[element]]],columns=[element])
            id_txt_one_line, non_standard = m.enum_col_to_id_othertext(
                one_line, element, enum_df, drop_type_col=False
            )
            for c in [f"{element}_Id", f"Other{element}"]:
                working = m.add_constant_column(
                    working,
                    c,
                    id_txt_one_line.loc[0, c]
                )
            working.drop(element, axis=1, inplace=True)
        else:
            working = m.add_constant_column(
                working,
                f"{element}_Id",
                db.name_to_id(session, element, constants[element])
            )
            working.drop(element, axis=1, inplace=True)
            working, err_df = m.clean_ids(working, ["CountItemType_Id"])
            if not err_df.empty:
                err = ui.add_new_error(
                    err,
                    "warn-munger",
                    munger_name,
                    f"Problem cleaning these Ids:\nerr_df"
                )

    other_elements = [
        t for t in all_munge_elements
        if (t[-7:] != "Contest") and (t[-9:] != "Selection") and (t not in constants.keys())
    ]
    working, new_err = m.raw_to_id_simple(working, juris, other_elements, session)
    if new_err:
        err = ui.consolidate_errors([err, new_err])
        if ui.fatal_error(new_err):
            return working, err

    # add Selection_Id (combines info from BallotMeasureSelection and CandidateContestSelection)
    try:
        working, err = m.add_selection_id(working, session.bind, juris, err)
        working, err_df = m.clean_ids(working, ["Selection_Id"])
    except Exception as exc:
        err = ui.add_new_error(
            err,
            "system",
            f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
            f"Unexpected exception while adding Selection_Id:\n{exc}",
        )
        return err
    if working.empty:
        err = ui.add_new_error(
            err,
            "jurisdiction",
            juris.short_name,
            "No contests found, or no selections found for contests.",
        )
        return err

    return working, err


def munge_source_to_raw(
        df: pd.DataFrame,
        munger_path: str,
        p: Dict[str, Any],
        orig_string_cols: List[str],
        suffix: str,
) -> (pd.DataFrame, Optional[dict]):
    """NB: assumes columns of dataframe have <suffix> appended already"""
    err = None
    if df.empty:
        return df, err
    munger_name = Path(munger_path).name
    working = df.copy()

    # # get munge formulas
    # # for all but constant-over-file
    sources = [x for x in p["string_locations"] if x != "constant_over_file"]
    for source in sources:
        formulas, new_err = ui.get_parameters(
            required_keys=[],
            optional_keys=all_munge_elements,
            header=source,
            param_file=munger_path,
        )
        elements = [k for k in formulas.keys() if formulas[k] is not None]
        for element in elements:
            try:
                formula = formulas[element]
                # append suffix to formula fields
                for c in orig_string_cols:
                    formula = formula.replace(f"<{c}>", f"<{c}{suffix}>")
                # add col with munged values
                working, new_err = m.add_column_from_formula(
                    working, formula, f"{element}_raw", err, munger_name
                )
                if new_err:
                    err = ui.consolidate_errors([err, new_err])
                    if ui.fatal_error(new_err):
                        return working, err
                # TODO how to handle disambiguation? Here's how we did it before:
                """
                # correct any disambiguated names back to the original
                if element in munger.alt.keys():
                    working.replace({f"{element}_raw": munger.alt[element]}, inplace=True)
                """
            except Exception as exc:
                err = ui.add_new_error(
                    err,
                    "munger",
                    munger_name,
                    f"Error interpreting formula for {element} in cdf_element.txt. {exc}",
                )
                return working, err
            # compress whitespace for <element>_raw
            working.loc[:, f"{element}_raw"] = working[f"{element}_raw"].apply(
                m.compress_whitespace
            )

    # drop the original columns
    working.drop([f"{x}{suffix}" for x in orig_string_cols], axis=1, inplace=True)
    return working, err


def get_and_check_munger_params(munger_path: str) -> (dict, Optional[dict]):
    params, err = ui.get_parameters(
        required_keys=list(req_munger_params.keys()),
        optional_keys=list(opt_munger_params.keys()),
        param_file=munger_path,
        header="format",
        err=None,
    )
    if ui.fatal_error(err):
        return dict(), err
    # get name of munger for error reporting
    munger_name = Path(munger_path).name[:-7]
    # define dictionary of munger parameters
    format_options = jm.recast_options(params, {**opt_munger_params, **req_munger_params})

    # Check munger values
    # # main parameters recognized
    for k in req_munger_param_values.keys():
        if req_munger_params[k] == "string":
            if not format_options[k] in req_munger_param_values[k]:
                err = ui.add_new_error(
                    err,
                    "munger",
                    munger_name,
                    f"Value of {k} must be one of these: {req_munger_param_values[k]}"
                )
        elif req_munger_params[k] == "list_of_strings":
            bad = [x for x in format_options[k] if x not in req_munger_param_values]
            if bad:
                err = ui.add_new_error(
                    err,
                    "munger",
                    munger_name,
                    f"Each listed value of {k} must be one of these: {req_munger_param_values[k]}"
                )

    # # simple non-null dependencies
    for k0 in munger_dependent_reqs.keys():
        for k1 in munger_dependent_reqs[k0]:
            for v2 in munger_dependent_reqs[k0][k1]:
                if format_options[k0] == k1 and v2 is None:
                    err = ui.add_new_error(
                        err,
                        "munger",
                        munger_name,
                        f"{k0}={k1}', but {v2} not found"
                    )

    # # extra compatibility requirements for excel or flat text files
    if (format_options["file_type"] in ["excel", "flat_text"]) :
        # # count_field_name_row is given where required
        if (format_options["count_field_name_row"] is None) and (format_options["count_locations"] == "by_field_name"):
            err = ui.add_new_error(
                err,
                "munger",
                munger_name,
                f"file_type={format_options['file_type']}' but count_field_name_row not found"
            )

        # # if all rows are not data, need field names
        if (format_options["all_rows"] is None):
            if format_options["string_field_name_row"] is None:
                err = ui.add_new_error(
                    err,
                    "munger",
                    munger_name,
                    f"file_type={format_options['file_type']}' and absence of"
                    f" all_rows=data means field names must be in the "
                    f"file. But string_field_name_row not given."
                )

    # # for each value in list of string locations, requirements are met
    for k0 in format_options["string_locations"]:
        for v2 in string_location_reqs[k0]:
            if v2 is None:
                err = ui.add_new_error(
                    err,
                    "munger",
                    munger_name,
                    f"{k0} is in list of string locations, but {v2} not found"
                )
    # TODO check formats (e.g., formulas for constant_over_sheet use only <sheet_name> and <row_{i}>
    return format_options, err


def get_string_fields(
        sources: list,
        munger_path: str,
) -> (Dict[str,List[str]], Optional[dict]):
    err = None
    pattern = re.compile(r'<([^>]+)>')
    munge_field_lists = dict()
    for source in sources:
        munge_field_set = set()
        formulas, new_err = ui.get_parameters(
            required_keys=[],
            optional_keys=all_munge_elements,
            header=source,
            param_file=munger_path,
        )
        if new_err:
            err = ui.consolidate_errors([err, new_err])
            if ui.fatal_error(new_err):
                return dict(), err

        for k in formulas.keys():
            if formulas[k]:
                munge_field_set.update(pattern.findall(formulas[k]))
        munge_field_lists[source] = list(munge_field_set)

    return munge_field_lists, err


def to_standard_count_frame(f_path: str, munger_path: str, p, constants) -> (pd.DataFrame, Optional[dict]):
    """Read data from file at <f_path>; return a standard dataframe with one clean count column
    and all other columns typed as 'string' """
    munger_name = Path(munger_path).name
    err = None

    # check that all necessary constants were passed
    if p["constant_over_file"] is not None:
        bad = [x for x in p["constant_over_file"] if x not in constants.keys()]
        if bad:
            bad_str = ",".join(bad)
            err = ui.add_new_error(
                err,
                "file",
                f_path,
                f"Required constants not given in .ini file:\n\t{bad_str}",
            )
            return pd.DataFrame(), err

    # read count dataframe(s) from file
    raw_dict, read_err = ui.read_single_datafile(p, f_path)
    if read_err:
        err = ui.consolidate_errors([err,read_err])
        if ui.fatal_error(read_err):
            return pd.DataFrame(), err

    # get lists of string fields expected in raw file
    munge_field_lists, new_err = get_string_fields(
        [x for x in p["string_locations"] if x != "constant_over_file"],
        munger_path,
    )
    if new_err:
        err = ui.consolidate_errors([err, new_err])
        if ui.fatal_error(new_err):
            return pd.DataFrame(), err
    # remove any sheets designated to be removed
    if p["sheets_to_skip"]:
        for k in p["sheets_to_skip"]:
            raw_dict.pop(k)
    # for each df:
    standard = dict()
    for k, raw in raw_dict.items():
        # transform to df with single count column 'Count' and all raw munge info in other columns
        standard[k], k_err = m.melt_to_one_count_column(raw, p, munger_name)
        if k_err:
            err = ui.consolidate_errors([err, k_err])
            if ui.fatal_error(k_err):
                return err

        # add columns for any constant-over-sheet elements
        if "constant_over_sheet" in p["string_locations"]:
            # see if <sheet_name> is needed
            if "sheet_name" in munge_field_lists["constant_over_sheet"]:
                standard[k] = m.add_constant_column(standard[k], "sheet_name", k)
            # find max row needed
            try:
                rows_needed = [int(var[4:]) for var in munge_field_lists["constant_over_sheet"] if var != "sheet_name"]
                if rows_needed:
                    max_row = max(rows_needed)
                    data = pd.read_excel(f_path, nrows=max_row+1)
                    for row in rows_needed:
                        standard[k] = m.add_constant_column(
                            standard[k],
                            f"row_{row}",
                            data.loc[row,data.loc[row].first_valid_index()],
                        )
            except ValueError as ve:
                err = ui.add_new_error(
                    err,
                    "munger",
                    munger_name,
                    f"Ill-formed reference to row of file in munger formulas in {ve}"
                )
                return pd.DataFrame(), err
            except KeyError() as ke:
                variables = ",".join(munge_field_lists["constant_over_sheet"])
                err = ui.add_new_error(
                    err,
                    "file",
                    Path(f_path).name,
                    f"No data found for one of these: \n\t{variables}",
                )
                return pd.DataFrame(), err

        # keep only necessary columns
        necessary = [item for sublist in munge_field_lists.values() for item in sublist] + ["Count"]
        standard[k] = standard[k][necessary]

        # clean Count column
        standard[k], bad_rows = m.clean_count_cols(standard[k], ["Count"], p["thousands_separator"])
        if not bad_rows.empty:
            err = ui.add_err_df(err, bad_rows, munger_name, f_path)

    df = pd.concat(standard.values())

    non_count = [c for c in df.columns if c != "Count"]
    df[non_count] = df[non_count].astype("string")

    return df, err


def fill_vote_count(
        df: pd.DataFrame,
        session,
        err: Optional[dict],
) -> Optional[dict]:

    working = df.copy()
    # restrict to just the VoteCount columns (so that groupby.sum will work)
    vc_cols = [
        "Count",
        "CountItemType_Id",
        "OtherCountItemType",
        "ReportingUnit_Id",
        "Contest_Id",
        "Selection_Id",
        "Election_Id",
        "_datafile_Id",
    ]
    working = working[vc_cols]

    # add CountItemType total if it's not already there
    working = m.ensure_total_counts(working, session)
    # TODO there are edge cases where this might include dupes
    #  that should be omitted. E.g., if data mistakenly read twice
    # Sum any rows that were disambiguated (otherwise dupes will be dropped
    #  when VoteCount is filled)
    group_cols = [c for c in working.columns if c != "Count"]
    working = working.groupby(group_cols).sum().reset_index()

    # Fill VoteCount
    try:
        err_str = db.insert_to_cdf_db(session.bind, working, "VoteCount")
        if err_str:
            err = ui.add_new_error(
                err,
                "system",
                f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
                f"database insertion error {err_str}",
            )
            return err
    except Exception as exc:
        err = ui.add_new_error(
            err,
            "system",
            f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
            f"Error filling VoteCount:\n{exc}",
        )

    return err


def load_results_file(
        session,
        munger_path: str,
        f_path: str,
        juris: jm.Jurisdiction,
        election_datafile_ids: dict,
        constants: Dict[str, str],
) -> Optional[dict]:

    # TODO complete this routine
    munger_name = Path(munger_path).name
    # read parameters from munger file
    p, err = get_and_check_munger_params(munger_path)
    if ui.fatal_error(err):
        return err

    # read data into standard count format dataframe
    df, err = to_standard_count_frame(f_path, munger_path, p, constants)
    if ui.fatal_error(err):
        return err
    # TODO what if returned df is empty?

    # append "_SOURCE" to all non-Count column names (to avoid confilcts if e.g., source has col names 'Party'
    original_string_columns = [c for c in df.columns if c != "Count"]
    df.columns = [c if c == "Count" else f"{c}_SOURCE" for c in df.columns]

    # transform source to completely munged (possibly with foreign keys if there is aux data)
    # # add raw-munged column for each element, removing old
    df, new_err = munge_source_to_raw(
        df,
        munger_path,
        p,
        original_string_columns,
        "_SOURCE",
    )

    # # add columns for constant-over-file elements
    for element in constants.keys():
        df = m.add_constant_column(
            df,
            element,
            constants[element],
        )

    # # add Id columns for all but Count, removing raw-munged
    df, new_err = munge_raw_to_ids(df, constants, juris, munger_name, session)
    if new_err:
        err = ui.consolidate_errors([err, new_err])
        if ui.fatal_error(new_err):
            return err
    # #  TODO replace any foreign keys with true values
    # add_datafile_Id and Election_Id columns
    for c in ["_datafile_Id", "Election_Id"]:
        df = m.add_constant_column(df, c, election_datafile_ids[c])
    # load counts to db
    err = fill_vote_count(df, session, err)
    return err


if __name__ == "__main__":
    ini_directory = '/Users/singer3/PycharmProjects/election_data_analysis/src/ini_files_for_results'
    old_mungers_directory = '/Users/singer3/PycharmProjects/election_data_analysis/src/mungers'
    new_mungers_directory = '/Users/singer3/PycharmProjects/election_data_analysis/src/mungers_new'
    g_drive = '/Users/singer3/PycharmProjects/election_data_analysis/tests/TestingData/'
    results_directory = {
        "2020 General": os.path.join(g_drive, "_000-Final-2020-General", "archived"),
        "2020 Primary": os.path.join(g_drive, "_001-Final-2020-Primary", "Processed"),
        "2018 General": os.path.join(g_drive, "_002-Final-2018-General", "Processed"),
        "2016 General": os.path.join(g_drive, "_004-Final-2016-General", "Processed"),
        "2011 Primary": os.path.join(g_drive, "_009-Final-2011-Primary")
    }

    """    aa = count_fields(ini_directory, old_mungers_directory, results_directory)
    """
    error = create_munger_files(
        ini_directory,
        old_mungers_directory,
        old_mungers_directory,
        results_directory,
        munger_list=["wi_gen20"]
    )

    err = create_ini_files(ini_directory)



    results_path = "/Users/singer3/PycharmProjects/election_data_analysis/tests/TestingData/_000-Final-2020-General/archived/Wisconsin/UNOFFICIAL WI Election Results 2020 by County 11-5-2020.xlsx"
    juris_path = "/Users/singer3/PycharmProjects/election_data_analysis/src/jurisdictions/Wisconsin"
    mu_path = "/Users/singer3/PycharmProjects/election_data_analysis/src/mungers_new_by_hand/wi_gen20.munger"
    cons = {
        "Party": "Democratic Party",
        "CandidateContest": "US President (WI)",
        "CountItemType": "total",
    }
    results_info = {"_datafile_Id": 909, "Election_Id": 1352}
"""
    juris, juris_err = ui.pick_juris_from_filesystem(
        juris_path=juris_path,
        err=None,
        check_files=False,
    )

    dl = e.DataLoader()

    jurs_load_err = juris.load_juris_to_db(dl.session)

    load_error = load_results_file(dl.session, mu_path, results_path, juris, results_info, cons)
"""
exit()
