from election_data_analysis import user_interface as ui
from election_data_analysis import juris_and_munger as jm
from election_data_analysis import munge as m
import os
from pathlib import Path
import pandas as pd
from typing import List, Dict, Optional

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
                new_sections["format"].append(f"count_columns={d['count_columns']}")
            elif mu in count_field_dict.keys():
                new_sections["format"].append(f"count_locations=by_field_name")
                new_sections["format"].append(f"count_fields={count_field_dict[mu]}")
            elif d["field_names_if_no_field_name_row"] is not None:
                new_sections["format"].append(f"count_locations=by_column_number")
                new_sections["format"].append(f"count_columns={d['count_columns']}")

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
                new_sections["format"].append(f"rows_to_skip={d['rows_to_skip']}")

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
                        new_sections["in_count_headers"].append(f"{r['name']}={r['raw_identifier_formula']}")
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
                for idx, r in cdf_elements_df.iterrows():
                    pass  # TODO

            # write to new munger file
            section_strings = ["\n".join(new_sections[k]) for k in new_sections.keys()]
            file_string = "\n\n".join(section_strings)
            new_mu_file_path = os.path.join(new_mungers_dir, f"{mu}.munger")
            with open(new_mu_file_path, "w") as f:
                f.write(file_string)
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
    "rows_to_skip": "list-of-integers",
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


def get_and_check_munger_params(munger_path: str) -> (dict, Optional[dict]):
    params, err = ui.get_parameters(
        required_keys=list(req_munger_params.keys()),
        optional_keys=list(opt_munger_params.keys()),
        param_file=munger_path,
        header="format",
        err=dict(),
    )
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
    # # count_field_name_row is given where required
    if (format_options["file_type"] in ["excel", "flat_text"]) and (format_options["count_locations"] == "by_field_name"):
        if format_options["count_field_name_row"] is None:
            err = ui.add_new_error(
                err,
                "munger",
                munger_name,
                f"file_type={format_options['file_type']}' but count_field_name_row not found"
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

    return format_options, err


def read_results_from_file(f_path, format_options) -> (Dict[str,pd.DataFrame], dict):
    # TODO
    return results, err

def load_results_file(
        # session,
        munger_path: str,
        f_path: str,
        # juris: jm.Jurisdiction,
        # results_info: dict,
) -> Optional[dict]:
    # get name of munger
    munger_name = Path(munger_path).name
    # TODO complete this routine
    # read parameters from munger file
    p, err = get_and_check_munger_params(munger_path)
    if ui.fatal_error(err):
        return err

    # read count dataframe(s) from file
    raw_dict, read_err = ui.read_single_datafile(p, f_path)
    if read_err:
        err = ui.consolidate_errors([err,read_err])
        if ui.fatal_error(read_err):
            return err
    # remove any sheets designated to be removed
    if p["sheets_to_skip"]:
        for k in p["sheets_to_skip"]:
            raw_dict.pop(k)
    # for each df:
    for k, raw in raw_dict.items():
        # # transform to df where all columns are count columns and all raw munge info is in row multi-index
        df = raw_dict[k].copy()
        if p["count_locations"] == "by_field_name":
            string_columns = [c for c in df.columns if c not in p["count_fields_by_name"]]
        elif p["count_locations"] == "by_column_number":
            string_columns = [x for x in df.columns if df.columns.index(x) not in p["count_column_numbers"]]
        else:
            err = ui.add_new_error(
                err,
                "system",
                "load_results_file",
                "TODO: system should check munger file to avoid this error"
            )
            return err
        df.set_index(string_columns, inplace=True)

        # # clean all columns (they are all count columns now!) and report any rows with bad counts
        df, bad_rows = m.clean_count_cols(df, df.columns, p["thousands_separator"])
        if not bad_rows.empty:
            err = ui.add_err_df(err, bad_rows, munger_name, f_path)

        # # transform raw multi-index to munged multi-index (possibly with foreign keys if there is aux data)

        # # add columns for any constant-over-sheet elements
        # # replace any foreign keys with true values
        # # load counts to db
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
        new_mungers_directory,
        results_directory,
        munger_list=["wi_gen20"]
    )
    results_path = "/Users/singer3/PycharmProjects/election_data_analysis/tests/TestingData/_000-Final-2020-General/archived/Wisconsin/UNOFFICIAL WI Election Results 2020 by County 11-5-2020.xlsx"
    munger_path = "/mungers_new_by_hand/wi_gen20.munger"
    load_results_file(munger_path, results_path)
    exit()
