import election_data_analysis as e
from election_data_analysis import user_interface as ui
from election_data_analysis import juris_and_munger as jm
import re
import os
import pandas as pd
from typing import List, Dict, Optional


def count_columns_by_name(
        format_file_path: str,
        results_dir: dict,
        state: str,
        d_ini: dict,
        mu: str,
) -> Optional[str]:
    "returns comma-separated list of count columns"
    # get file_type, count_columns, count_columns_by_name and header_row_count
    d_mu, mu_err = ui.get_runtime_parameters(
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
            skiprows=0
        else:
            skiprows=int(d_mu["count_of_top_lines_to_skip"])
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
                d_ini, err = ui.get_runtime_parameters(
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


def run(ini_dir, old_mungers_dir, new_mungers_dir, results_dir):
    err = None

    headers = ["format", "by_field_name", "in_count_headers", "in_constant_row"]

    count_field_dict = count_fields(ini_dir, old_mungers_dir, results_dir)

    for mu in os.listdir(old_mungers_dir):
        mu_dir = os.path.join(old_mungers_dir, mu)
        # for each genuine munger
        if os.path.isdir(mu_dir):

            # initialize dict to hold info to be written
            new_sections = dict()
            for h in headers:
                new_sections[h] = [f"[{h}]"]

            # get contents of format.config
            d, err = ui.get_runtime_parameters(
                required_keys=jm.munger_pars_req,
                optional_keys=jm.munger_pars_opt,
                header="format",
                param_file=os.path.join(mu_dir, "format.config"),
                err=err,
            )
            # get contents of cdf_elements
            cdf_elements_df = pd.read_csv(os.path.join(mu_dir, "cdf_elements.txt"))

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

            if d["count_columns"] is not None:
                new_sections["format"].append(f"count_location=by_column_number")
                new_sections["format"].append(f"count_columns={d['count_columns']}")
            elif mu in count_field_dict.keys():
                new_sections["format"].append(f"count_location=by_field_name")
                new_sections["format"].append(f"count_fields={count_field_dict[mu]}")
            elif d["field_names_if_no_field_name_row"] is not None:
                new_sections["format"].append(f"count_location=by_column_number")
                new_sections["format"].append(f"count_columns={d['count_columns']}")

                # create contents for 'by_field_name' header

                # create contents for 'in_count_headers' header

                # create contents for 'in_constant_row' header

    return


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

    aa = count_fields(ini_directory, old_mungers_directory, results_directory)

    run(
        ini_directory,
        old_mungers_directory,
        new_mungers_directory,
        results_directory,
    )
exit()