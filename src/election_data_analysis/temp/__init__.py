from election_data_analysis import user_interface as ui
from election_data_analysis import juris_and_munger as jm
import os
import re
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
    if ("count_columns" not in d_mu.keys()) or (d_mu["count_columns"] is not None):
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
             sep = "tab"
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
            try:
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
                new_sections["format_top_lines"] = ["[format]"]
                new_sections["format"] = []
                # copy simple parameters
                for param in ["encoding", "thousands_separator"]:
                    if (d[param] is not None) and (d[param] != "None"):
                        new_sections["format"].append(f"{param}={d[param]}")

                # set file_type and related params
                if d["file_type"] == "csv":
                    new_sections["format_top_lines"].append(f"file_type=flat_text")
                    new_sections["format"].append(f"flat_file_delimiter=,")
                elif d["file_type"] == "txt":
                    new_sections["format_top_lines"].append(f"file_type=flat_text")
                    new_sections["format"].append(f"flat_file_delimiter=tab")
                elif d["file_type"] == "txt-semicolon-separated":
                    new_sections["format_top_lines"].append(f"file_type=flat_text")
                    new_sections["format"].append(f"flat_file_delimiter=;")
                elif d["file_type"] == "xls":
                    new_sections["format_top_lines"].append(f"file_type=excel")
                elif d["file_type"] == "xls-multi":
                    new_sections["format_top_lines"].append(f"file_type=excel")
                    new_sections["format"].append(f"sheets_to_skip={d['sheets_to_skip']}")
                elif d["file_type"] in ["json-nested", "xml"]:
                    new_sections["format_top_lines"].append(f"file_type={d['file_type']}")

                # set count_locations and related params
                if d["count_columns"] is not None:
                    new_sections["format_top_lines"].append(f"count_locations=by_column_number")
                    new_sections["format"].append(f"count_column_numbers={d['count_columns']}")
                elif mu in count_field_dict.keys():
                    new_sections["format_top_lines"].append(f"count_locations=by_field_name")
                    new_sections["format"].append(f"count_fields={count_field_dict[mu]}")
                elif d["field_names_if_no_field_name_row"] is not None:
                    new_sections["format_top_lines"].append(f"count_locations=by_column_number")
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
                new_sections["format_top_lines"].append(f"string_locations={str_l_str}")

                # get info from field_name_row
                if d["field_name_row"] is not None:
                    new_sections["format"].append(f"string_field_name_row={d['field_name_row']}")
                    new_sections["format"].append(f"count_field_name_row={d['field_name_row']}")


                # set rows to skip
                if d["count_of_top_lines_to_skip"]:
                    new_sections["format"].append(f"rows_to_skip={d['count_of_top_lines_to_skip']}")

                # note if all rows of a flat file contain only data (not field names)
                if (d["field_names_if_no_field_name_row"] is not None) and (d["field_names_if_no_field_name_row"] != "None"):
                    new_sections["format"].append("all_rows=data")

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
                    """
                    # if revising single directory
                    if old_mungers_dir == new_mungers_dir:
                        # delete old files
                        for fi in os.listdir(mu_dir):
                            os.remove(os.path.join(mu_dir, fi))
                        os.rmdir(mu_dir)"""
            except Exception as exc:
                print(f"Skipping {mu}: {exc}")
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


if __name__ == "__main__":
    ini_directory = '/Users/singer3/PycharmProjects/election_data_analysis/src/ini_files_for_results'
    old_mungers_directory = '/Users/singer3/PycharmProjects/election_data_analysis/src/mungers_old'
    new_mungers_directory = '/Users/singer3/PycharmProjects/election_data_analysis/src/mungers'
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
        munger_list=["nc_gen"],
    )

    # err = create_ini_files(ini_directory)



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
