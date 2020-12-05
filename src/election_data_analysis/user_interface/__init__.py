from configparser import ConfigParser, MissingSectionHeaderError
from election_data_analysis import munge as m
from election_data_analysis import special_formats as sf
from election_data_analysis import database as db
import election_data_analysis as e
import pandas as pd
import numpy as np
from pandas.errors import ParserError, ParserWarning
import csv
import os
from pathlib import Path
from election_data_analysis import juris_and_munger as jm
from typing import Optional, Dict, Any, List
import datetime

# constants
recognized_encodings = {
    "iso2022jp",
    "arabic",
    "cp861",
    "csptcp154",
    "shiftjisx0213",
    "950",
    "IBM775",
    "IBM861",
    "shift_jis",
    "shift-jis",
    "euc_jp",
    "euc-jp",
    "ibm1026",
    "ascii",
    "ASCII",
    "IBM437",
    "EBCDIC-CP-BE",
    "csshiftjis",
    "cp1253",
    "jisx0213",
    "latin",
    "cp874",
    "861",
    "windows-1255",
    "cp1361",
    "macroman",
    "ms950",
    "iso-2022-jp-3",
    "iso8859_14",
    "cp949",
    "utf_16",
    "utf-16",
    "932",
    "cp737",
    "iso2022_jp_2004",
    "ks_c-5601",
    "iso-2022-kr",
    "ms936",
    "cp819",
    "iso-8859-3",
    "windows-1258",
    "csiso2022kr",
    "iso-8859-2",
    "iso2022_jp_ext",
    "hz",
    "iso-8859-13",
    "IBM855",
    "cp1140",
    "866",
    "862",
    "iso2022jp-2004",
    "cp1250",
    "windows-1254",
    "cp1258",
    "gb2312-1980",
    "936",
    "L6",
    "iso-8859-6",
    "ms932",
    "macgreek",
    "cp154",
    "big5-tw",
    "maccentraleurope",
    "iso-8859-7",
    "ks_x-1001",
    "csbig5",
    "cp1257",
    "latin1",
    "mac_roman",
    "mac-roman",
    "euckr",
    "latin3",
    "eucjis2004",
    "437",
    "cp500",
    "mac_latin2",
    "CP-GR",
    "IBM863",
    "hz-gb-2312",
    "iso2022jp-3",
    "iso-8859-15",
    "koi8_r",
    "sjisx0213",
    "windows-1252",
    "850",
    "cp855",
    "windows1256",
    "eucjisx0213",
    "hkscs",
    "gb18030",
    "iso-2022-jp-2004",
    "L1",
    "cyrillic-asian",
    "iso2022jp-ext",
    "cp1006",
    "utf16",
    "iso2022_kr",
    "iso2022jp-2",
    "shiftjis",
    "IBM037",
    "gb2312-80",
    "IBM500",
    "865",
    "UTF-16BE",
    "IBM864",
    "EBCDIC-CP-CH",
    "iso-8859-4",
    "cp856",
    "iso2022_jp_1",
    "eucjp",
    "iso-2022-jp-1",
    "iso8859_3",
    "gb18030-2000",
    "cp860",
    "mskanji",
    "iso2022jp-1",
    "iso-8859-8",
    "iso-2022-jp-ext",
    "csiso58gb231280",
    "shift_jis_2004",
    "L2",
    "ms1361",
    "cp852",
    "ms949",
    "IBM865",
    "cp437",
    "iso8859_4",
    "iso8859_2",
    "cp1255",
    "euc_jisx0213",
    "cp1252",
    "macturkish",
    "iso8859_9",
    "ptcp154",
    "949",
    "cp864",
    "s_jisx0213",
    "big5-hkscs",
    "korean",
    "iso2022_jp_2",
    "cp932",
    "euc-cn",
    "latin5",
    "utf_8",
    "utf-8",
    "ibm1140",
    "cp862",
    "euc_kr",
    "euc-kr",
    "iso8859_8",
    "iso-8859-9",
    "utf8",
    "cp1251",
    "863",
    "cp850",
    "cp857",
    "greek",
    "latin8",
    "iso2022_jp_3",
    "iso-8859-10",
    "big5hkscs",
    "ms-kanji",
    "iso2022kr",
    "646",
    "iso8859_7",
    "koi8_u",
    "mac_greek",
    "mac-greek",
    "windows-1251",
    "cp775",
    "IBM860",
    "u-jis",
    "iso-8859-5",
    "us-ascii",
    "maccyrillic",
    "IBM866",
    "L3",
    "sjis2004",
    "cp1256",
    "sjis_2004",
    "sjis-2004",
    "852",
    "windows-1250",
    "latin4",
    "cp037",
    "shift_jisx0213",
    "greek8",
    "latin6",
    "latin2",
    "mac_turkish",
    "mac-turkish",
    "IBM862",
    "iso8859-1",
    "cp1026",
    "IBM852",
    "pt154",
    "iso-2022-jp-2",
    "ujis",
    "855",
    "iso-8859-14",
    "iso-2022-jp",
    "utf_16_be",
    "chinese",
    "maclatin2",
    "U7",
    "hzgb",
    "iso8859_5",
    "857",
    "IBM850",
    "8859",
    "gb2312",
    "cp866",
    "CP-IS",
    "latin_1",
    "latin-1",
    "L4",
    "euccn",
    "cyrillic",
    "IBM424",
    "cp863",
    "UTF-16LE",
    "mac_cyrillic",
    "mac-cyrillic",
    "iso8859_10",
    "L8",
    "IBM869",
    "ksc5601",
    "860",
    "iso2022_jp",
    "hz-gb",
    "UTF",
    "utf8ascii",
    "utf_7",
    "utf-7",
    "cp936",
    "euc_jis_2004",
    "iso-ir-58",
    "csiso2022jp",
    "IBM039",
    "eucgb2312-cn",
    "cp950",
    "iso8859_13",
    "shiftjis2004",
    "sjis",
    "U8",
    "cp1254",
    "s_jis",
    "s-jis",
    "gbk",
    "hebrew",
    "U16",
    "big5",
    "cp865",
    "cp424",
    "uhc",
    "windows-1257",
    "869",
    "iso-8859-1",
    "windows-1253",
    "ksx1001",
    "johab",
    "IBM857",
    "L5",
    "iso8859_6",
    "cp869",
    "cp875",
    "mac_iceland",
    "mac-iceland",
    "iso8859_15",
    "maciceland",
    "utf_16_le",
    "EBCDIC-CP-HE",
    "ks_c-5601-1987",
}

# keys for error- and warning-tracking dictionary
error_keys = {
    "ini",
    "munger",
    "jurisdiction",
    "file",
    "system",
}

warning_keys = {
    "warn-ini",
    "warn-munger",
    "warn-jurisdiction",
    "warn-file",
    "warn-system",
}

contest_type_mappings = {
    "congressional": "Congressional",
    "state": "Statewide",
    "state-house": "State House",
    "state-senate": "State Senate",
}


def read_results(
    results_file_path, munger_path, aux_data_path, error: Optional[dict]
) -> (pd.DataFrame, jm.Munger, dict):
    """Reads results (appending '_SOURCE' to the columns)
    and initiates munger."""

    # check munger files and (if no error) create munger
    mu, mu_err = jm.check_and_init_munger(munger_path, aux_data_path=aux_data_path)
    error = consolidate_errors([error, mu_err])
    if fatal_error(mu_err):
        wr = pd.DataFrame()
    else:
        # read info from results file (and auxiliary files, if any)
        wr, error = read_combine_results(
            mu, results_file_path, error, aux_data_path=aux_data_path
        )
        wr.columns = [f"{x}_SOURCE" for x in wr.columns]
    return wr, mu, error


def pick_juris_from_filesystem(juris_path, err, check_files=False):
    """Returns a Jurisdiction object. <juris_path> is the path to the directory containing the
    defining files for the particular jurisdiction.
    """

    if check_files:
        new_err = jm.ensure_jurisdiction_dir(juris_path)
    err = consolidate_errors([err, new_err])
    if fatal_error(new_err):
        ss = None
    else:
        # initialize the jurisdiction
        ss = jm.Jurisdiction(juris_path)
    return ss, err


def find_dupes(df):
    dupes_df = df[df.duplicated()].drop_duplicates(keep="first")
    deduped = df.drop_duplicates(keep="first")
    return dupes_df, deduped


def read_single_datafile(
    munger: jm.Munger, f_path: str, err: Optional[dict]
) -> (pd.DataFrame, dict):
    try:
        dtype = {c: str for c in munger.field_list}
        kwargs = {"dtype": dtype}
        if munger.thousands_separator is not None:
            kwargs["thousands"] = munger.thousands_separator

        if munger.options["file_type"] in ["json"]:
            pass
        elif munger.options["field_name_row"] is None:
            kwargs["header"] = None
            kwargs["names"] = munger.options["field_names_if_no_field_name_row"]
            kwargs["index_col"] = False
        else:
            # if not a multi-index
            if munger.options["header_row_count"] == 1:
                # can protect against a problem with pandas.read_csv and read_excel
                #  see https://github.com/pandas-dev/pandas/issues/11733
                # TODO how to protect in case of multi-index?
                kwargs["header"] = 0
                kwargs["index_col"] = False
            else:
                kwargs["header"] = list(range(munger.options["header_row_count"]))
                kwargs["index_col"] = None

        if munger.options["count_of_top_lines_to_skip"]:
            kwargs["skiprows"] = range(munger.options["count_of_top_lines_to_skip"])

        if munger.file_type in ["txt", "csv", "txt-semicolon-separated"]:
            kwargs["encoding"] = munger.encoding
            kwargs["quoting"] = csv.QUOTE_MINIMAL
            if munger.file_type == "txt":
                kwargs["sep"] = "\t"
            elif munger.file_type == "txt-semicolon-separated":
                kwargs["sep"] = ";"
            df = pd.read_csv(f_path, **kwargs)
        elif munger.file_type in ["xls", "xlsx"]:
            df = pd.read_excel(f_path, **kwargs)
        elif munger.file_type in ["json"]:
            kwargs["encoding"] = munger.encoding
            df = pd.read_json(f_path, **kwargs)
        elif munger.file_type in ["concatenated-blocks", "xls-multi", "xml", "json-nested"]:
            err = add_new_error(
                err,
                "system",
                "user_interface.read_single_datafile",
                f"Munger ({munger.name}) with file_type {munger.file_type} "
                f"should not have reached this part of the code.",
            )
            return pd.DataFrame(), err
        else:
            err = add_new_error(
                err,
                "munger",
                munger.name,
                f"Unrecognized file_type: {munger.file_type}",
            )
            return pd.DataFrame(), err
        if df.empty:
            err = add_new_error(
                err,
                "munger",
                munger.name,
                f"Nothing read from datafile. Munger may be inconsistent, or datafile may be empty.",
            )
        else:
            # get count columns by name
            if munger.options["count_columns_by_name"]:
                count_cols_by_name = munger.options["count_columns_by_name"]
            elif munger.options["count_columns"]:
                count_cols_by_name = [
                    df.columns[j]
                    for j in munger.options["count_columns"]
                    if j < df.shape[1]
                ]
            else:
                count_cols_by_name = None

            # clean the column names
            df, count_cols_by_name, err_str = m.clean_column_names(
                df, count_cols_by_name
            )
            if err_str:
                err = add_new_error(err, "warn-file", Path(f_path).name, err_str)

            # clean the count columns
            df, err_df = m.clean_count_cols(df, count_cols_by_name, thousands=munger.thousands_separator)
            if not err_df.empty:
                err = add_err_df(err, err_df, munger, f_path)
                # show all columns of dataframe holding rows where counts were set to 0
                pd.set_option("max_columns", None)
                err = add_new_error(
                    err,
                    "warn-munger",
                    munger.name,
                    f"At least one count was set to 0 in certain rows of {Path(f_path).name}:\n{err_df}",
                )
                pd.reset_option("max_columns")

            # clean the string columns
            str_cols = [c for c in df.columns if df.dtypes[c] == np.object]
            df = m.clean_strings(df, str_cols)

            err = jm.check_results_munger_compatibility(
                munger, df, Path(f_path).name, err
            )
        return df, err
    except FileNotFoundError as fnfe:
        e = f"File not found: {f_path}"
    except UnicodeDecodeError as ude:
        e = f"Encoding error. Datafile not read completely.\n{ude}"
    except ParserError as pe:
        # DFs have trouble comparing against None. So we return an empty DF and
        # check for emptiness below as an indication of an error.
        e = f"Error parsing results file.\n{pe}"
    err = add_new_error(
        err,
        "file",
        Path(f_path).name,
        e,
    )
    return pd.DataFrame(), err


def add_err_df(err, err_df, munger, f_path):
    # show all columns of dataframe holding rows where counts were set to 0
    pd.set_option("max_columns", None)
    err = add_new_error(
        err,
        "warn-munger",
        munger.name,
        f"At least one count was set to 0 in certain rows of {Path(f_path).name}:\n{err_df}",
    )
    pd.reset_option("max_columns")
    return err


def read_combine_results(
    mu: jm.Munger,
    results_file_path: str,
    err: Optional[dict],
    aux_data_path: str = None,
) -> (pd.DataFrame, dict):
    # if results are not a flat file type or json
    if mu.options["file_type"] in ["concatenated-blocks", "xls-multi", "xml", "json-nested"]:
        working, new_err = sf.read_alternate_munger(
            mu.options["file_type"],
            results_file_path,
            mu,
            None,
        )
        if new_err:
            err = consolidate_errors([err, new_err])
        if working.empty or fatal_error(new_err):
            return working, err

        # revise munger as appropriate
        mu.options["header_row_count"] = 1
        mu.options["field_name_row"] = 0
        if mu.options["file_type"] in ["concatenated-blocks", "xls-multi"]:
            mu.options["count_columns"] = [working.columns.to_list().index("count")]

        elif mu.file_type in ["xml", "json-nested"]:
            # TODO tech debt some of this may not be necessary
            # change formulas in cdf_elements to match column names inserted by read_xml
            mu.cdf_elements["idx"] = mu.cdf_elements.index
            mu.cdf_elements["source"] = "row"
            mu.options["count_columns"] = [working.columns.to_list().index(c) for c in mu.options["count_columns_by_name"]]

    # if results are a flat file or json
    else:
        try:
            working, new_err = read_single_datafile(mu, results_file_path, None)
        except Exception as exc:
            err = add_new_error(
                err,
                "file",
                results_file_path,
                f"Unexpected exception while reading file: {exc}",
            )
            return pd.DataFrame(), err

        if new_err:
            err = consolidate_errors([err, new_err])
            if fatal_error(new_err):
                return pd.DataFrame(), err

        else:
            # if json
            if mu.options["file_type"] in ["json"]:
                # get numbers of count columns (now that we've read in the data)
                mu.options["count_columns"] = [
                    working.columns.to_list().index(c)
                    for c in mu.options["count_columns_by_name"]
                ]
            working, new_err = m.cast_cols_as_int(
                working,
                mu.options["count_columns"],
                mode="index",
                munger_name=mu.name,
            )
            if new_err:
                err = consolidate_errors([err, new_err])
                if fatal_error(new_err):
                    return working, err

        # merge with auxiliary files (if any)
        if aux_data_path is not None:
            # get auxiliary data (includes cleaning and setting (multi-)index of primary key column(s))
            aux_data, new_err = mu.get_aux_data(
                aux_data_path,
                None,
            )
            if new_err:
                err = consolidate_errors([err, new_err])
                if fatal_error(new_err):
                    return pd.DataFrame(), err
            for abbrev, r in mu.aux_meta.iterrows():
                # cast foreign key columns of main results file as int if possible
                foreign_key = r["foreign_key"].split(",")
                working, new_err = m.cast_cols_as_int(
                    working, foreign_key, munger_name=mu.name
                )
                if new_err:
                    err = consolidate_errors([err, new_err])
                    if fatal_error(new_err):
                        return working, err

                # rename columns
                col_rename = {
                    f"{c}": f"{abbrev}[{c}]" for c in aux_data[abbrev].columns
                }
                # merge auxiliary info into <working>
                a_d = aux_data[abbrev].rename(columns=col_rename)
                working = working.merge(
                    a_d, how="left", left_on=foreign_key, right_index=True
                )

    return working, err


def archive_from_param_file(param_file: str, current_dir: str, archive_dir: str):
    params, err = get_parameters(
        required_keys=["results_file", "aux_data_dir"],
        header="election_data_analysis",
        param_file=os.path.join(current_dir, param_file),
    )
    # TODO error handling
    # if the ini file specifies an aux_data_directory
    if "aux_data_dir" in params.keys() and params["aux_data_dir"] and params["aux_data_dir"] != "":
        archive(params["aux_data_dir"], current_dir, archive_dir)
    archive(params["results_file"], current_dir, archive_dir)
    archive(param_file, current_dir, archive_dir)
    return


def archive(relative_path: str, current_dir: str, archive_dir: str):
    """Move <relative_path> from <current_dir> to <archive_dir>. If <archive_dir> already has a file with that name,
    add a number prefix to the name of the created file."""
    old_path = os.path.join(current_dir, relative_path)
    new_path = os.path.join(archive_dir, relative_path)

    # Create archive directory (and any necessary subdirectories)
    archive_dir_with_subs = Path(os.path.join(archive_dir, relative_path)).parent
    Path(archive_dir_with_subs).mkdir(parents=True, exist_ok=True)

    i = 0
    while os.path.exists(new_path):
        i += 1
        new_path = os.path.join(archive_dir, f"{i}_{relative_path}")
    try:
        os.rename(old_path, new_path)
    except Exception as exc:
        print(f"File {relative_path} not moved: {exc}")
    return


def load_datafile(
    session,
    munger: jm.Munger,
    raw_path: str,
    juris: jm.Jurisdiction,
    results_info: dict,
    aux_data_path: str = None,
) -> Optional[dict]:
    """Guide user through process of uploading data in <raw_file>
    into common data format.
    Assumes cdf db exists already"""
    err = None
    raw, err = read_combine_results(munger, raw_path, err, aux_data_path=aux_data_path)
    if fatal_error(err):
        return err
    elif raw.empty:
        err = add_new_error(
            err,
            "file",
            raw_path,
            f"No data read from file",
        )
        return err
    # ensure that there is at least one count column
    if not munger.options["count_columns"]:
        err = add_new_error(
            err, "munger", munger.name, f"No count_columns specified for munger"
        )
        return err
    else:
        count_columns_by_name = [
            raw.columns[x] for x in munger.options["count_columns"] if x < raw.shape[1]
        ]

    try:
        new_err = m.raw_elements_to_cdf(
            session,
            juris,
            munger,
            raw,
            count_columns_by_name,
            err,
            constants=results_info,
        )
        if new_err:
            # append munger name to jurisdiction errors/warnings key
            keys = [x for x in new_err["warn-jurisdiction"].keys()]
            for k in keys:
                new_err["warn-jurisdiction"][f"{k}-{munger.name}"] = new_err[
                    "warn-jurisdiction"
                ].pop(k)
            err = consolidate_errors([err, new_err])
            if fatal_error(new_err):
                return err
    except Exception as exc:
        err = add_new_error(
            err,
            "system",
            "user_interface.new_datafile",
            f"Unexpected error during munging: {exc}",
        )
        return err

    print(
        f"\n\tResults uploaded with munger {munger.name} "
        f"to database {session.bind.engine}\nfrom file {raw_path}\n"
        f"assuming jurisdiction {juris.path_to_juris_dir}"
    )
    return err


def get_parameters(
    required_keys: list,
    param_file: str,
    header: str,
    err: Optional[Dict[Any, dict]] = None,
    optional_keys: list = None,
) -> (dict, Optional[dict]):
    d = {}

    # read info from file
    parser = ConfigParser()
    # find header
    try:
        p = parser.read(param_file)
        if len(p) == 0:
            err = add_new_error(err, "file", param_file, "File not found")
            return d, err

        h = parser[header]
    except (KeyError, MissingSectionHeaderError) as ke:
        err = add_new_error(err, "ini", param_file, f"Missing header: {ke}")
        return d, err

    # read required info
    missing_required_params = list()
    for k in required_keys:
        try:
            d[k] = h[k]
        except KeyError:
            missing_required_params.append(k)
    if missing_required_params:
        err = add_new_error(
            err,
            "ini",
            param_file,
            f"Missing required parameters: {missing_required_params}",
        )
        return d, err

    # read optional info
    if optional_keys is None:
        optional_keys = []
    for k in optional_keys:
        try:
            d[k] = parser[header][k]
        except KeyError:
            d[k] = None

    return d, err


def consolidate_errors(list_of_err: list) -> Optional[Dict[Any, dict]]:
    """Takes two error dictionaries (assumed to have same bottom-level keys)
    and consolidates them, concatenating the error messages"""
    """Consolidate the error dictionaries in <list_of_err>. If any dictionary is None, ignore it.
    If all dictionaries are None, return None"""

    # take union of all error-types appearing
    err_types = set().union(*[x.keys() for x in list_of_err if x])
    # if errs are all empty or none
    if err_types == set():
        return None

    d = dict()
    for et in err_types:
        # initialize
        d[et] = dict()
        # find errs that have the error-type
        err_list = [x[et] for x in list_of_err if x and (et in x.keys())]
        # take union of all name-keys appearing for this error-type
        name_keys = set().union(*[y.keys() for y in err_list])
        for nk in name_keys:
            msg_list_of_lists = [y[nk] for y in err_list if nk in y.keys()]
            # assign list of all messages (avoiding duplicates)
            d[et][nk] = list({y for x in msg_list_of_lists for y in x})
    return d


def report(
    err_warn: Optional[Dict[Any, dict]],
    loc_dict: Optional[Dict[Any, dict]] = None,
    key_list: list = None,
    file_prefix: str = "",
) -> dict:
    """unpacks error dictionary <err> for reporting.
    Keys of <location_dict> are error_types;
    values of <loc_dict> are directories for writing error files.
    Use <key_list> to report only on some keys, and return a copy of err_warn with those keys removed"""
    if not loc_dict:
        loc_dict = dict()
    if err_warn:
        if not key_list:
            # report all keys (otherwise report only key-list keys)
            key_list = err_warn.keys()

        # create error/warning messages for each error_type/name_key pair
        # list keys with content
        active_keys = [k for k in key_list if err_warn[k] != {}]

        # create working list of ets to process
        ets_to_process = [
            et
            for et in error_keys
            if (et in active_keys) or (f"warn-{et}" in active_keys)
        ]

        # create all et-nk tuples
        tuples = set()
        for et in active_keys:
            tuples = tuples.union({(et, nk) for nk in err_warn[et].keys()})

        # map each tuple to its message
        msg = {(et, nk): "\n".join(err_warn[et][nk]) for et, nk in tuples}

        # write errors/warns to error files
        while ets_to_process:
            et = ets_to_process.pop()
            # et is an error type. <et> might be a key of err_warn,
            # or warn-<et> might be a key, or both

            # if et has any errors
            if et in active_keys:
                # process name keys with actual errors
                for nk in err_warn[et].keys():
                    # prepare output string (errors and warns if any)
                    nk_name = Path(nk).name
                    if (f"warn-{et}" in active_keys) and (
                        nk in err_warn[f"warn-{et}"].keys()
                    ):
                        warn_str = f"\n{et.title()} warnings ({nk_name}):\n{msg[(f'warn-{et}', nk)]}\n\n"
                        and_warns = " and warnings"
                    else:
                        warn_str = and_warns = ""
                    out_str = f"\n{et.title()} errors ({nk_name}):\n{msg[(et, nk)]}\n\n{warn_str}"

                    # print/write output
                    if et in loc_dict.keys():
                        # get timestamp
                        ts = datetime.datetime.now().strftime("%m%d_%H%M")
                        # write info to a .errors or .errors file named for the name_key <nk>
                        out_path = os.path.join(
                            loc_dict[et], f"{file_prefix}{nk_name}_{ts}.errors"
                        )
                        with open(out_path, "a") as f:
                            f.write(out_str)
                        print(f"{et.title()} errors{and_warns} written to {out_path}")
                    else:
                        # print for user
                        print(out_str)

            # process name keys with only warnings
            only_warns = [
                nk
                for nk in err_warn[f"warn-{et}"].keys()
                if nk not in err_warn[et].keys()
            ]
            for nk in only_warns:
                # prepare output string
                nk_name = Path(nk).name
                out_str = (
                    f"{et.title()} warnings ({nk_name}):\n{msg[(f'warn-{et}', nk)]}\n"
                )

                # print/write output
                if f"warn-{et}" in loc_dict.keys():
                    # ensure directory exists
                    # TODO error handline: what if the path is a file?
                    if not os.path.exists(loc_dict[f"warn-{et}"]):
                        Path(loc_dict[f"warn-{et}"]).mkdir(parents=True, exist_ok=True)

                    # get timestamp
                    ts = datetime.datetime.now().strftime("%m%d_%H%M")
                    # write info to a .errors or .errors file named for the name_key <nk>
                    out_path = os.path.join(
                        loc_dict[f"warn-{et}"], f"{file_prefix}{nk_name}_{ts}.warnings"
                    )
                    with open(out_path, "a") as f:
                        f.write(out_str)
                    print(f"{et.title()} warnings written to {out_path}")
                else:
                    # print for user
                    print(out_str)

        # define return dictionary with reported keys set to {} and othe keys preserved
        remaining = {k: v for k, v in err_warn.items() if k not in key_list}
        for k in key_list:
            remaining[k] = {}
    else:
        print("No errors or warnings")
        remaining = None

    return remaining


def add_new_error(
    err: Optional[Dict[Any, dict]], err_type: str, key: str, msg: str
) -> dict:
    """err is a dictionary of dictionaries, one for each err_type.
    This function return err, augmented by the error specified in <err_type>,<key> and <msg>"""
    if err is None or err == dict():
        err = {k: {} for k in warning_keys.union(error_keys)}
        # TODO document. Problems with results file are reported with "ini" key
    if err_type not in err.keys():
        err = add_new_error(
            err,
            "system",
            "user_interface.add_new_error",
            f"Unrecognized key ({err_type}) for message {msg}",
        )
        return err
    if key in err[err_type].keys():
        err[err_type][key].append(msg)
    else:
        err[err_type][key] = [msg]
    return err


def fatal_error(err, error_type_list=None, name_key_list=None) -> bool:
    """Returns true if there is a fatal error in the error dictionary <err>
    matching all given criteria"""
    if not err:
        return False
    # if no error types are given, use them all
    if not error_type_list:
        error_type_list = err.keys()
    # warnings are not fatal
    fatal_et_list = [x for x in error_type_list if "warn" not in x]
    for et in fatal_et_list:
        if not name_key_list:
            bad = err[et].keys()
        else:
            bad = [x for x in name_key_list if x in err[et].keys()]
        if bad:
            return True
    return False


def create_param_file(
    db_params: dict, multi_data_loader_pars: dict, target_dir: str
) -> Optional[str]:
    err_str = None
    if not os.path.isdir(target_dir):
        return f"Directory not found: {target_dir}"
    db_params_str = "\n".join([f"{s}={db_params[s]}" for s in db_params.keys()])
    mdlp_str = "\n".join(
        [f"{s}={multi_data_loader_pars[s]}" for s in multi_data_loader_pars.keys()]
    )

    with open(os.path.join(target_dir, "run_time.ini"), "w") as f:
        f.write(
            "[postgresql]\n"
            + db_params_str
            + "\n\n[election_data_analysis]\n"
            + mdlp_str
        )

    return err_str


def run_tests(
    test_dir: str, dbname: str, election_jurisdiction_list: Optional[list] = None
) -> (dict(), int):
    """move to tests directory, run tests, move back
    db_params must have host, user, pass, db_name.
    test_param_file is a reference run_time.ini file"""

    r = -1
    # note current directory
    original_dir = os.getcwd()

    # move to tests directory
    os.chdir(test_dir)

    result = dict()    # initialize result report
    # run pytest
    if election_jurisdiction_list is None:
        r = os.system(f"pytest --dbname {dbname}")
        if r != 0:
            result["all"] = "At least one test failed"
    else:
        for (election, juris) in election_jurisdiction_list:
            if election is None and juris is not None:
                keyword = f"{juris.replace(' ','-')}"
            elif juris is None and election is not None:
                keyword = f"{juris.replace(' ','-')}"
            elif juris is not None and election is not None:
                keyword = f"{juris.replace(' ','-')}_{election.replace(' ','-')}"
            else:
                keyword = "_"
            r = os.system(f"pytest --dbname {dbname} -k {keyword}")
            if r != 0:
                result[f"{keyword}"] = "all did not pass"

    # move back to original directory
    os.chdir(original_dir)
    # result is 0 if all tests pass, non-zero if something went wrong
    return result, r


def confirm_essential_info(
    directory: str,
    header: str,
    param_list: List[str],
    known: Optional[dict] = None,
    msg: str = "",
):
    """Returns True if user confirms all values in key_list for all *.ini files in
    the given directory; False otherwise"""

    # loop through files
    for f in [f for f in os.listdir(directory) if f[-4:] == ".ini"]:
        p_path = os.path.join(directory, f)
        file_confirmed = False
        while not file_confirmed:
            param_dict, err = get_parameters(
                required_keys=param_list + list(known.keys()),
                header=header,
                param_file=p_path,
            )
            if err:
                err_str = ""
                if "file" in err.keys():
                    err_str += f"File error: {err['file']}\n"
                if "ini" in err.keys():
                    err_str += f"Error in file: {err['ini']}"
                input(f"Please fix errors and try again: {err_str}")
            else:
                # have user confirm unknowns
                param_str = "\n".join([f"{s}={param_dict[s]}" for s in param_list])
                user_confirm = input(
                    f"Are all of these parameters from {f} correct (y/n)?{msg}\n{param_str}"
                )
                if user_confirm == "y":
                    file_confirmed = True

                # check knowns
                incorrect_knowns = []
                for k in known.keys():
                    if known[k] != param_dict[k]:
                        incorrect_knowns.append(f"Need {k}={param_dict[k]}")
                if incorrect_knowns:
                    incorrect_str = "\n".join(incorrect_knowns)
                    print(
                        f"Incorrect values in {f}:\n{incorrect_str}\n"
                        f"Either remove those files or correct their parameter values"
                    )
                    file_confirmed = False

                if not file_confirmed:
                    input("Correct file and hit return to continue.")
    return


def election_juris_list(dir_path: str) -> list:
    """Return list of all election-jurisdiction pairs in .ini files in the given directory"""
    ej_list = []
    for f in os.listdir(dir_path):
        if f[-4:] == ".ini":
            d, err = get_parameters(
                param_file=os.path.join(dir_path, f),
                header="election_data_analysis",
                required_keys=["election", "top_reporting_unit"],
            )
            # if parameters were read without error
            if not err:
                # and if the pair is not already in the list
                if (d["election"], d["top_reporting_unit"]) not in ej_list:
                    # append the pair from the param file to the list
                    ej_list.append((d["election"], d["top_reporting_unit"]))
    return ej_list


def reload_juris_election(
    juris_name: str,
    election_name: str,
    test_dir: str,
    from_cron: bool = None,
) -> bool:
    """Assumes run_time.ini in directory, and results to be loaded are in the results_dir named in run_time.ini"""
    # initialize dataloader
    dl = e.DataLoader()
    db_params = get_parameters(
        ["host", "port", "dbname", "user", "password",],
        "run_time.ini",
        "postgresql",
    )[0]

    if from_cron != True:
        # Ask user to confirm/correct essential info
        confirm_essential_info(
            dl.d["results_dir"],
            "election_data_analysis",
            ["results_file", "results_download_date", "results_source"],
            known={
                "top_reporting_unit": juris_name,
                "jurisdiction_directory": juris_name.replace(" ", "-"),
                "election": election_name,
            },
            msg=" Check download date carefully!!!",
        )

    # create temp_db (preserving live db name)
    live_db = dl.session.bind.url.database
    ts = datetime.datetime.now().strftime("%m%d_%H%M")
    temp_db = f"{live_db}_test_{ts}"
    db_params["dbname"] = temp_db
    db.create_or_reset_db(dbname=temp_db)

    # load all data into temp db
    dl.change_db(temp_db)
    dl.load_all(move_files=False)

    # run test files on temp db
    _, results = run_tests(
        test_dir,
        dl.d["dbname"],
        election_jurisdiction_list=[(election_name, juris_name)],
    )

    go_ahead = "y"
    if not from_cron:
        # Ask user to OK test results or abandon
        go_ahead = input(
            f"Did tests succeeded for election {election_name} in {juris_name} (y/n)?"
        )
    if go_ahead != "y" or results != 0:
        print("Something went wrong, new data not loaded")
        # cleanup
        db.remove_database(db_params)
        return False

    # switch to live db and get info needed later
    dl.change_db(live_db)
    election_id = db.name_to_id(dl.session, "Election", election_name)
    juris_id = db.name_to_id(dl.session, "ReportingUnit", juris_name)

    # Move *.ini and results files for juris-election pair to 'unloaded' directory
    archive_directory = dl.d["archive_dir"]
    if dl.d["unloaded_dir"]:
        unloaded_directory = dl.d["unloaded_dir"]
    else:
        unloaded_directory = os.path.join(archive_directory, "unloaded")
    for f in [f for f in os.listdir(archive_directory) if f[-4:] == ".ini"]:
        param_file = os.path.join(archive_directory, f)
        params, err = get_parameters(
            required_keys=["election", "top_reporting_unit", "results_file"],
            header="election_data_analysis",
            param_file=param_file,
        )
        # if the *.ini file is for the given election and jurisdiction
        if (
            (not err)
            and params["election"] == election_name
            and params["top_reporting_unit"] == juris_name
        ):
            # move the *.ini file and its results file (and any aux_data_directory) to the unloaded directory
            archive_from_param_file(param_file, archive_directory, unloaded_directory)

    # Remove existing data for juris-election pair from live db
    dl.remove_data(election_id, juris_id, (not from_cron))

    # Load new data into live db (and move successful to archive)
    dl.load_all()

    # run tests on live db
    run_tests(
        test_dir,
        dl.d["dbname"],
        election_jurisdiction_list=[(election_name, juris_name)],
    )

    #cleanup
    db.remove_database(db_params)
    return True


def get_contest_type_mappings(filters: list) -> list:
    """get mappings for a list to the contest type database labels"""
    contest_types = contest_type_mappings.items()
    for index, item in enumerate(filters):
        for contest_type in contest_types:
            if item == contest_type[1]:
                filters[index] = contest_type[0]
                break
    return filters


def get_contest_type_mapping(item: str) -> str:
    """get mappings for a string to the contest type database labels"""
    contest_types = contest_type_mappings.items()
    for contest_type in contest_types:
        if contest_type[1] in item:
            return item.replace(contest_type[1], contest_type[0])
    return item


def get_contest_type_display(item: str) -> str:
    """get the user-friendly version of the contest_type"""
    item_list = item.split(" ")
    for index in range(len(item_list)):
        for key in contest_type_mappings.keys():
            if key == item_list[index]:
                item_list[index] = contest_type_mappings[key] 
                break
    return " ".join(item_list)
