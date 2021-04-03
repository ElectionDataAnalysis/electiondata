from configparser import ConfigParser, MissingSectionHeaderError
from election_data_analysis import special_formats as sf
from election_data_analysis import database as db
from election_data_analysis import munge as m
import election_data_analysis as e
import pandas as pd
from pandas.errors import ParserError
import os
from pathlib import Path
from election_data_analysis import juris_and_munger as jm
from typing import Optional, Dict, Any, List
import datetime
import csv
import numpy as np
import inspect

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


def pick_juris_from_filesystem(
    juris_path: str, err: Optional[dict], check_files: bool = False
):
    """Returns a Jurisdiction object. <juris_path> is the path to the directory containing the
    defining files for the particular jurisdiction.
    """
    new_err = None
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


def tabular_kwargs(
    p: Dict[str, Any], kwargs: Dict[str, Any], aux=False
) -> Dict[str, Any]:
    """kwargs["header"] is single integer (if just one header row)
    or list of integers (if more than one header row)"""
    # designate header rows (for both count columns or string-location info/columns)
    if p["all_rows"] == "data":
        kwargs["header"] = None
    else:
        header_rows = set()
        # if count_locations are by field name, need count_field_name_row
        if not aux and p["count_locations"] == "by_field_names":
            header_rows.update({p["count_field_name_row"]})
        # if any string locations are from field values AND field names are in the table, need string_field_name_row
        if "in_field_values" in p["munge_strings"]:
            header_rows.update({p["string_field_name_row"]})
        # if any string locations are in count headers need count_header_row_numbers
        if "in_count_headers" in p["munge_strings"]:
            header_rows.update(p["count_header_row_numbers"])
        if header_rows:
            # if multi-index
            if len(header_rows) > 1:
                kwargs["header"] = sorted(header_rows)
            else:
                kwargs["header"] = header_rows.pop()
        else:
            kwargs["header"] = 0

    # designate rows to skip
    if p["rows_to_skip"]:
        kwargs["skiprows"] = range(p["rows_to_skip"])
    return kwargs


def basic_kwargs(p: Dict[str, Any], kwargs: Dict[str, Any]) -> Dict[str, Any]:
    # ensure that all field values will be read as strings
    kwargs["dtype"] = "string"

    # other parameters
    kwargs["index_col"] = False
    if p["thousands_separator"]:
        kwargs["thousands"] = p["thousands_separator"]
    if p["file_type"] in ["flat_text"]:
        if p["encoding"] is None:
            kwargs["encoding"] = m.default_encoding
        else:
            kwargs["encoding"] = p["encoding"]

    return kwargs


def list_desired_excel_sheets(f_path: str, p: dict) -> Optional[list]:
    if p["sheets_to_read_names"]:
        sheets_to_read = p["sheets_to_read_names"]
    else:
        xl = pd.ExcelFile(f_path)
        all_sheets = xl.sheet_names
        if p["sheets_to_skip_names"]:
            sheets_to_read = [
                s for s in all_sheets if s not in p["sheets_to_skip_names"]
            ]
        elif p["sheets_to_read_numbers"]:
            sheets_to_read = [all_sheets[n] for n in p["sheets_to_read_numbers"]]
        else:
            sheets_to_read = all_sheets
    return sheets_to_read


def read_single_datafile(
    f_path: str,
    p: Dict[str, Any],
    munger_name: str,
    err: Optional[Dict],
    aux: bool = False,
) -> (Dict[str, pd.DataFrame], dict):
    """Length of returned dictionary is the number of sheets read -- usually 1 except for multi-sheet Excel.
    Auxiliary files have different parameters (e.g., no count locations)"""
    kwargs = dict()  # for syntax checker
    df_dict = dict()  # for syntax checker
    file_name = Path(f_path).name

    # prepare keyword arguments for pandas read_* function
    if p["file_type"] in ["excel"]:
        kwargs = basic_kwargs(p, dict())
        kwargs = tabular_kwargs(p, kwargs, aux=aux)
    elif p["file_type"] in ["flat_text"]:
        kwargs = basic_kwargs(p, dict())
        kwargs = tabular_kwargs(p, kwargs, aux=aux)
        kwargs["quoting"] = csv.QUOTE_MINIMAL
        if p["flat_text_delimiter"] in ["tab", "\\t"]:
            kwargs["sep"] = "\t"
        else:
            kwargs["sep"] = p["flat_text_delimiter"]

    # read file
    try:
        if p["file_type"] in ["xml"]:
            df, err = sf.read_xml(f_path, p, munger_name, err)
            if not fatal_error(err):
                df_dict = {"Sheet1": df}
        elif p["file_type"] in ["json-nested"]:
            df, err = sf.read_nested_json(f_path, p, munger_name, err)
            if not fatal_error(err):
                df_dict = {"Sheet1": df}
        elif p["file_type"] == "excel":
            df_dict, err = excel_to_dict(
                f_path, kwargs, list_desired_excel_sheets(f_path, p)
            )
            if fatal_error(err):
                df_dict = dict()
        elif p["file_type"] == "flat_text":
            try:
                df = pd.read_csv(f_path, **kwargs)
            except ValueError as ve:
                print(
                    f"ValueError (while reading flat text file), possibly from uneven record lengths: {ve}\n "
                    f"Will try padding records"
                )
                # read file again, assuming no header rows
                ## and set any nulls to blank strings
                kwargs_pad = kwargs
                kwargs_pad["index_col"] = None
                kwargs_pad["header"] = None
                df = pd.read_csv(f_path, **kwargs_pad).fillna("")
                # set headers per munger
                header_int_or_list = tabular_kwargs(p, dict())["header"]
                if isinstance(
                    header_int_or_list, int
                ):  # TODO tech debt ugly! but tracks index vs. multiindex
                    header_list = [header_int_or_list]
                else:
                    header_list = header_int_or_list
                try:
                    df = set_and_fill_headers(df, header_list)
                except Exception as exc:
                    err = add_new_error(
                        err,
                        "system",
                        f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
                        f"Unexpected error setting and filling headers after padding file {file_name}",
                    )

            df_dict = {"Sheet1": df}

        # rename any columns from header-less tables to column_0, column_1, etc.
        if p["all_rows"] == "data":
            for k in df_dict.keys():
                df_dict[k].columns = [f"column_{j}" for j in range(df_dict[k].shape[1])]

    except FileNotFoundError:
        err_str = f"File not found: {f_path}"
        err = add_new_error(err, "file", file_name, err_str)
    except UnicodeDecodeError as ude:
        err_str = f"Encoding error. Datafile not read completely.\n\t{ude}"
        err = add_new_error(err, "file", file_name, err_str)
    except ParserError as pe:
        # DFs have trouble comparing against None. So we return an empty DF and
        # check for emptiness below as an indication of an error.
        err_str = f"Error parsing results file.\n{pe}"
        err = add_new_error(
            err,
            "file",
            file_name,
            err_str,
        )
        err = add_new_error(err, "file", f_path, err_str)

    # drop any empty dataframes
    df_dict = {k: v for k, v in df_dict.items() if not v.empty}
    return df_dict, err


def excel_to_dict(
    f_path: str, kwargs: Dict[str, Any], sheet_list: Optional[List[str]]
) -> (Dict[str, pd.DataFrame], dict):
    kwargs["index_col"] = None
    #  need to omit index_col here since multi-index headers are possible
    # to avoid getting fatal error when a sheet doesn't read in correctly
    df_dict = dict()
    error: Optional[dict] = None
    for sheet in sheet_list:
        try:
            df_dict[sheet] = pd.read_excel(f_path, **kwargs, sheet_name=sheet)
            # ignore any empty sheet
            if df_dict[sheet].empty:
                df_dict.pop(sheet)
                file_name = Path(f_path).name
                error = add_new_error(
                    error, "file", file_name, f"No data read from sheet {sheet}"
                )
        except Exception as exc:
            df_dict[sheet] = pd.DataFrame()
            error = add_new_error(
                error,
                "warn-file",
                Path(f_path).name,
                f"Sheet {sheet} not read due to exception:\n\t{exc}",
            )
    return df_dict, error


def add_err_df(err, err_df, munger_name, f_path):
    # show all columns of dataframe holding rows where counts were set to 0
    pd.set_option("max_columns", None)
    err = add_new_error(
        err,
        "warn-munger",
        munger_name,
        f"At least one count was set to 0 in certain rows of {Path(f_path).name}:\n{err_df}",
    )
    pd.reset_option("max_columns")
    return err


def archive_from_param_file(param_file: str, current_dir: str, archive_dir: str):
    params, err = get_parameters(
        required_keys=["results_file", "aux_data_dir"],
        header="election_data_analysis",
        param_file=os.path.join(current_dir, param_file),
    )
    # TODO error handling
    # if the ini file specifies an aux_data_directory
    if (
        "aux_data_dir" in params.keys()
        and params["aux_data_dir"]
        and params["aux_data_dir"] != ""
    ):
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


def get_parameters(
    required_keys: List[str],
    param_file: str,
    header: str,
    err: Optional[Dict] = None,
    optional_keys: Optional[List[str]] = None,
) -> (Dict[str, str], Optional[Dict[str, dict]]):
    """Collects the values of the parameters corresponding to the <required_keys>
    and <optional_keys> into a dictionary."""
    d = dict()
    err = None

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


def get_section_headers(param_file: str) -> (List[str], Optional[dict]):
    err = None
    # read info from file
    parser = ConfigParser()
    # find header
    p = parser.read(param_file)
    if len(p) == 0:
        err = add_new_error(err, "file", param_file, "File not found")
        return list(), err
    headers = parser.sections()
    return headers, err


def consolidate_errors(list_of_err: Optional[list]) -> Optional[Dict[Any, dict]]:
    """Takes two error dictionaries (assumed to have same bottom-level keys)
    and consolidates them, concatenating the error messages"""
    """Consolidate the error dictionaries in <list_of_err>. If any dictionary is None, ignore it.
    If all dictionaries are None, return None"""
    if list_of_err is None:
        return None
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
    loc_dict: Optional[Dict[Any, str]] = None,
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

        # map each tuple to its message (sorting the warnings)
        msg = {(et, nk): "\n".join(sorted(err_warn[et][nk])) for et, nk in tuples}

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


def fatal_to_warning(err: Optional[Dict[Any, dict]]) -> Optional[Dict[Any, dict]]:
    """Returns the same dictionary, but with all fatal errors downgraded to nonfatal errors"""
    non_fatal_err = None
    for k in err.keys():
        if k[:5] == "warn-":
            for j in err[k].keys():
                non_fatal_err = add_new_error(non_fatal_err, k, j, err[k][j])
        else:
            for j in err[k].keys():
                non_fatal_err = add_new_error(
                    non_fatal_err,
                    f"warn-{k}",
                    j,
                    err[k][j],
                )
    return non_fatal_err


def add_new_error(
    err: Optional[Dict[str, dict]], err_type: str, key: str, msg: str
) -> Optional[Dict[str, dict]]:
    """err is a dictionary of dictionaries, one for each err_type.
    This function returns err, augmented by the error specified in <err_type>,<key> and <msg>"""
    if err is None or err == dict():
        err = {k: {} for k in warning_keys.union(error_keys)}
        # TODO document. Problems with results file are reported with "ini" key
    if err_type not in err.keys():
        err = add_new_error(
            err,
            "system",
            f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
            f"Unrecognized key ({err_type}) for message {msg}",
        )
        return err
    if key in err[err_type].keys():
        err[err_type][key].append(msg)
    else:
        err[err_type][key] = [msg]
    return err


def fatal_err_to_non(err: Optional[Dict[Any, dict]]) -> Optional[Dict[Any, dict]]:
    """Returns the same dictionary, but with all fatal errors downgraded to nonfatal errors"""
    if err:
        non_fatal_err = err.copy()
        for k in err.keys():
            if k[:5] != "warn-":
                non_fatal_err[f"warn-{k}"] = non_fatal_err.pop(k)
    else:
        non_fatal_err = None
    return non_fatal_err


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


def run_tests(
    test_dir: str, dbname: str, election_jurisdiction_list: Optional[list] = None
) -> (dict, int):
    """move to tests directory, run tests, move back
    db_params must have host, user, pass, db_name.
    test_param_file is a reference run_time.ini file"""

    r = -1
    # note current directory
    original_dir = os.getcwd()

    # move to tests directory
    os.chdir(test_dir)

    result = dict()  # initialize result report
    # run pytest
    if election_jurisdiction_list is None:
        r = os.system(f"pytest --dbname {dbname}")
        if r != 0:
            result["all"] = "At least one test failed"
    else:
        for (election, juris) in election_jurisdiction_list:
            if election is None and juris is not None:
                keyword = f"_{juris.replace(' ', '-')}"
            elif juris is None and election is not None:
                keyword = f"{election.replace(' ', '-')}"
            elif juris is not None and election is not None:
                keyword = f"_{juris.replace(' ', '-')}_{election.replace(' ', '-')}"
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


def election_juris_list(ini_path: str, results_path: Optional[str] = None) -> list:
    """Return list of all election-jurisdiction pairs in .ini files in the ini_path directory
    or in any of its subdirectories. Ignores 'template.ini' If results_path is given, filters
    for ini files whose results files are in the results_path directory
    """
    ej_set = set()
    for subdir, dirs, files in os.walk(ini_path):
        for f in files:
            if (f[-4:] == ".ini") and (f != "template.ini"):
                full_path = os.path.join(subdir, f)
                d, err = get_parameters(
                    param_file=full_path,
                    header="election_data_analysis",
                    required_keys=["election", "top_reporting_unit", "results_file"],
                )
                # if parameters were read without error
                if not err:
                    # if we're not checking against results directory, or if we are and the ini file
                    #  points to a file in or below the results directory
                    if (not results_path) or (
                        os.path.isfile(os.path.join(results_path, d["results_file"]))
                    ):
                        # include the pair in the output
                        ej_set.update({(d["election"], d["top_reporting_unit"])})
    return list(ej_set)


def file_full_paths(dir_path: str, ext: str) -> List[str]:
    """Return list of full paths all .ini files in the directory or its subdirectories"""
    ini_paths = list()
    for subdir, dirs, files in os.walk(dir_path):
        for f in files:
            if (f[-len(ext) - 1 :] == f".{ext}") and (f != f"template.{ext}"):
                ini_paths.append(os.path.join(subdir, f))
    return ini_paths


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
        [
            "host",
            "port",
            "dbname",
            "user",
            "password",
        ],
        "run_time.ini",
        "postgresql",
    )[0]

    if not from_cron:
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
    err_str = dl.add_totals_if_missing(election_name, juris_name)
    if err_str:
        print(f"Error while adding totals: {err_str}")

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
    if go_ahead != "y" or results != 0 or err_str:
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
    err_str = dl.add_totals_if_missing(election_name, juris_name)

    # run tests on live db
    run_tests(
        test_dir,
        dl.d["dbname"],
        election_jurisdiction_list=[(election_name, juris_name)],
    )

    # cleanup
    db.remove_database(db_params)
    return True


def get_contest_type_mappings(filters: list) -> Optional[list]:
    """get mappings for a list to the contest type database labels"""
    if not filters:
        return None
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


def get_filtered_input_options(session, input_str, filters):
    """ Display dropdown options for user selection """
    df_cols = ["parent", "name", "type"]
    if input_str == "election":
        if filters:
            election_df = db.get_relevant_election(session, filters)
            elections = list(election_df["Name"].unique())
            elections.sort(reverse=True)
            data = {
                "parent": [filters[0] for election in elections],
                "name": elections,
                "type": [None for election in elections],
            }
            df = pd.DataFrame(data=data)
            df[["year", "election_type"]] = df["name"].str.split(" ", expand=True)
            df.sort_values(
                ["year", "election_type"], ascending=[False, True], inplace=True
            )
            df.drop(columns=["year", "election_type"], inplace=True)
        else:
            df = db.display_elections(session)
    elif input_str == "jurisdiction":
        df = db.display_jurisdictions(session, df_cols)
        if filters:
            df = df[df["parent"].isin(filters)]
    elif input_str == "contest_type":
        contest_df = db.get_relevant_contests(session, filters)
        contest_types = contest_df["type"].unique()
        contest_types.sort()
        data = {
            "parent": [filters[0] for contest_type in contest_types],
            "name": contest_types,
            "type": [None for contest_type in contest_types],
        }
        df = pd.DataFrame(data=data)
    elif input_str == "contest":
        contest_type = list(set(db.contest_types_model) & set(filters))[0]

        connection = session.bind.raw_connection()
        cursor = connection.cursor()
        reporting_unit_id = db.list_to_id(session, "ReportingUnit", filters)
        reporting_unit = db.name_from_id_cursor(
            cursor, "ReportingUnit", reporting_unit_id
        )
        connection.close()

        contest_type_df = pd.DataFrame(
            [
                {
                    "parent": reporting_unit,
                    "name": f"All {contest_type_mappings[contest_type]}",
                    "type": contest_type,
                }
            ]
        )
        contest_df = db.get_relevant_contests(session, filters)
        contest_df = contest_df[contest_df["type"].isin(filters)]
        df = pd.concat([contest_type_df, contest_df])
    elif input_str == "category":
        election_id = db.list_to_id(session, "Election", filters)
        reporting_unit_id = db.list_to_id(session, "ReportingUnit", filters)

        # get the census data
        connection = session.bind.raw_connection()
        cursor = connection.cursor()
        election = db.name_from_id_cursor(cursor, "Election", election_id)
        census_df = db.read_external(
            cursor, int(election[0:4]), reporting_unit_id, ["Label"]
        )
        cursor.close()
        if census_df.empty:
            census = []
        else:
            census = ["Census data"]

        type_df = db.read_vote_count(
            session,
            election_id,
            reporting_unit_id,
            ["CountItemType"],
            ["CountItemType"],
        )
        count_types = list(type_df["CountItemType"].unique())
        count_types.sort()
        data = {
            "parent": [filters[0] for count_type in count_types]
            + [filters[0] for count_type in count_types]
            + [filters[0] for count_type in count_types]
            + [filters[0] for c in census],
            "name": [f"Candidate {count_type}" for count_type in count_types]
            + [f"Contest {count_type}" for count_type in count_types]
            + [f"Party {count_type}" for count_type in count_types]
            + [c for c in census],
            "type": [None for count_type in count_types]
            + [None for count_type in count_types]
            + [None for count_type in count_types]
            + [None for c in census],
        }
        df = pd.DataFrame(data=data)
    # check if it's looking for a count of contests
    elif input_str == "count" and bool([f for f in filters if f.startswith("Contest")]):
        election_id = db.list_to_id(session, "Election", filters)
        reporting_unit_id = db.list_to_id(session, "ReportingUnit", filters)
        df = db.read_vote_count(
            session,
            election_id,
            reporting_unit_id,
            ["ReportingUnitName", "ContestName", "unit_type"],
            ["parent", "name", "type"],
        )
        df = df.sort_values(["parent", "name"]).reset_index(drop=True)
    # check if it's looking for a count of candidates
    elif input_str == "count" and bool(
        [f for f in filters if f.startswith("Candidate")]
    ):
        election_id = db.list_to_id(session, "Election", filters)
        reporting_unit_id = db.list_to_id(session, "ReportingUnit", filters)
        df_unordered = db.read_vote_count(
            session,
            election_id,
            reporting_unit_id,
            ["ContestName", "BallotName", "PartyName", "unit_type"],
            ["parent", "name", "type", "unit_type"],
        )
        df = clean_candidate_names(df_unordered)
        df = df[["parent", "name", "unit_type"]].rename(columns={"unit_type": "type"})
    # check if it's looking for census data
    elif input_str == "count" and "Census data" in filters:
        election_id = db.list_to_id(session, "Election", filters)
        reporting_unit_id = db.list_to_id(session, "ReportingUnit", filters)
        connection = session.bind.raw_connection()
        cursor = connection.cursor()
        election = db.name_from_id_cursor(cursor, "Election", election_id)
        df = db.read_external(
            cursor,
            int(election[0:4]),
            reporting_unit_id,
            ["Source", "Label", "Category"],
        )
        cursor.close()
    # check if it's looking for a count by party
    elif input_str == "count":
        election_id = db.list_to_id(session, "Election", filters)
        reporting_unit_id = db.list_to_id(session, "ReportingUnit", filters)
        df = db.read_vote_count(
            session,
            election_id,
            reporting_unit_id,
            ["PartyName", "unit_type"],
            ["parent", "type"],
        )
        df["name"] = df["parent"].str.replace(" Party", "") + " " + df["type"]
        df = df[df_cols].sort_values(["parent", "type"])
    # Otherwise search for candidate
    else:
        election_id = db.list_to_id(session, "Election", filters)
        reporting_unit_id = db.list_to_id(session, "ReportingUnit", filters)
        df_unordered = db.read_vote_count(
            session,
            election_id,
            reporting_unit_id,
            ["ContestName", "BallotName", "PartyName", "unit_type"],
            ["parent", "name", "type", "unit_type"],
        )
        df_unordered = df_unordered[df_unordered["unit_type"].isin(filters)].copy()
        df_filtered = df_unordered[
            df_unordered["name"].str.contains(input_str, case=False)
        ].copy()
        df = clean_candidate_names(df_filtered[df_cols].copy())
    # TODO: handle the "All" and "other" options better
    # TODO: handle sorting numbers better
    return package_display_results(df)


def package_display_results(data):
    """takes a result set and packages into JSON to return"""
    results = []
    for i, row in data.iterrows():
        if row[1] in contest_type_mappings:
            row[1] = contest_type_mappings[row[1]]
        temp = {"parent": row[0], "name": row[1], "type": row[2], "order_by": i + 1}
        results.append(temp)
    return results


def clean_candidate_names(df):
    """takes a df that has contest, candidate name, and party in the columns. Cleans the
    data as described in https://github.com/ElectionDataAnalysis/election_data_analysis/issues/207"""
    # Get first letter of each word in the party name except for "Party"
    # if "Party" is not in the name, then it's "None"
    cols = df.columns
    df_cols = ["parent", "name", "type"]
    extra_cols = [col for col in cols if col not in df_cols]
    extra_df = df[extra_cols]
    df = df[df_cols]
    df["party"] = df["type"].str.split(" ")
    df["party"] = np.where(
        df["party"].str.contains("party", case=False),
        df["party"]
        .map(lambda x: x[0:-1])
        .map(lambda words: "".join([word[0] for word in words])),
        "None",
    )

    # create the abbreviated contest name
    df["contest"] = df["parent"].str.replace(r"\(.*\)", "")
    df["jurisdiction"] = df["contest"].map(lambda x: x[0:2])
    mask_us_pres = df["contest"].str.contains("president", case=False)
    mask_us_sen = (df["jurisdiction"] == "US") & (
        df["contest"].str.contains("senate", case=False)
    )
    mask_us_house = (df["jurisdiction"] == "US") & (
        df["contest"].str.contains("house", case=False)
    )
    mask_st_sen = (df["jurisdiction"] != "US") & (
        df["contest"].str.contains("senate", case=False)
    )
    mask_st_house = (df["jurisdiction"] != "US") & (
        df["contest"].str.contains("house", case=False)
    )
    df["chamber"] = None
    df.loc[mask_us_pres, "chamber"] = "Pres"
    df.loc[mask_us_sen, "chamber"] = "Sen"
    df.loc[mask_us_house, "chamber"] = "House"
    df.loc[mask_st_sen, "chamber"] = "S"
    df.loc[mask_st_house, "chamber"] = "H"
    df["chamber"] = df["chamber"].fillna("unknown")
    df["district"] = df["contest"].str.extract(r"(\d+)")
    df["contest_short"] = ""
    df["contest_short"] = np.where(
        df["chamber"] != "unknown",
        df[df.columns[5:]].apply(lambda x: "".join(x.dropna().astype(str)), axis=1),
        df["contest_short"],
    )
    df["contest_short"] = np.where(
        df["chamber"] == "unknown",
        df["contest"]
        .str.split(" ")
        .map(lambda words: "".join([word[0:3] for word in words if word != "of"])),
        df["contest_short"],
    )
    # Handle GA 2020 runoff senate elections
    df["contest_short"] = np.where(
        df["parent"].str.contains("runoff"),
        df["contest_short"] + "Runoff",
        df["contest_short"],
    )
    df["name"] = df[["name", "party", "contest_short"]].apply(
        lambda x: " - ".join(x.dropna().astype(str)), axis=1
    )
    df = df.sort_values(by=["contest_short", "party", "name"])
    df = df[df_cols].merge(extra_df, how="inner", left_index=True, right_index=True)
    df.reset_index(drop=True, inplace=True)
    return df


def set_and_fill_headers(df: pd.DataFrame, header_list: list) -> pd.DataFrame:
    # correct column headers
    # standardize the index and columns to 0, 1, 2, ...
    df = df.reset_index(drop=True).T.reset_index(drop=True).T
    # rename any leading blank header entries to match convention of pd.read_excel, and any trailing to
    # closest non-blank value to left
    for i in header_list:
        prev_non_blank = None
        for j in df.columns:
            if df.loc[i, j] == "":
                if prev_non_blank:
                    df.loc[i, j] = prev_non_blank
                else:
                    df.loc[i, j] = f"Unnamed: {j}_level_{i}"
            else:
                prev_non_blank = df.loc[i, j]

    # push appropriate rows into headers
    df = df.reset_index(drop=True).T.set_index(header_list).T
    return df


def check_results_ini_params(
    p: Dict[str, Any],
    ini_file_path: str,
) -> Optional[dict]:
    """Checks results parameters"""
    err_str = None
    ini_file_name = Path(ini_file_path).name
    try:
        datetime.datetime.strptime(p["results_download_date"], "%Y-%m-%d")
    except TypeError:
        err_str = f"No download date found"
    except ValueError:
        err_str = f"Date could not be parsed. Expected format is 'YYYY-MM-DD', actual is {p['results_download_date']}"
    if err_str:
        ini_err = add_new_error(None, "ini", ini_file_name, err_str)
    else:
        ini_err = None
    return ini_err
