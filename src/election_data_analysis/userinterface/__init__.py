from configparser import (
    ConfigParser,
    MissingSectionHeaderError,
    DuplicateOptionError,
    ParsingError,
)

from election_data_analysis import (
    database as db,
    munge as m,
    juris as jm,
    nistformats as nist,
)
import pandas as pd
from pandas.errors import ParserError
import os
from pathlib import Path
from typing import Optional, Dict, Any, List
import datetime
import csv
import numpy as np
import inspect
import xml.etree.ElementTree as et
import json
import shutil
import xlrd

# import openpyxl
from sqlalchemy.orm import Session

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
    "database",
    "test",
}
warning_keys = {f"warn-{ek}" for ek in error_keys}

# mapping from internal database reportingunit types to the user-facing contest types
# (contests are categorized by the reporting unit type of their corresponding districts)


def find_dupes(df):
    dupes_df = df[df.duplicated()].drop_duplicates(keep="first")
    deduped = df.drop_duplicates(keep="first")
    return dupes_df, deduped


def json_kwargs(
    munge_fields: List[str],
    driving_path: str,
    driver_new_col_name: str,
) -> (Dict[str, Any], Dict[str, str]):
    meta_set = set()
    json_rename: Dict[str, str] = dict()
    record_path = driving_path.split("/")[:-1]
    driver_old_col_name = driving_path.split("/")[-1]
    json_rename[driver_old_col_name] = driver_new_col_name
    for mf in munge_fields:
        path_list = mf.split(".")
        if path_list[0] not in record_path:
            meta_set.update({tuple(path_list)})
        elif len(path_list) > 2:
            meta_set.update({tuple(path_list)})
        else:
            json_rename[path_list[-1]] = mf
    meta = list(list(t) for t in meta_set)
    json_kwargs = {"meta": meta, "record_path": record_path, "errors": "ignore"}

    return json_kwargs, json_rename


def tabular_kwargs(
    p: Dict[str, Any], kwargs: Dict[str, Any], aux=False
) -> Dict[str, Any]:
    """kwargs["header"] is single integer (if just one header row)
    or list of integers (if more than one header row)"""
    # designate header rows (for both count columns or string-location info/columns)
    if p["all_rows"] == "data" or p["multi_block"] == "yes":
        kwargs["header"] = None
    else:
        header_rows = set()
        # if count_location is by name, need count_field_name_row
        if not aux and p["count_location"] == "by_name":
            header_rows.update({p["count_field_name_row"]})
        # need noncount_header_row
        if isinstance(p["noncount_header_row"], int):
            header_rows.update({p["noncount_header_row"]})
        #  need count_header_row_numbers
        if p["count_header_row_numbers"]:
            header_rows.update(p["count_header_row_numbers"])

        # define header parameter for reading file
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


def get_row_constant_kwargs(kwargs: dict, rows_to_read: List[int]) -> dict:
    rck = kwargs.copy()
    rck["header"] = None
    rck["nrows"] = max(rows_to_read) + 1
    return rck


def list_desired_excel_sheets(f_path: str, p: dict) -> (Optional[list], Optional[dict]):
    err = None
    file_name = Path(f_path).name
    if p["sheets_to_read_names"]:
        sheets_to_read = p["sheets_to_read_names"]
    else:
        try:
            # read an xlsx file
            # # nb: the following fails on VT 2020 files
            xl = pd.ExcelFile(f_path)
            all_sheets = xl.sheet_names
            # xlsx = openpyxl.load_workbook(f_path)
            # all_sheets = xlsx.get_sheet_names()
        except Exception as exc:
            try:
                # read xls file
                xls = xlrd.open_workbook(f_path, on_demand=True)
                all_sheets = xls.sheet_names()
            except Exception as exc:
                err = add_new_error(
                    err,
                    "file",
                    file_name,
                    f"Error reading sheet names from Excel file ({f_path}): {exc}",
                )
                sheets_to_read = None
                return sheets_to_read, err
        if p["sheets_to_skip_names"]:
            sheets_to_read = [
                s for s in all_sheets if s not in p["sheets_to_skip_names"]
            ]
        elif p["sheets_to_read_numbers"]:
            sheets_to_read = [all_sheets[n] for n in p["sheets_to_read_numbers"]]
        else:
            sheets_to_read = all_sheets
    return sheets_to_read, err


def read_single_datafile(
    f_path: str,
    p: Dict[str, Any],
    munger_path: str,
    err: Optional[Dict],
    aux: bool = False,
    driving_path: Optional[str] = None,
    lookup_id: Optional[str] = None,
) -> (Dict[str, pd.DataFrame], Dict[str, Dict[int, Any]], Optional[dict]):
    """Length of returned dictionary is the number of sheets read -- usually 1 except for multi-sheet Excel.
    Auxiliary files have different parameters (e.g., no count locations)"""
    err = None
    kwargs = dict()  # for syntax checker
    df_dict = dict()  # for syntax checker
    row_constants = dict()  # for syntax checker
    rename = dict()  # for syntax checker
    file_name = Path(f_path).name
    munger_name = Path(munger_path).stem

    # prepare keyword arguments for pandas read_* function
    if p["file_type"] in ["excel"]:
        kwargs = basic_kwargs(p, dict())
        kwargs = tabular_kwargs(p, kwargs, aux=aux)
        if p["multi_block"] == "yes":
            kwargs["header"] = None
    elif p["file_type"] in ["flat_text"]:
        kwargs = basic_kwargs(p, dict())
        kwargs = tabular_kwargs(p, kwargs, aux=aux)
        if p["multi_block"] == "yes":
            kwargs["header"] = None
        kwargs["quoting"] = csv.QUOTE_MINIMAL
        if p["flat_text_delimiter"] in ["tab", "\\t"]:
            kwargs["sep"] = "\t"
        else:
            kwargs["sep"] = p["flat_text_delimiter"]
    elif p["file_type"] in ["json-nested"]:
        kwargs, rename = json_kwargs(p["munge_fields"], p["count_location"], "Count")
    # read file
    try:
        if p["file_type"] in ["xml"]:
            if driving_path:
                temp_d = nist.tree_parse_info(driving_path,None)
                driver = {"main_path": temp_d["path"], "main_attrib": temp_d["attrib"]}
            else:
                driver = nist.xml_count_parse_info(p,ignore_namespace=True)
            xml_path_info = nist.xml_string_path_info(p["munge_fields"],p["namespace"])
            tree = et.parse(f_path)
            df, err = nist.df_from_tree(
                tree,
                xml_path_info=xml_path_info,
                file_name=file_name,
                **driver,
                ns=p["namespace"],
                lookup_id=lookup_id,
            )
            if not fatal_error(err):
                df_dict = {"Sheet1": df}
        elif p["file_type"] in ["json-nested"]:
            # TODO what if json-nested is a lookup?
            with open(f_path, "r") as f:
                data = json.loads(f.read())
            df = pd.json_normalize(data, **kwargs)
            if not fatal_error(err):
                df.rename(columns=rename, inplace=True)
                df_dict = {"Sheet1": df}

        elif p["file_type"] == "excel":
            desired_sheets, new_err = list_desired_excel_sheets(f_path, p)
            if new_err:
                err = consolidate_errors([err, new_err])
                if fatal_error(new_err):
                    df_dict = dict()
                    return df_dict, row_constants, err
            df_dict, row_constants, new_err = excel_to_dict(
                f_path,
                kwargs,
                desired_sheets,
                p["rows_with_constants"],
            )
            if fatal_error(new_err):
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
                    merged_cells = p["merged_cells"] == "yes"
                    df = set_and_fill_headers(
                        df, header_list, merged_cells, drop_empties=False
                    )
                except Exception as exc:
                    err = add_new_error(
                        err,
                        "system",
                        f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
                        f"Unexpected error setting and filling headers after padding file {file_name}",
                    )

            df_dict = {"Sheet1": df}
            # get the row constants
            if p["rows_with_constants"]:
                row_constant_kwargs = get_row_constant_kwargs(
                    kwargs, p["rows_with_constants"]
                )
                row_df = pd.read_csv(f_path, **row_constant_kwargs)
                row_constants["Sheet1"], new_err = build_row_constants_from_df(
                    row_df, p["rows_with_constants"], file_name, "Sheet1"
                )
                if new_err:
                    err = consolidate_errors([err, new_err])

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
    except Exception as exc:
        err = add_new_error(
            err,
            "file",
            file_name,
            f"Unknown exception while reading file using munger {munger_name}: {exc}",
        )
    else:
        # drop any empty dataframes
        df_dict = {k: v for k, v in df_dict.items() if not v.empty}
    return df_dict, row_constants, err


def excel_to_dict(
    f_path: str,
    kwargs: Dict[str, Any],
    sheet_list: Optional[List[str]],
    rows_to_read: List[int],
) -> (Dict[str, pd.DataFrame], Dict[str, Dict[str, Any]], Optional[dict]):
    """Returns dictionary of dataframes (one for each sheet), dictionary of dictionaries of constant values
    (one dictionary for each sheet) and error."""
    kwargs["index_col"] = None
    #  need to omit index_col here since multi-index headers are possible
    # to avoid getting fatal error when a sheet doesn't read in correctly
    df_dict = dict()
    row_constants = dict()
    row_constant_kwargs = dict()
    file_name = Path(f_path).name
    err = None
    try:
        if rows_to_read:
            row_constant_kwargs = get_row_constant_kwargs(kwargs, rows_to_read)
    except Exception as exc:
        err = add_new_error(
            err,
            "system",
            f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
            f"Unexpected exception while getting row-constant keyword arguments for \n"
            f"rows_to_read: {rows_to_read}\n"
            f"kwargs: {kwargs}.\n"
            f"Exception: {exc}",
        )
    for sheet in sheet_list:
        try:
            df_dict[sheet] = pd.read_excel(f_path, **kwargs, sheet_name=sheet)
            # ignore any empty sheet
            if df_dict[sheet].empty:
                df_dict.pop(sheet)
                err = add_new_error(
                    err, "file", file_name, f"No data read from sheet {sheet}"
                )
        except Exception as exc:
            df_dict[sheet] = pd.DataFrame()
            err = add_new_error(
                err,
                "warn-file",
                Path(f_path).name,
                f"Sheet {sheet} not read due to exception:\n\t{exc}",
            )

        try:
            if rows_to_read:
                row_constant_df = pd.read_excel(
                    f_path, **row_constant_kwargs, sheet_name=sheet
                )
                row_constants[sheet], new_err = build_row_constants_from_df(
                    row_constant_df, rows_to_read, file_name, sheet
                )
                if new_err:
                    err = consolidate_errors([err, new_err])
        except Exception as exc:
            err = add_new_error(
                err,
                "file",
                file_name,
                f"Exception while reading rows {rows_to_read} from {sheet}\n"
                f"with keyword arguments {row_constant_kwargs}\n"
                f"in ui.excel_to_dict():\n"
                f"{exc}",
            )
    return df_dict, row_constants, err


def build_row_constants_from_df(
    df: pd.DataFrame, rows_to_read: List[int], file_name: str, sheet: str
) -> (Dict[int, Any], Optional[dict]):
    """Returns first entries in rows corresponding to row_list
    (as a dictionary with rows in row_list as keys)"""
    working = df.reset_index(drop=True)
    row_constants = dict()
    err = None
    for row in rows_to_read:
        try:
            first_valid_idx = working.loc[row].fillna("").first_valid_index()
            row_constants[row] = working.fillna("").loc[row, first_valid_idx]
        except KeyError as ke:
            err = add_new_error(
                err,
                "warn-file",
                file_name,
                f"In sheet {sheet} no data found in row_{row}",
            )
            row_constants[row] = ""
            continue
        except Exception as exc:
            err = add_new_error(
                err,
                "file",
                file_name,
                f"In sheet {sheet}: Unexpected exception while reading data from row_{row}: {exc}",
            )
    return row_constants, err


def copy_directory_with_backup(
    original_path: str,
    copy_path: str,
    backup_suffix: Optional[str] = None,
    report_error: bool = False,
) -> Optional[dict]:
    """copy entire directory <original_path> to <copy_path>. If
    <copy_path> exists, move it to a backup file whose name gets the suffix
    <backup_suffix>"""
    err = None
    # TODO
    # if the original to be copied is actually a directory
    if os.path.isdir(original_path):
        if backup_suffix:
            # make backup of anything with existing name
            if os.path.isdir(copy_path):
                shutil.move(copy_path, f"{copy_path}{backup_suffix}")
            elif os.path.isfile(copy_path):
                old_stem = Path(copy_path).stem
                backup_path = os.path.join(
                    Path(copy_path).parent,
                    f"{old_stem}{backup_suffix}.{Path(copy_path).suffix}",
                )
                copy_with_err_handling(
                    copy_path, backup_path, report_error=report_error
                )
        # copy original to desired location
        # # ensure parent directory exists
        Path(copy_path).parent.mkdir(parents=True, exist_ok=True)
        new_err = copy_with_err_handling(
            original_path, copy_path, report_error=report_error
        )
        err = consolidate_errors([err, new_err])
    # if the original is not a directory
    else:
        # throw error
        err = add_new_error(
            err,
            "warn-system",
            f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
            f"No such directory: {original_path}",
        )
    return err


def copy_with_err_handling(
    original_path: str, copy_path: str, report_error: bool = True
) -> Optional[dict]:
    err = None
    Path(copy_path).mkdir(parents=True, exist_ok=True)
    for root, dirs, files in os.walk(original_path, topdown=True):
        new_root = root.replace(original_path, copy_path)
        for f in files:
            old = os.path.join(root, f)
            new = os.path.join(new_root, f)
            try:
                shutil.copy(old, new)
            except Exception as she:
                if report_error:
                    err = add_new_error(
                        err,
                        "warn-file",
                        f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
                        f"Error while copying {old} to {new}:\n{she}",
                    )
        for d in dirs:
            Path(os.path.join(new_root, d)).mkdir(parents=True, exist_ok=True)
    return err


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
    except DuplicateOptionError as doe:
        err = add_new_error(
            err, "ini", param_file, f"Something is defined twice: {doe}"
        )
    except ParsingError as pe:
        err = add_new_error(
            err,
            "ini",
            param_file,
            pe,
        )
    except Exception as exc:
        err = add_new_error(
            err,
            "ini",
            param_file,
            exc,
        )

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
    output_location: str,
    key_list: list = None,
    file_prefix: str = "",
) -> Optional[dict]:
    """unpacks error dictionary <err> for reporting.
    <output_location> is directory for writing error files.
    Use <key_list> to report only on some keys, and return a copy of err_warn with those keys removed"""
    if err_warn:
        # create reporting directory if it does not exist
        if os.path.isfile(output_location):
            print(
                "Target directory for errors and warnings exists as a file. Nothing will be reported."
            )
            return None
        elif not os.path.isdir(output_location):
            Path(output_location).mkdir(parents=True, exist_ok=True)

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

                    # write info to a .errors or .errors file named for the name_key <nk>
                    out_path = os.path.join(
                        output_location, f"{file_prefix}_{nk_name}.errors"
                    )
                    with open(out_path, "a") as f:
                        f.write(out_str)
                    print(f"{et.title()} errors{and_warns} written to {out_path}")

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

                # write output
                # get timestamp
                ts = datetime.datetime.now().strftime("%m%d_%H%M")
                # write info to a .errors or .errors file named for the name_key <nk>
                out_path = os.path.join(
                    output_location, f"{file_prefix}{nk_name}.warnings"
                )
                with open(out_path, "a") as f:
                    f.write(out_str)
                print(f"{et.title()} warnings written to {out_path}")

        # define return dictionary with reported keys set to {} and othe keys preserved
        remaining = {k: v for k, v in err_warn.items() if k not in key_list}
        for k in key_list:
            remaining[k] = {}
    else:
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
    test_dir: str,
    dbname: str,
    election_jurisdiction_list: list,
    report_dir: Optional[str] = None,
    file_prefix: str = "",
) -> Dict[str, Any]:
    """run tests from test_dir
    db_params must have host, user, pass, db_name.
    test_param_file is a reference run_time.ini file.
    Returns dictionary of failures (keys are jurisdiction;election strings)"""

    failures = dict()  # initialize result report
    # run pytest

    for (election, juris) in election_jurisdiction_list:
        # run tests
        e_system = jm.system_name_from_true_name(election)
        j_system = jm.system_name_from_true_name(juris)
        test_file = os.path.join(test_dir, f"{j_system}/test_{j_system}_{e_system}.py")
        if not os.path.isfile(test_file):
            failures[f"{juris};{election}"] = f"No test file found: {test_file}"
            continue
        cmd = f"pytest --dbname {dbname} {test_file}"
        if report_dir:
            Path(report_dir).mkdir(exist_ok=True, parents=True)
            report_file = os.path.join(
                report_dir, f"{file_prefix}{j_system}_{e_system}.test_results"
            )
            cmd = f"{cmd} > {report_file}"
        r = os.system(cmd)
        if r != 0:
            failures[f"{juris};{election}"] = "At least one test failed"

    return failures


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
                    required_keys=["election", "jurisdiction", "results_file"],
                )
                # if parameters were read without error
                if not err:
                    # if we're not checking against results directory, or if we are and the ini file
                    #  points to a file in or below the results directory
                    if (not results_path) or (
                        os.path.isfile(os.path.join(results_path, d["results_file"]))
                    ):
                        # include the pair in the output
                        ej_set.update({(d["election"], d["jurisdiction"])})
    return list(ej_set)


def get_contest_type_mappings(filters: list) -> Optional[list]:
    """get mappings for a list to the contest type database labels"""
    if not filters:
        return None
    contest_types = db.contest_type_mappings.items()
    for index, item in enumerate(filters):
        for contest_type in contest_types:
            if item == contest_type[1]:
                filters[index] = contest_type[0]
                break
    return filters


def get_contest_type_mapping(item: str) -> str:
    """get mappings for a string to the contest type database labels"""
    contest_types = db.contest_type_mappings.items()
    for contest_type in contest_types:
        if contest_type[1] in item:
            return item.replace(contest_type[1], contest_type[0])
    return item


def get_contest_type_display(item: str) -> str:
    """get the user-friendly version of the contest_type"""
    item_list = item.split(" ")
    for index in range(len(item_list)):
        for key in db.contest_type_mappings.keys():
            if key == item_list[index]:
                item_list[index] = db.contest_type_mappings[key]
                break
    return " ".join(item_list)


def get_filtered_input_options(
    session: Session, menu_type: str, filters: List[str], repository_content_root: str
) -> List[Dict[str, Any]]:
    """Display dropdown menu options for menu <menu_type>, limited to any strings in <filters>
    (unless <filters> is None, in which case all are displayed. Sort as necessary"""
    df_cols = ["parent", "name", "type"]
    if menu_type == "election":
        if filters:
            election_df = db.get_relevant_election(session, filters)
            elections = list(election_df["Name"].unique())
            elections.sort(reverse=True)
            dropdown_options = {
                "parent": [filters[0] for election in elections],
                "name": elections,
                "type": [None for election in elections],
            }
            df = pd.DataFrame(data=dropdown_options)
            df["year"] = df["name"].str[:4]
            df["election_type"] = df["name"].str[5:]
            df.sort_values(
                ["year", "election_type"], ascending=[False, True], inplace=True
            )
            df.drop(columns=["year", "election_type"], inplace=True)
        else:
            df = db.display_elections(session)
    elif menu_type == "jurisdiction":
        df = db.display_jurisdictions(session, df_cols)
        if filters:
            df = df[df["parent"].isin(filters)]
    elif menu_type == "contest_type":
        contest_df = db.get_relevant_contests(session, filters, repository_content_root)
        contest_types = contest_df["type"].unique()
        contest_types.sort()
        dropdown_options = {
            "parent": [filters[0] for contest_type in contest_types],
            "name": contest_types,
            "type": [None for contest_type in contest_types],
        }
        df = pd.DataFrame(data=dropdown_options)
    elif menu_type == "contest":
        contest_type = list(set(db.contest_types_model) & set(filters))[0]

        connection = session.bind.raw_connection()
        cursor = connection.cursor()
        jurisdiction_id = db.list_to_id(session, "ReportingUnit", filters)
        reporting_unit = db.name_from_id_cursor(
            cursor, "ReportingUnit", jurisdiction_id
        )
        connection.close()

        # define input option for all contests of the given type
        contest_type_df = pd.DataFrame(
            [
                {
                    "parent": reporting_unit,
                    "name": f"All {db.contest_type_mappings[contest_type]}",
                    "type": contest_type,
                }
            ]
        ).sort_values(by="parent")
        # define input options for each particular contest
        contest_df = db.get_relevant_contests(session, filters, repository_content_root)
        contest_df = contest_df[contest_df["type"].isin(filters)]
        df = pd.concat([contest_type_df, contest_df])
    elif menu_type == "category":
        election_id = db.list_to_id(session, "Election", filters)
        jurisdiction_id = db.list_to_id(session, "ReportingUnit", filters)

        # get the census data categories
        connection = session.bind.raw_connection()
        cursor = connection.cursor()

        # TODO filter by major subdivision of jurisdiction
        population_df = db.read_external(
            cursor, election_id, jurisdiction_id, ["Category"]
        )
        cursor.close()
        if population_df.empty:
            population = []
        else:
            population = sorted(population_df.Category.unique())

        # get the vote count categories
        type_df = db.read_vote_count(
            session,
            election_id,
            jurisdiction_id,
            ["CountItemType"],
            ["CountItemType"],
        )
        count_types = list(type_df["CountItemType"].unique())
        count_types.sort()
        dropdown_options = {
            "parent": [filters[0] for count_type in count_types]
            + [filters[0] for count_type in count_types]
            + [filters[0] for count_type in count_types]
            + [filters[0] for c in population],
            "name": [f"Candidate {count_type}" for count_type in count_types]
            + [f"Contest {count_type}" for count_type in count_types]
            + [f"Party {count_type}" for count_type in count_types]
            + [c for c in population],
            "type": [None for count_type in count_types]
            + [None for count_type in count_types]
            + [None for count_type in count_types]
            + [None for c in population],
        }
        df = pd.DataFrame(data=dropdown_options)
    # check if it's looking for a count of contests
    elif menu_type == "count" and bool([f for f in filters if f.startswith("Contest")]):
        election_id = db.list_to_id(session, "Election", filters)
        jurisdiction_id = db.list_to_id(session, "ReportingUnit", filters)
        df = db.read_vote_count(
            session,
            election_id,
            jurisdiction_id,
            ["ElectionDistrict", "ContestName", "unit_type"],
            ["parent", "name", "type"],
        )
        df = df.sort_values(["parent", "name"]).reset_index(drop=True)
    # check if it's looking for a count of candidates
    elif menu_type == "count" and bool(
        [f for f in filters if f.startswith("Candidate")]
    ):
        election_id = db.list_to_id(session, "Election", filters)
        jurisdiction_id = db.list_to_id(session, "ReportingUnit", filters)
        df_unordered = db.read_vote_count(
            session,
            election_id,
            jurisdiction_id,
            ["ContestName", "BallotName", "PartyName", "unit_type"],
            ["parent", "name", "type", "unit_type"],
        )
        df = clean_candidate_names(df_unordered)
        df = df[["parent", "name", "unit_type"]].rename(columns={"unit_type": "type"})
    # check if it's looking for population data
    elif menu_type == "count" and bool(
        [f for f in filters if f.startswith("Population")]
    ):
        election_id = db.list_to_id(session, "Election", filters)
        jurisdiction_id = db.list_to_id(session, "ReportingUnit", filters)
        connection = session.bind.raw_connection()
        cursor = connection.cursor()
        df_unfiltered = db.read_external(
            cursor,
            election_id,
            jurisdiction_id,
            ["Source", "Label", "Category"],
        )
        df = df_unfiltered[df_unfiltered.Category.isin(filters)]

        cursor.close()
    # check if it's looking for a count by party
    elif menu_type == "count":
        election_id = db.list_to_id(session, "Election", filters)
        jurisdiction_id = db.list_to_id(session, "ReportingUnit", filters)
        df = db.read_vote_count(
            session,
            election_id,
            jurisdiction_id,
            ["PartyName", "unit_type"],
            ["parent", "type"],
        )
        df["name"] = df["parent"].str.replace(" Party", "", regex=True) + " " + df["type"]
        df = df[df_cols].sort_values(["parent", "type"])
    # Otherwise search for candidate
    else:
        election_id = db.list_to_id(session, "Election", filters)
        jurisdiction_id = db.list_to_id(session, "ReportingUnit", filters)
        df_unordered = db.read_vote_count(
            session,
            election_id,
            jurisdiction_id,
            ["ContestName", "BallotName", "PartyName", "unit_type"],
            ["parent", "name", "type", "unit_type"],
        )
        df_unordered = df_unordered[df_unordered["unit_type"].isin(filters)].copy()
        df_filtered = df_unordered[
            df_unordered["name"].str.contains(menu_type, case=False)
        ].copy()
        df = clean_candidate_names(df_filtered[df_cols].copy())
    # TODO: handle the "All" and "other" options better
    # TODO: handle sorting numbers better
    return package_display_results(df)


def package_display_results(data: pd.DataFrame) -> List[Dict[str, Any]]:
    """takes a result set and packages into JSON to return.
    Result set should already be ordered as desired for display"""
    results = []
    for i, row in data.iterrows():
        if row[1] in db.contest_type_mappings:
            row[1] = db.contest_type_mappings[row[1]]
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
    df["contest"] = df["parent"].str.replace(r"\(.*\)", "", regex=True)
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


def disambiguate_empty_cols(
    df_in: pd.DataFrame,
    drop_empties: bool,
    start: int = 0,
) -> pd.DataFrame:
    """Returns new df with empties dropped, or kept with non-blank placeholder info"""
    original_number_of_columns = df_in.shape[1]
    # set row index to default
    df = df_in.reset_index(drop=True)

    # put dummy info into the tops of the bad columns
    # in order to meet MultiIndex uniqueness criteria
    mask = df.eq("").loc[start:].all()
    bad_column_numbers = [j for j in range(original_number_of_columns) if mask[j]]
    for j in bad_column_numbers:
        for i in range(start):
            df.iloc[i, j] = f"place_holder_{i}_{j}"

    if drop_empties:
        good_column_numbers = [
            j for j in range(original_number_of_columns) if j not in bad_column_numbers
        ]
        df = df.iloc[:, good_column_numbers]
    return df


def set_and_fill_headers(
    df_in: pd.DataFrame,
    header_list: Optional[list],
    merged_cells: bool,
    drop_empties: bool = True,
) -> pd.DataFrame:
    # standardize the index  to 0, 1, 2, ...
    df = df_in.reset_index(drop=True)
    # rename all blank header entries to match convention of pd.read_excel
    #
    #
    if header_list:
        # fill blanks to match pandas standard
        df = m.fill_blanks(df, header_list, merged_cells)
        # set column index to default
        df.columns = range(df.shape[1])
        # drop empties
        df = disambiguate_empty_cols(
            df, drop_empties=drop_empties, start=max(header_list) + 1
        )
        # push appropriate rows into headers
        df = df.T.set_index(header_list).T
        # drop unused header rows
        df.drop(
            [x for x in range(max(header_list)) if x not in header_list], inplace=True
        )
    return df
