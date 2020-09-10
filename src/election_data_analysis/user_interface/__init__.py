from configparser import ConfigParser
from election_data_analysis import munge as m
from election_data_analysis import special_formats as sf
import pandas as pd
from pandas.errors import ParserError, ParserWarning
import csv
import os
from pathlib import Path
from election_data_analysis import juris_and_munger as jm
from tkinter import filedialog
from configparser import MissingSectionHeaderError


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
    "euc_jp",
    "ibm1026",
    "ascii",
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
    "ibm1140",
    "cp862",
    "euc_kr",
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
    "852",
    "windows-1250",
    "latin4",
    "cp037",
    "shift_jisx0213",
    "greek8",
    "latin6",
    "latin2",
    "mac_turkish",
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
    "L4",
    "euccn",
    "cyrillic",
    "IBM424",
    "cp863",
    "UTF-16LE",
    "mac_cyrillic",
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
    "iso8859_15",
    "maciceland",
    "utf_16_le",
    "EBCDIC-CP-HE",
    "ks_c-5601-1987",
}


def get_params_to_read_results(d: dict, results_file, munger_name) -> (dict, list):
    kwargs = d
    if results_file:
        kwargs["results_file"] = results_file
    if munger_name:
        kwargs["munger_name"] = munger_name
    missing = [
        x for x in ["results_file", "munger_name", "project_root"] if kwargs[x] is None
    ]
    return kwargs, missing


def read_results(params, error: dict) -> (pd.DataFrame, jm.Munger, dict):
    """Reads results (appending '_SOURCE' to the columns)
    and initiates munger. <params> must include these keys:
    'project_root', 'munger_name', 'results_file'"""
    if "aux_data_dir" in params.keys():
        aux_data_dir = params["aux_data_dir"]
    else:
        aux_data_dir = None
    mu = jm.Munger(
        os.path.join(params["project_root"], "mungers", params["munger_name"]),
        aux_data_dir=aux_data_dir,
        project_root=params["project_root"],
    )
    wr, error = read_combine_results(
        mu, params["results_file"], params["project_root"], error
    )
    wr.columns = [f"{x}_SOURCE" for x in wr.columns]
    return wr, mu, error


def pick_juris_from_filesystem(juris_path, project_root, err, check_files=False):
    """Returns a State object. <juris_path> is the path to the directory containing the
    defining files for the particular jurisdiction.
    """

    if check_files:
        missing_values = jm.ensure_jurisdiction_dir(juris_path, project_root)

    # initialize the jurisdiction
    if missing_values:
        err = add_new_error(
            err,
            "jurisdiction",
            juris_path,
            # TODO check missing_values -- what kind of object?
            f"Missing values: {missing_values}",
        )
        ss = None
    else:
        ss = jm.Jurisdiction(juris_path)
    return ss, err


def find_dupes(df):
    dupes_df = df[df.duplicated()].drop_duplicates(keep="first")
    deduped = df.drop_duplicates(keep="first")
    return dupes_df, deduped


def pick_munger(mungers_dir="mungers", project_root=None, munger_name=None):
    error = jm.ensure_munger_files(mungers_dir, munger_name, project_root=project_root)

    # if a munger error was found
    if "munger" in error.keys():
        return None, error

    else:
        munger_path = os.path.join(mungers_dir, munger_name)
        munger = jm.Munger(munger_path, project_root=project_root)

        return munger, error


def read_single_datafile(
    munger: jm.Munger, f_path: str, err: dict
) -> [pd.DataFrame, dict]:
    try:
        dtype = {c: str for c in munger.field_list}
        kwargs = {"thousands": munger.thousands_separator, "dtype": dtype}

        if munger.options["field_name_row"] is None:
            kwargs["header"] = None
            kwargs["names"] = munger.options["field_names_if_no_field_name_row"]
        else:
            kwargs["header"] = list(range(munger.options["header_row_count"]))

        if munger.file_type in ["txt", "csv"]:
            kwargs["encoding"] = munger.encoding
            kwargs["quoting"] = csv.QUOTE_MINIMAL
            kwargs["index_col"] = None
            if munger.file_type == "txt":
                kwargs["sep"] = "\t"
            df = pd.read_csv(f_path, **kwargs)
        elif munger.file_type in ["xls", "xlsx"]:
            df = pd.read_excel(f_path, **kwargs)
        else:
            err = add_new_error(
                err,
                "munger",
                munger.name,
                f"Unrecognized file_type: {munger.file_type}",
            )
            df = pd.DataFrame()
        if df.empty:
            err = add_new_error(
                err,
                "munger",
                munger.name,
                f"Nothing read from datafile. Munger may be inconsistent, or datafile may be empty.",
            )
        else:
            df = m.generic_clean(df)
            err = jm.check_results_munger_compatibility(munger, df, err)
        return [df, err]
    except FileNotFoundError as fnfe:
        e = fnfe
    except UnicodeDecodeError as ude:
        e = f"Encoding error. Datafile not read completely.\n{ude}"
    except ParserError as pe:
        # DFs have trouble comparing against None. So we return an empty DF and
        # check for emptiness below as an indication of an error.
        e = f"Error parsing results file.\n{pe}"
    err = add_new_error(
        err,
        "file",
        f_path,
        e,
    )
    return [pd.DataFrame(), err]


def read_combine_results(
    mu: jm.Munger, results_file, project_root, err, aux_data_dir=None
):
    if mu.options["file_type"] in ["concatenated-blocks"]:
        working, err = sf.read_concatenated_blocks(results_file, mu, err)
        if working.empty or [x for x in err.keys() if 'error' in x]:
            return working, err
        # set options that will be needed for going forward
        mu.options["count_columns"] = [working.columns.to_list().index("count")]
        mu.options["header_row_count"] = 1
        mu.options["field_name_row"] = 0
    else:
        try:
            working, err = read_single_datafile(mu, results_file, err)
        except Exception as exc:
            err = add_new_error(
                err,
                "file",
                results_file,
                f"Unexpected exception while reading file: {exc}",
            )
            return pd.DataFrame(), err
        if [k for k in err.keys() if err[k] != None]:
            return pd.DataFrame(), err
        else:
            working = m.cast_cols_as_int(
                working, mu.options["count_columns"], mode="index"
            )

        # merge with auxiliary files (if any)
        if aux_data_dir is not None:
            # get auxiliary data (includes cleaning and setting (multi-)index of primary key column(s))
            aux_data, err = mu.get_aux_data(
                aux_data_dir, err, project_root=project_root
            )
            for abbrev, r in mu.aux_meta.iterrows():
                # cast foreign key columns of main results file as int if possible
                foreign_key = r["foreign_key"].split(",")
                working = m.cast_cols_as_int(working, foreign_key)
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


def archive(file_name: str, current_dir: str, archive_dir: str):
    """Move <file_name> from <current_dir> to <archive_dir>. If <archive_dir> already has a file with that name,
    prefix <prefix> to the file name and try again. If that doesn't work, add prefix and timestamp"""
    archive = Path(archive_dir)
    archive.mkdir(parents=True, exist_ok=True)
    old_path = os.path.join(current_dir, file_name)
    new_path = os.path.join(archive_dir, file_name)
    i = 0
    while os.path.exists(new_path):
        i += 1
        new_path = os.path.join(archive_dir, f"{i}_{file_name}")
    try:
        os.rename(old_path, new_path)
    except Exception as exc:
        print(f"File {file_name} not moved: {exc}")
    return


def new_datafile(
    session,
    munger: jm.Munger,
    raw_path: str,
    project_root: str,
    juris: jm.Jurisdiction,
    results_info: list = None,
    aux_data_dir: str = None,
) -> dict:
    """Guide user through process of uploading data in <raw_file>
    into common data format.
    Assumes cdf db exists already"""
    err = dict()
    raw, err = read_combine_results(
        munger, raw_path, project_root, err, aux_data_dir=aux_data_dir
    )
    if raw.empty:
        err = add_new_error(
            err,
            "file",
            raw_path,
            f"No data read from file",
        )
        return err

    count_columns_by_name = [raw.columns[x] for x in munger.options["count_columns"]]

    try:
        raw = m.munge_clean(raw, munger)
    except:
        err["datafile_error"] = [
            "Cleaning of datafile failed. Results not loaded to database."
        ]
        return err

    try:
        err = m.raw_elements_to_cdf(
            session,
            project_root,
            juris,
            munger,
            raw,
            count_columns_by_name,
            err,
            ids=results_info,
        )
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
    if err == dict():
        err = None
    else:
        print(f"\tNote:\n{err}")
    return err


def get_runtime_parameters(
    required_keys: list,
    param_file: str,
    header: str,
    err: dict,
    optional_keys: list = None,
) -> (dict, dict):
    d = {}

    # read info from file
    parser = ConfigParser()
    p = parser.read(param_file)
    if len(p) == 0:
        err = add_new_error(
            err,
            "file",
            param_file,
            "File not found"
        )
        return d, err

    # find header
    try:
        h = parser[header]
    except KeyError as ke:
        err = add_new_error(
            err,
            "ini",
            param_file,
            f"Missing header: {ke}"
        )
        return d, err

    # read required info
    missing_required_params = list()
    for k in required_keys:
        try:
            d[k] = h[k]
        except KeyError:
            missing_required_params.append(k)
    if missing_required_params:
        mrp = ",".join(missing_required_params)
        err = add_new_error(
            err,
            "ini",
            param_file,
            f"Missing required parameters: {mrp}"
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


def consolidate_errors(err1: dict, err2: dict) -> dict:
    """Takes two error dictionaries (assumed to have same bottom-level keys)
    and consolidates them, concatenating the error messages"""
    d = dict()
    err_types = err1.keys()
    assert set(err_types) == set(err2.keys())
    for et in err_types:
        d[et] = dict()
        allkeys = set(err1[et].keys()).union(set(err2[et].keys()))
        for k in allkeys:
            # if k in both
            if k in err2[et].keys() and k in err1[et].keys():
                # concatenate the string values
                d[et][k] = f"{err1[et][k]}|{err2[et][k]}"
            # if k only in err2[et]
            elif k in err2[et].keys():
                d[et][k] = err2[et][k]
            # if k only in err1[et]
            else:
                d[et][k] = err1[et][k]
    return d


def add_new_error(
        err: dict,
        err_type: str,
        key: str,
        msg: str
) -> dict:
    """err is a dictionary of dictionaries, one for each err_type.
    This function return err, augmented by the error specified in <err_type>,<key> and <msg>"""
    if err is None:
        print ("Initializing error dictionary")
        err = {
            "ini":{},
            "munger":{},
            "jurisdiction":{},
            "file":{},
            "system":{},
            "warn-ini":{},
            "warn-munger":{},
            "warn-jurisdiction":{},
            "warn-file":{},
            "warn-system":{},
        }
    if err_type not in err.keys():
        err = add_new_error(
            err,
            "system",
            "user_interface.add_new_error",
            f"{err_type}: {msg}")
        return err
    if key in err[err_type].keys():
        err[err_type][key].append(f"|{msg}")
    else:
        err[err_type][key] = msg
    return err


# TODO remove this diagnostic
def add_error():
    return