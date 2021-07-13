import inspect
from pathlib import Path

from electiondata import (
    database as db,
    userinterface as ui,
    juris as jm,
    constants,
)
import pandas as pd
from pandas.api.types import is_numeric_dtype
from typing import Optional, List, Dict, Any
import re
import os
from sqlalchemy.orm.session import Session


def clean_count_cols(
    df: pd.DataFrame, cols: Optional[List[str]], thousands: Optional[str] = None
) -> (pd.DataFrame, pd.DataFrame):
    """Casts the given columns as integers, replacing any bad
    values with 0 and reporting a dataframe of any rows so changed.
    If <thousands> separator is given, check for it.
    Also returns dataframe of rows where count failed"""
    if cols is None:
        return df, pd.DataFrame(columns=df.columns)
    else:
        err_df = pd.DataFrame()
        working = df.copy()
        for c in cols:
            if c in working.columns:
                # remove the thousands separator if the column is not already int64
                if thousands and (df.dtypes[c] == "object"):
                    working[c] = working[c].str.replace(thousands, "")
                mask = ~working[c].astype(str).str.isdigit()
                if mask.any():
                    # return bad rows for error reporting
                    err_df = pd.concat([err_df, working[mask]]).drop_duplicates()

                    # cast as int, changing any non-integer values to 0
                    working[c] = (
                        pd.to_numeric(working[c], errors="coerce")
                        .fillna(0)
                        .astype("int64")
                    )
                else:
                    working[c] = working[c].astype("int64")
        return working, err_df


def clean_ids(
    df: pd.DataFrame,
    cols: List[str],
) -> (pd.DataFrame(), pd.DataFrame):
    """Changes to integer type each column <cols> that is actually a column of <df>.
    Nulls are changed to 0, as are entire columns with any entries that
    cannot be parsed as integers."""

    if cols == list():
        return df, pd.DataFrame()
    err_df = pd.DataFrame()
    working = df.copy()
    for c in [x for x in cols if x in working.columns]:
        if c in working.columns and is_numeric_dtype(working[c]):
            err_df = pd.concat([err_df, working[working[c].isnull()]])
            working[c] = working[c].fillna(0).astype("int64")
        else:
            err_df = working
            working[c] = 0

    err_df.drop_duplicates(inplace=True)
    return working, err_df


def clean_strings(
    df: pd.DataFrame,
    cols: List[str],
) -> pd.DataFrame():

    working = df.copy()

    for c in cols:
        # cast all specified columns as strings
        try:
            working[c] = working[c].astype("string")
        except Exception as exc:
            print(f"Could not convert column {c} to strings: {exc}")

        if c in cols:
            # change nulls to the empty string
            working[c] = working[c].fillna("")
            # replace any double quotes with single quotes
            try:
                # replace any " by '
                mask = working[c].str.contains('"').fillna(False)
                working.loc[mask, c] = working[c].str.replace('"', "'")[mask]
            except AttributeError or TypeError:
                pass
            try:
                # strip extraneous whitespace from any value recognized as string
                mask = working[c].apply(lambda x: isinstance(x, str))
                working.loc[mask, c] = working[c][mask].apply(compress_whitespace)
            except (AttributeError, TypeError):
                pass
    return working


def add_regex_column(
    df: pd.DataFrame,
    old_col: str,
    new_col: str,
    pattern_str: str,
    munger_name: str,
) -> (pd.DataFrame, [dict, None]):
    """Return <df> with <new_col> appended, where <new_col> is pulled from <old_col> by the <pattern>.
    Note that only the first group (per <pattern>) is returned"""
    err = None
    working = df.copy()
    try:
        p = re.compile(pattern_str)
        # replace via regex if possible; otherwise msg
        # # put informative error message in new_col (to be overwritten if no error)
        old = working[old_col].copy()
        working[new_col] = working[old_col].str.cat(
            old, f"Does not match regex {pattern_str}: "
        )
        # # where regex succeeds, replace error message with good value
        mask = working[old_col].str.match(p)
        working.loc[mask, new_col] = working[mask][old_col].str.extract(
            pattern_str, expand=False
        )

    except re.error as e:
        err = ui.add_new_error(
            err, "munger", munger_name, f"Regex error ({e}) in pattern:\n {pattern_str}"
        )
    except Exception as e:
        err = ui.add_new_error(
            err,
            "system",
            f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
            f"Unexpected exception: {e}",
        )

    return working, err


def text_fragments_and_fields(formula: str) -> (List[List[str]], str):
    """Given a formula with fields enclosed in angle brackets,
    return a list of text-fragment,field pairs (in order of appearance) and a final text fragment.
    E.g., if formula is <County>;<Precinct>, returned are [(None,County),(';',Precinct)] and None."""
    # use regex to apply formula (e.g., decode '<County>;<Precinct>'
    p = re.compile(
        "(?P<text>[^<>]*)<(?P<field>[^<>]+)>"
    )  # pattern to find text,field pairs
    q = re.compile("(?<=>)[^<]*$")  # pattern to find text following last pair
    text_field_list = re.findall(p, formula)
    if not text_field_list:
        last_text = [formula]
    else:
        last_text = re.findall(q, formula)
    return text_field_list, last_text


def add_column_from_formula(
    working: pd.DataFrame,
    formula: str,
    new_col: str,
    err: Optional[dict],
    munger_name: str,
    suffix=None,
) -> (pd.DataFrame, Optional[dict]):
    """If <suffix> is given, add it to each field in the formula
    If formula is enclosed in braces, parse first entry as formula, second as a
    regex (with one parenthesized group) as a recipe for pulling the value via regex analysis
    """
    w = working.copy()
    #  for each {} pair in the formula, create a new column
    # (assuming formula is well-formed)
    try:
        temp_cols = []
        for x in constants.brace_pattern.finditer(formula):
            # create a new column with the extracted info
            old_col, pattern_str = x.groups()
            temp_col = f"extracted_from_{old_col}"
            w, new_err = add_regex_column(
                w, old_col, temp_col, pattern_str, munger_name
            )
            # change the formula to use the temp column
            formula = formula.replace(f"{{<{old_col}>,{pattern_str}}}", f"<{temp_col}>")
            if new_err:
                err = ui.consolidate_errors([err, new_err])
                if ui.fatal_error(new_err):
                    return w, err
            temp_cols.append(temp_col)
        # once all {} pairs are gone, use concatenation to build the column to be returned
        text_field_list, last_text = text_fragments_and_fields(formula)

        # add suffix, if required
        if suffix:
            text_field_list = [(t, f"{f}{suffix}") for (t, f) in text_field_list]

        # add column to <working> dataframe via the concatenation formula
        if last_text:
            w = add_constant_column(w, new_col, last_text[0], dtype="string")
        else:
            err = ui.add_new_error(
                err,
                "system",
                f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
                f"No last_text found by text_fragments_and_fields for {formula}",
            )
            return w, err
        text_field_list.reverse()
        for t, f in text_field_list:
            try:
                w[new_col] = t + w[f].map(str) + w[new_col].map(str)
            except KeyError as ke:
                err = ui.add_new_error(
                    err,
                    "munger",
                    munger_name,
                    f"Expected transformed column '{f}' not found, "
                    f"perhaps because of mismatch between munger and results file. KeyError: {ke}",
                )
                return w, err

    except Exception as e:
        err = ui.add_new_error(
            err,
            "system",
            f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
            f"Unexpected error: {e}",
        )
        return w, err

    # delete temporary columns
    w.drop(temp_cols, axis=1, inplace=True)
    return w, err


def compress_whitespace(s: Optional[str]) -> str:
    """Return a string where every instance of consecutive whitespaces internal to <s> has been replace
    by the first of those consecutive whitespace characters,
    leading and trailing whitespace is eliminated
    and any carriage returns are changed to spaces"""
    if not s:
        return ""
    new_s = re.sub(r"(\s)\s+", "\\1", s)
    new_s = new_s.strip()
    new_s = new_s.replace("\n", " ")
    return new_s


def replace_raw_with_internal_ids(
    df: pd.DataFrame,
    path_to_jurisdiction_dir: str,
    juris_true_name: str,  # for error reporting
    file_name: str,  # for error reporting
    munger_name: str,  # for error reporting
    table_df: pd.DataFrame,
    element: str,
    internal_name_column: str,
    error: dict,
    drop_unmatched: bool = False,
    drop_extraneous: bool = True,
    unmatched_id: int = 0,
    drop_all_ok: bool = False,
) -> (pd.DataFrame, dict):
    """replace columns in <working> with raw_identifier values by columns with internal names and Ids
    from <table_df>, which has structure of a db table for <element>.
    <unmatched_id> is the id to assign to unmatched records.
    If <drop_extraneous> = True and dictionary matches raw_identifier to "row should be dropped",
    drop that row EVEN IF <drop_unmatched> = False.
    """
    working = df.copy()
    # join the 'cdf_internal_name' from the raw_identifier table -- this is the internal name field value,
    # no matter what the name field name is in the internal element table (e.g. 'Name', 'BallotName' or 'Selection')
    # use dictionary.txt from jurisdiction

    raw_identifiers = pd.read_csv(
        os.path.join(path_to_jurisdiction_dir, "dictionary.txt"),
        sep="\t",
        encoding=constants.default_encoding,
        dtype=str,
    )

    # restrict to the element at hand
    raw_ids_for_element = raw_identifiers[
        raw_identifiers["cdf_element"] == element
    ].copy()

    if element == "Candidate":
        # remove any lines with nulls
        raw_ids_for_element = raw_ids_for_element[
            raw_ids_for_element.notnull().all(axis=1)
        ]

        # Regularize candidate names from dictionary (to match what's done during upload of candidates to Candidate
        #  table in db)
        raw_ids_for_element["cdf_internal_name"] = regularize_candidate_names(
            raw_ids_for_element["cdf_internal_name"]
        )
        # Regularize candidate names from results file and from dictionary.txt
        working.Candidate_raw = regularize_candidate_names(working.Candidate_raw)
        raw_ids_for_element.raw_identifier_value = regularize_candidate_names(
            raw_ids_for_element.raw_identifier_value
        )
        # NB: regularizing can create duplicates (e.g., HILLARY CLINTON and Hillary Clinton regularize to the sam)
        raw_ids_for_element.drop_duplicates(inplace=True)

    working = working.merge(
        raw_ids_for_element,
        how="left",
        left_on=f"{element}_raw",
        right_on="raw_identifier_value",
        suffixes=["", f"_{element}_ei"],
    )

    # identify unmatched
    try:
        unmatched = working[
            working["cdf_internal_name"].isnull() & working[f"{element}_raw"].notnull()
        ]
        unmatched_raw = sorted(unmatched[f"{element}_raw"].unique(), reverse=True)
        unmatched_raw = [x for x in unmatched_raw if x != ""]
    except Exception:
        unmatched_raw = list()  # for syntax-checker
        pass
    if len(unmatched_raw) > 0 and element != "BallotMeasureContest":
        unmatched_str = "\n".join(unmatched_raw)
        e = f"\n{element}s not found in dictionary.txt with munger {munger_name}:\n{unmatched_str}"
        error = ui.add_new_error(error, "warn-jurisdiction", juris_true_name, e)

    if drop_unmatched:
        working = working[working["cdf_internal_name"].notnull()]

    if drop_extraneous:
        # TODO tech debt - note change of case for Candidate above which, if
        #  changed, might affect this in unexpected ways
        # drop extraneous rows identified in dictionary
        working = working[working["cdf_internal_name"] != "row should be dropped"]

    if working.empty:
        raws = "\n".join(list(df[f"{element}_raw"].unique()))
        e = (
            f"No true raw {element} in 'dictionary.txt' matched any raw {element} derived from the result file.\n"
            f"true raw {element}s:\n{raws}"
        )
        if drop_unmatched and not drop_all_ok:

            error = ui.add_new_error(
                error,
                "jurisdiction",
                juris_true_name,
                e,
            )
        else:
            error = ui.add_new_error(error, "warn-jurisdiction", juris_true_name, e)
        # give working the proper columns and return
        new_cols = [
            c
            for c in working.columns
            if (
                c
                not in [
                    "raw_identifier_value",
                    "cdf_element",
                    f"_{element}_ei",
                    "cdf_internal_name",
                ]
            )
        ] + [f"{element}_Id", element]
        working = pd.DataFrame(columns=new_cols)

        return working, error

    # unmatched elements get nan in fields from dictionary table. Change these to "none or unknown"
    if not drop_unmatched:
        working["cdf_internal_name"] = working["cdf_internal_name"].fillna(
            "none or unknown"
        )

    # drop extraneous cols from mu.raw_identifier
    working = working.drop(["raw_identifier_value", "cdf_element"], axis=1)

    # ensure that there is a column in working called by the element
    # containing the internal name of the element
    if f"_{element}_ei" in working.columns:
        working.rename(columns={f"_{element}_ei": element}, inplace=True)
    else:
        working.rename(columns={"cdf_internal_name": element}, inplace=True)

    # join the element table Id and name columns.
    # This will create two columns with the internal name field,
    # whose names will be <element> (from above)
    # and either internal_name_column or internal_name_column_table_name
    working = working.merge(
        table_df[["Id", internal_name_column]],
        how="left",
        left_on=element,
        right_on=internal_name_column,
    )

    # error/warning for unmatched elements
    working_unmatched = working[(working.Id.isnull()) & (working[element].notnull())]
    if not working_unmatched.empty and element != "BallotMeasureContest":
        unmatched_pairs = [
            f'({r[element]},{r[f"{element}_raw"]})'
            for i, r in working_unmatched[[f"{element}_raw", element]]
            .drop_duplicates()
            .iterrows()
        ]
        unmatched_str = "\n".join(unmatched_pairs)
        e = (
            f"\nResults for {working_unmatched.shape[0]} rows with unmatched {element}s "
            f"will not be loaded to database from file {file_name} with munger {munger_name}. "
            f"These records (internal name, raw identifier) were found in dictionary.txt, but "
            f"no corresponding record was found in the {element} table in the database: \n{unmatched_str}"
        )
        error = ui.add_new_error(
            error,
            "warn-jurisdiction",
            juris_true_name,
            e,
        )

    if drop_unmatched:
        # if all are unmatched
        if working_unmatched.shape[0] == working.shape[0]:
            error = ui.add_new_error(
                error,
                "jurisdiction",
                juris_true_name,
                (
                    f"No {element} was matched. Either raw values are not in dictionary.txt, or "
                    f"the corresponding cdf_internal_names are missing from {element}.txt"
                ),
            )
            return working.drop(working.index), error
        # if only some are unmatched
        else:
            # drop the unmatched ones
            working.drop(labels=working_unmatched.index, inplace=True)

    else:
        # change name of unmatched to 'none or unknown' and assign <unmatched_id> as Id
        working.loc[working.Id.isnull(), internal_name_column] = "none or unknown"
        working["Id"].fillna(unmatched_id, inplace=True)

    working = working.drop([internal_name_column, f"{element}_raw"], axis=1)
    working.rename(columns={"Id": f"{element}_Id"}, inplace=True)
    return working, error


def get_non_standard(
    df: pd.DataFrame,
    type_col: str,
) -> (pd.DataFrame, List[str]):
    """Returns list of all non-standard types found"""
    non_standard = list(
        x
        for x in df[type_col].unique()
        if x not in constants.nist_standard[type_col]
    )
    return non_standard


def regularize_candidate_names(
    candidate_column: pd.Series,
) -> pd.Series:
    ws = candidate_column.copy()

    # compress whitespace
    ws = ws.apply(compress_whitespace)

    mask = ws.str.isupper()
    # if original is all caps
    if mask.any():
        # change to title case
        ws[mask] = candidate_column[mask].str.title()
        # TODO test out NameCleaver from SunlightLabs
        # respect the Irish

    # if original is not all upper case
    else:
        # do nothing
        pass
    return ws


def melt_to_one_count_column(
    df: pd.DataFrame,
    p: dict,
    count_columns_by_name: List[str],
    munger_name: str,
    file_name: str,
    sheet_name: Optional[str] = None,
) -> (pd.DataFrame, Optional[dict]):
    """transform to df with single count column and all raw munge info in other columns"""
    err = None
    multi = isinstance(df.columns, pd.MultiIndex)
    # drop any empty columns (and remove them from count_columns list)
    df = ui.disambiguate_empty_cols(df, drop_empties=False)
    if multi:
        # transform multi-index to plain index
        df.columns = [";:;".join([f"{x}" for x in tup]) for tup in df.columns]
        count_cols_compatible = [
            ";:;".join([f"{x}" for x in tup]) for tup in count_columns_by_name
        ]
    else:
        count_cols_compatible = count_columns_by_name
    count_cols = {c for c in count_cols_compatible if c in df.columns}

    # NB merged cells in excel can lead to spurious empty columns
    if not count_cols:
        if sheet_name:
            extra = f" on sheet {sheet_name}"
        else:
            extra = ""
        msg = f"No count columns found for at least one block of data{extra} with munger {munger_name}"

        err = ui.add_new_error(err, "file", file_name, msg)
        return pd.DataFrame(), err
    # melt so that there is one single count column
    id_columns = {c for c in df.columns if c not in count_cols}
    melted = df.melt(
        id_vars=id_columns,
        value_vars=count_cols,
        var_name="header_0",
        value_name="Count",
    )
    if multi:
        tab_to_df = df_header_rows_from_sheet_header_rows(p)
        new_columns = list(melted.columns)
        # remove extraneous text from column multi-headers for string fields (id_columns),
        #  leaving only noncount_header_row
        for idx in range(melted.shape[1]):
            if melted.columns[idx] in id_columns:
                new_columns[idx] = melted.columns[idx].split(";:;")[
                    tab_to_df[p["noncount_header_row"]]
                ]
        melted.columns = new_columns

        if "in_count_headers" in p["munge_field_types"]:
            # split header_0 column into separate columns
            # # get header_rows
            melted[
                [f"count_header_{idx}" for idx in p["count_header_row_numbers"]]
            ] = pd.DataFrame(melted["header_0"].str.split(";:;", expand=True).values)[
                [tab_to_df[idx] for idx in p["count_header_row_numbers"]]
            ]
            melted.drop("header_0", axis=1, inplace=True)
    elif len(p["count_header_row_numbers"]) == 1:
        count_header_row = p["count_header_row_numbers"][0]
        # rename header_0 to count_header_i
        melted.rename(
            columns={"header_0": f"count_header_{count_header_row}"}, inplace=True
        )
    return melted, err


def df_header_rows_from_sheet_header_rows(p: Dict[str, Any]) -> Dict[int, int]:
    """produces mapping from the header row numbering in the tabular file -- which could be
    any collection of rows -- to the header numbering in the dataframe
    (which must be 0 - n for some n)."""
    if p["count_header_row_numbers"] or p["noncount_header_row"]:
        # create sorted list with no dupes
        table_rows = sorted(
            set(p["count_header_row_numbers"] + [p["noncount_header_row"]])
        )
        tab_to_df = {table_rows[idx]: idx for idx in range(len(table_rows))}
    else:
        tab_to_df = dict()
    return tab_to_df


def add_constant_column(
    df: pd.DataFrame, col_name: str, col_value: Any, dtype: Optional[str] = None
) -> pd.DataFrame:
    new_df = df.assign(**dict.fromkeys([col_name], col_value))
    if dtype:
        new_df[col_name] = new_df[col_name].astype(dtype)
    return new_df


def add_contest_id(
    df: pd.DataFrame,
    path_to_jurisdiction_dir: str,
    juris_true_name: str,
    file_name: str,
    munger_name: str,
    err: Optional[dict],
    session: Session,
) -> (pd.DataFrame, dict):
    working = df.copy()
    """Append Contest_Id and contest_type. Add contest_type column and fill it correctly.
    Drop rows which match neither BM nor C contest"""

    # add Contest_Id and contest_type
    df_for_type = dict()
    w_for_type = dict()
    df_contest = pd.read_sql_table(f"Contest", session.bind)
    for c_type in ["BallotMeasure", "Candidate"]:
        if f"{c_type}Contest_raw" in working.columns:
            # restrict df_contest to the contest_type <c_type> and get the <c_type>Contest_Id
            df_for_type[c_type] = df_contest[df_contest.contest_type == c_type]
            none_or_unknown_id = db.name_to_id(
                session, f"{c_type}Contest", "none or unknown"
            )
            working, new_err = replace_raw_with_internal_ids(
                working,
                path_to_jurisdiction_dir,
                juris_true_name,
                file_name,
                munger_name,
                df_for_type[c_type],
                f"{c_type}Contest",
                "Name",
                err,
                drop_unmatched=False,
                unmatched_id=none_or_unknown_id,
                drop_all_ok=True,
            )
            if new_err:
                err = ui.consolidate_errors([err, new_err])
            # restrict working to the contest_type <c_type>, add contest_type column
            w_for_type[c_type] = working[
                working[f"{c_type}Contest"] != "none or unknown"
            ]
            w_for_type[c_type] = add_constant_column(
                w_for_type[c_type], "contest_type", c_type
            ).rename(columns={f"{c_type}Contest_Id": "Contest_Id"})

            # drop text column
            w_for_type[c_type] = w_for_type[c_type].drop(f"{c_type}Contest", axis=1)
        else:
            w_for_type[c_type] = pd.DataFrame()

    # FIXME: check somewhere that no name (other than 'none or unknown') is shared by BMContests and CandidateContests
    # TODO check this also when juris files loaded, to save time for user

    # drop obsolete columns
    if w_for_type["BallotMeasure"].empty:
        working_temp = w_for_type["Candidate"]
    elif w_for_type["Candidate"].empty:
        working_temp = w_for_type["BallotMeasure"]
    else:
        common_cols = [
            c
            for c in w_for_type["BallotMeasure"].columns
            if c in w_for_type["Candidate"].columns
        ]
        for c_type in ["BallotMeasure", "Candidate"]:
            w_for_type[c_type] = w_for_type[c_type][common_cols]

        # assemble working from the two pieces
        working_temp = pd.concat(
            [w_for_type[ct] for ct in ["BallotMeasure", "Candidate"]]
        )

    # fail if fatal errors or no contests recognized (in reverse order, just for fun
    if working_temp.empty:
        err = ui.add_new_error(
            err, "jurisdiction", juris_true_name, f"No contests recognized."
        )
    else:
        working = working_temp
    if ui.fatal_error(err):
        return working, err

    return working, err


def add_selection_id(  # TODO tech debt: why does this add columns 'I' and 'd'?
    df: pd.DataFrame,
    engine,
    path_to_jurisdiction_dir: str,
    juris_true_name: str,
    munger_name: str,
    file_name: str,
    err: dict,
) -> (pd.DataFrame, dict):
    """Assumes <df> has contest_type, BallotMeasureSelection_raw, Candidate_Id column.
    Loads CandidateSelection table.
    Appends & fills Selection_Id columns"""

    # split df by contest type
    w = dict()
    for ct in ["BallotMeasure", "Candidate"]:
        w[ct] = df[df.contest_type == ct].copy()

    # append BallotMeasureSelection_Id as Selection_Id to w['BallotMeasure']
    if not w["BallotMeasure"].empty:
        bms = pd.read_sql_table(f"BallotMeasureSelection", engine)
        w["BallotMeasure"], err = replace_raw_with_internal_ids(
            w["BallotMeasure"],
            path_to_jurisdiction_dir,
            juris_true_name,
            file_name,
            munger_name,
            bms,
            "BallotMeasureSelection",
            "Name",
            err,
            drop_unmatched=True,
            drop_all_ok=True,
        )
        w["BallotMeasure"].rename(
            columns={"BallotMeasureSelection_Id": "Selection_Id"}, inplace=True
        )
        w["BallotMeasure"].drop(
            ["BallotMeasureSelection", "Candidate_Id"], axis=1, inplace=True
        )

    # prepare to append CandidateSelection_Id as Selection_Id
    if not w["Candidate"].empty:
        c_df = w["Candidate"][["Candidate_Id", "Party_Id"]].drop_duplicates()

        # clean Ids and drop any that were null (i.e., 0 after cleaning)
        c_df, err_df = clean_ids(c_df, ["Candidate_Id", "Party_Id"])
        if not err_df.empty:
            err = ui.add_new_error(
                err,
                "warn-system",
                f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
                f"some Candidate_Ids or Party_Ids null\n{err_df}",
            )
        c_df = c_df[c_df.Candidate_Id != 0]

        # pull any existing Ids into a new CandidateSelection_Id column
        col_map = {c: c for c in ["Party_Id", "Candidate_Id"]}
        c_df = db.append_id_to_dframe(
            engine, c_df, "CandidateSelection", col_map=col_map
        )

        # find unmatched records
        # TODO this throws error (FutureWarning: elementwise comparison failed),
        #  maybe because CandidateSelection_Id cannot be compared to ""?
        c_df_unmatched = c_df[
            (c_df.CandidateSelection_Id == 0)
            | (c_df.CandidateSelection_Id == "")
            | (c_df.CandidateSelection_Id.isnull())
        ].copy()

        if not c_df_unmatched.empty:
            #  Load CandidateSelections to Selection table (for unmatched)
            id_list = db.add_records_to_selection_table(engine, c_df_unmatched.shape[0])

            # Load unmatched records into CandidateSelection table
            c_df_unmatched["Id"] = pd.Series(id_list, index=c_df_unmatched.index)
            new_err = db.insert_to_cdf_db(
                engine, c_df_unmatched, "CandidateSelection", "munger", munger_name
            )
            if new_err:
                err = ui.consolidate_errors([err, new_err])
                if ui.fatal_error(new_err):
                    return pd.DataFrame(), err

            # update CandidateSelection_Id column for previously unmatched, merging on Candidate_Id and Party_Id
            c_df.loc[c_df_unmatched.index, "CandidateSelection_Id"] = c_df_unmatched[
                "Id"
            ]
        # recast Candidate_Id and Party_Id to int in w['Candidate'];
        # Note that neither should have nulls, but rather the 'none or unknown' Id
        #  NB: c_df had this recasting done in the append_id_to_dframe routine
        w["Candidate"], err_df = clean_ids(w["Candidate"], ["Candidate_Id", "Party_Id"])
        if not err_df.empty:
            # show all columns of dataframe with problem in Party_Id or Candidate_Id
            pd.set_option("max_columns", None)
            err = ui.add_new_error(
                err,
                "system",
                f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
                f"Problem with Candidate_Id or Party_Id in some rows:\n{err_df}",
            )
            pd.reset_option("max_columns")

        # append CandidateSelection_Id to w['Candidate']
        w["Candidate"] = w["Candidate"].merge(
            c_df, how="left", on=["Candidate_Id", "Party_Id"]
        )

        # rename to Selection_Id
        w["Candidate"] = w["Candidate"].rename(
            columns={"CandidateSelection_Id": "Selection_Id"}
        )
        # and drop extraneous
        to_drop = [
            x
            for x in w["Candidate"].columns
            if x in ["Candidate_Id", "BallotMeasureSelection_raw"]
        ]
        w["Candidate"].drop(to_drop, axis=1, inplace=True)

    working = pd.concat([w["BallotMeasure"], w["Candidate"]])

    # drop Candidate_Id if it is still a column
    if "Candidate_Id" in working.columns:
        working.drop("Candidate_Id", axis=1, inplace=True)
    return working, err


def raw_to_id_simple(
    df: pd.DataFrame,
    path_to_jurisdiction_dir,
    element_list: list,
    session: Session,
    file_name: str,
    munger_name: str,
    juris_true_name,
    file_type: str,
) -> (pd.DataFrame, Optional[dict]):
    """Append ids to <df> for any row- or column- sourced elements given in <element_list>"""

    err = None
    working = df.copy()
    element_df = pd.DataFrame()
    name_field = ""
    for element in element_list:
        try:
            # capture id from db in new column and erase any now-redundant cols
            if element != "CountItemType":
                element_df = pd.read_sql_table(element, session.bind)
                name_field = db.get_name_field(element)
            # set drop_unmatched = True for fields necessary to BallotMeasure rows,
            #  drop_unmatched = False otherwise to prevent losing BallotMeasureContests for BM-inessential fields
            if element == "ReportingUnit" or element == "CountItemType":
                drop = True
            else:
                drop = False
            if element == "CountItemType":
                # munge raw to internal CountItemType
                if file_type == "nist_v2_xml":
                    r_i = constants.cit_from_raw_nist_df
                else:
                    r_i = pd.read_csv(
                        os.path.join(path_to_jurisdiction_dir, "dictionary.txt"),
                        sep="\t",
                        encoding=constants.default_encoding,
                        dtype=str,
                    )
                    r_i = r_i[r_i.cdf_element == "CountItemType"]
                recognized = r_i.raw_identifier_value.unique()
                matched = working.CountItemType_raw.isin(recognized)
                if not matched.all():
                    unmatched = "\n".join(
                        (working[~matched]["CountItemType_raw"]).unique()
                    )
                    err = ui.add_new_error(
                        err,
                        "warn-jurisdiction",
                        juris_true_name,
                        f"\nSome unmatched CountItemTypes:\n{unmatched}",
                    )
                # get list of raw CountItemTypes in case they are needed for error reporting
                all_raw_cit = working.CountItemType_raw.unique().tolist()
                # get internal CountItemType for all matched lines
                working = (
                    working[matched]
                    .merge(
                        r_i,
                        how="left",
                        left_on="CountItemType_raw",
                        right_on="raw_identifier_value",
                    )
                    .rename(columns={"cdf_internal_name": "CountItemType"})
                )

                # if no CountItemTypes matched to dictionary
                if working.CountItemType.isnull().all():
                    err = ui.add_new_error(
                        err,
                        "jurisdiction",
                        juris_true_name,
                        f"No CountItemTypes from results file found in dictionary.txt, so no data loaded."
                        f"CountItemTypes from file: {all_raw_cit}",
                    )
                    return working, err

                # report non-standard CountItemTypes
                non_standard = get_non_standard(
                    working, "CountItemType"
                )
                if non_standard:
                    try:
                        ns = "\n\t".join(non_standard)
                    except TypeError:
                        ns = non_standard
                    err = ui.add_new_error(
                        err,
                        "warn-jurisdiction",
                        juris_true_name,
                        f"Some recognized CountItemTypes are not in the NIST standard list:\n\t{ns}",
                    )
                working = working.drop(
                    ["raw_identifier_value", "cdf_element", "CountItemType_raw"], axis=1
                )
            else:
                none_or_unknown_id = db.name_to_id(session, element, "none or unknown")
                working, new_err = replace_raw_with_internal_ids(
                    working,
                    path_to_jurisdiction_dir,
                    juris_true_name,
                    file_name,
                    munger_name,
                    element_df,
                    element,
                    name_field,
                    err,
                    drop_unmatched=drop,
                    unmatched_id=none_or_unknown_id,
                )
                err = ui.consolidate_errors([err, new_err])
                if ui.fatal_error(new_err):
                    return working, err
                working.drop(element, axis=1, inplace=True)

        except KeyError as exc:
            err = ui.add_new_error(
                err,
                "munger",
                munger_name,
                f"KeyError ({exc}) while adding internal ids for {element}. " f"Check munger",
            )
        except AttributeError as exc:
            err = ui.add_new_error(
                err,
                "munger",
                munger_name,
                f"AttributeError ({exc}) while adding internal ids for {element}."
                f"Check munger",
            )
        except Exception as exc:
            err = ui.add_new_error(
                err,
                "system",
                f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
                f"Unexpected exception while adding internal ids for {element}:\n{exc}",
            )

    return working, err


def missing_total_counts(
    df: pd.DataFrame,
    session: Session,
) -> pd.DataFrame:
    """Assumes cols of df include Count and CountItemType
    Return df of "total" type records that are missing
    """
    # get all totals by all but count, count-item-type-related and id columns
    group_cols = [
        x
        for x in df.columns
        if x not in ["Count", "CountItemType", "Id"]
    ]
    sums = df.groupby(group_cols)["Count"].sum()

    # keep only sums that already involve a total
    no_total = (
        df.groupby(group_cols)["CountItemType"]
        .apply(list)
        .apply(lambda x: "total" not in x)
    )
    sums = sums[no_total]
    sums_df = sums.reset_index()

    sums_df = add_constant_column(sums_df, "CountItemType", "total")

    return sums_df


def munge_raw_to_ids(
    df: pd.DataFrame,
    constant_dict: dict,
    path_to_jurisdiction_dir: str,
    file_name: str,
    munger_name: str,
    juris_true_name: str,
    session: Session,
    file_type: str,
) -> (pd.DataFrame, Optional[dict]):
    """Replace raw-munged columns with internal columns. For CountItemType
    this will be a text column; for others it will be an Id column """

    err = None
    working = df.copy()

    # add Contest_Id column and contest_type column
    if "CandidateContest" in constant_dict.keys():
        contest_id = db.name_to_id(
            session, "Contest", constant_dict["CandidateContest"]
        )
        if not contest_id:
            err = ui.add_new_error(
                err,
                "jurisdiction",
                juris_true_name,
                f"CandidateContest specified in ini file ({constant_dict['CandidateContest']}) "
                f"not found. Check CandidateContest.txt.",
            )
            return df, err
        working = add_constant_column(
            working,
            "Contest_Id",
            contest_id,
        )
        working = add_constant_column(working, "contest_type", "Candidate")
        working.drop("CandidateContest", axis=1, inplace=True)
    elif "BallotMeasureContest" in constant_dict.keys():
        working = add_constant_column(
            working,
            "Contest_Id",
            db.name_to_id(session, "Contest", constant_dict["BallotMeasureContest"]),
        )
        working.drop("BallotMeasureContest", axis=1, inplace=True)
        working = add_constant_column(working, "contest_type", "BallotMeasure")
    else:
        try:
            working, err = add_contest_id(
                working,
                path_to_jurisdiction_dir,
                juris_true_name,
                file_name,
                munger_name,
                err,
                session,
            )
        except Exception as exc:
            err = ui.add_new_error(
                err,
                "system",
                f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
                f"Unexpected exception while adding Contest_Id: {exc}",
            )
            return working, err
        if ui.fatal_error(err):
            return working, err

    # add all other _Ids/Types except Selection_Id
    # # for constants
    other_constants = [
        t
        for t in constant_dict.keys()
        if t[-7:] != "Contest" and (t[-9:] != "Selection")
    ]
    for element in other_constants:
        # only CountItemType is kept as text
        if element == "CountItemType":
            working = add_constant_column(working, "CountItemType", constant_dict["CountItemType"])
        else:
            working = add_constant_column(
                working,
                f"{element}_Id",
                db.name_to_id(session, element, constant_dict[element]),
            )
            working.drop(element, axis=1, inplace=True)
            working, err_df = clean_ids(working, [f"{element}_Id"])
            if not err_df.empty:
                bad_ids = list(err_df[f"{element}_Id"].unique())
                err = ui.add_new_error(
                    err,
                    "warn-munger",
                    munger_name,
                    f"Problem cleaning these {element}_Ids:\n{bad_ids}",
                )

    other_elements = [
        element
        for element in constants.all_munge_elements
        if (element[-7:] != "Contest")
        and (element[-9:] != "Selection")
        and (element not in constant_dict.keys())
    ]
    working, new_err = raw_to_id_simple(
        working,
        path_to_jurisdiction_dir,
        other_elements,
        session,
        file_name,
        munger_name,
        juris_true_name,
        file_type,
    )
    if new_err:
        err = ui.consolidate_errors([err, new_err])
        if ui.fatal_error(new_err):
            return working, err

    # add Selection_Id (combines info from BallotMeasureSelection and CandidateContestSelection)
    try:
        working, err = add_selection_id(  # TODO this is where FutureWarning occurs
            working,
            session.bind,
            path_to_jurisdiction_dir,
            juris_true_name,
            file_name,
            munger_name,
            err,
        )
        working, err_df = clean_ids(working, ["Selection_Id"])
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
            juris_true_name,
            "No contests found, or no selections found for contests.",
        )
        return working, err

    return working, err


def get_munge_formulas(
    munger_path: str,
) -> (Dict[str, str], Optional[dict]):
    err = None
    f, new_err = ui.get_parameters(
        required_keys=[],
        optional_keys=constants.all_munge_elements,
        header="munge formulas",
        param_file=munger_path,
    )
    # drop any empty formulas
    f = {k: v for k, v in f.items() if v}
    if new_err:
        err = ui.consolidate_errors([err, new_err])
        if ui.fatal_error(new_err):
            return dict(), err
    return f, err


def order_lookup_keys(dependents: Dict[str, List[str]]) -> List[str]:
    temp = list(dependents.keys())
    key_list = list()
    moved = list()
    while temp:
        k = temp.pop(0)
        if k in moved:
            return list()
        elif [y for y in temp if k in dependents[k]]:
            temp.append(k)
        else:
            key_list.append(k)
    return key_list


def munge_source_to_raw(
    df: pd.DataFrame,
    munger_path: str,
    suffix: str,
    aux_directory_path,
    results_file_path,
) -> (pd.DataFrame, Optional[dict]):
    """NB: assumes columns of dataframe have <suffix> appended already"""
    err = None

    if df.empty:
        return df, err
    munger_name = Path(munger_path).stem
    working = df.copy()

    # # get munge formulas
    # # for all but constant-over-file
    formulas, new_err = get_munge_formulas(munger_path)

    # # get list of all elements for which we have formulas
    elements = [
        k
        for k in formulas.keys()
        if (formulas[k] is not None) and (formulas[k] != "None")
    ]
    # get any lookup info (NB: does not include suffix)
    combo_formula = " ".join([formulas[e] for e in elements])
    aux_params, lookup_map, munge_fields, new_err = get_aux_info(
        combo_formula, munger_path
    )
    if new_err:
        err = ui.consolidate_errors([err, new_err])
        if ui.fatal_error(new_err):
            return working, err

    # if there is auxiliary info, i.e., if lookups need to be done
    if aux_params:
        # get lookup tables
        lookup_table, new_err = get_lookup_tables(
            list(aux_params.keys()),
            aux_params,
            aux_directory_path,
            results_file_path,
            munger_path,
        )
        if new_err:
            err = ui.consolidate_errors([err, new_err])
            if ui.fatal_error(new_err):
                return working, err

        else:
            working, new_err = incorporate_aux_info(
                working,
                lookup_map,
                lookup_table,
                aux_params,
                munger_path,
                suffix,
            )
        if new_err:
            err = ui.consolidate_errors([err, new_err])
            if ui.fatal_error(new_err):
                return working, err

    for element in elements:
        try:
            formula = formulas[element]
            # if formula refers to any fields that need to be looked up, make appropriate replacements
            formula = re.sub("<([^>]*)>", f"<\\1{suffix}>", formula)

            # add col with munged values
            working, new_err = add_column_from_formula(
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
                f"Error interpreting formula for {element}. {exc}",
            )
            return working, err

        try:
            # compress whitespace for <element>_raw
            compression = pd.DataFrame(
                [
                    [x, compress_whitespace(x)]
                    for x in working[f"{element}_raw"].unique()
                ],
                columns=["uncompressed", "compressed"],
            )

            working = (
                working.merge(
                    compression,
                    left_on=f"{element}_raw",
                    right_on="uncompressed",
                )
                .drop([f"{element}_raw", "uncompressed"], axis=1)
                .rename(columns={"compressed": f"{element}_raw"})
            )
        except Exception as exc:
            err = ui.add_new_error(
                err,
                "system",
                f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
                f"Unexpected exception while compressing whitespace for {element}: {exc}",
            )
            return working, err
    # drop all source columns
    source_cols = [c for c in working.columns if c[-len(suffix) :] == suffix]
    working.drop(source_cols, axis=1, inplace=True)

    return working, err


def get_lookup_tables(
    foreign_key_list: List[str],
    aux_params: Dict[str, Dict[str, Any]],
    aux_directory_path: str,
    results_file_path: str,
    munger_path: str,
) -> (Dict[str, pd.DataFrame], Optional[dict]):
    err = None
    lookup_table = dict()
    munger_name = Path(munger_path).stem
    for fk in foreign_key_list:
        # grab the lookup table
        if aux_params[fk]["source_file"]:
            lt_path = os.path.join(aux_directory_path, aux_params[fk]["source_file"])
        else:
            lt_path = results_file_path
        lookup_df_dict, row_constants, fk_err = ui.read_single_datafile(
            lt_path,
            aux_params[fk],
            munger_path,
            None,
            aux=True,
            driving_path=aux_params[fk]["lookup_id"],
            lookup_id=aux_params[fk]["lookup_id"],
        )
        if len(lookup_df_dict) > 1:
            fk_err = ui.add_new_error(
                fk_err,
                "munger",
                munger_name,
                f"Specify lookup sheet with sheets_to_read_names parameter in lookup section of munger. Sheets in lookup file {Path(lt_path).name} are:\n{list(lookup_df_dict.keys())}",
            )
        elif len(lookup_df_dict) == 0:
            fk_err = ui.add_new_error(
                fk_err,
                "munger",
                munger_name,
                f"Nothing read from lookup file {Path(lt_path).name} for foreign key {fk}",
            )

        if fk_err:
            err = ui.consolidate_errors([err, fk_err])
            if ui.fatal_error(fk_err):
                return pd.DataFrame(), err

        # only one key (per error-handling above), so this is the one we want
        sheet_name = list(lookup_df_dict.keys())[0]

        lookup_df = lookup_df_dict[sheet_name]
        lookup_key_cols = aux_params[fk]["lookup_id"].split(",")

        # clean the lookup table
        lookup_df = clean_strings(lookup_df, lookup_df.columns)
        # if any lookup keys are duplicated, delete all but the first record
        lookup_df.drop_duplicates(subset=lookup_key_cols, inplace=True)
        lookup_table[fk] = lookup_df

    return lookup_table, err


def get_and_check_munger_params(
    munger_path: str, results_dir: Optional[str] = None
) -> (dict, Optional[dict]):
    """Checks that munger parameter file is internally consistent.
    If results_dir is included, then existence of any required
    auxiliary files is checked as well"""
    raw_params, err = ui.get_parameters(
        required_keys=list(constants.req_munger_parameters.keys()),
        optional_keys=list(constants.opt_munger_data_types.keys()),
        param_file=munger_path,
        header="format",
        err=None,
    )
    if ui.fatal_error(err):
        return dict(), err

    if raw_params["file_type"] in constants.no_param_file_types:
        return raw_params, err

    # get name of munger for error reporting
    munger_name = Path(munger_path).stem

    # define dictionary of munger parameters
    data_types = {
        **{
            k: constants.req_munger_parameters[k]["data_type"]
            for k in constants.req_munger_parameters.keys()
        },
        **constants.opt_munger_data_types,
    }
    params, new_err = jm.recast_options(raw_params, data_types, munger_name)

    # additional parameters
    # collect necessary row number (for constant_over_sheet_or_block items) into rows_with_constants
    # and columns referenced by number in munge formulas
    params["rows_with_constants"] = list()
    params["columns_referenced_by_munge_formulas"] = list()

    # Check munger values
    # # main parameters recognized
    for k in constants.req_munger_parameters.keys():
        if constants.req_munger_parameters[k]["data_type"] == "string":
            if not params[k] in constants.req_munger_parameters[k]["allowed_values"]:
                err = ui.add_new_error(
                    err,
                    "munger",
                    munger_name,
                    f'Value of {k} must be one of these: {constants.req_munger_parameters[k]["allowed_values"]}',
                )
        elif constants.req_munger_parameters[k]["data_type"] == "list_of_strings":
            bad = [
                x
                for x in params[k]
                if x not in constants.req_munger_parameters[k]["allowed_values"]
            ]
            if bad:
                err = ui.add_new_error(
                    err,
                    "munger",
                    munger_name,
                    f'Each listed value of {k} must be one of these: {constants.req_munger_parameters[k]["allowed_values"]}',
                )

    # # simple non-null dependencies
    for k0 in constants.munger_dependent_reqs.keys():
        for k1 in constants.munger_dependent_reqs[k0]:
            for v2 in constants.munger_dependent_reqs[k0][k1]:
                if (params[k0] == k1) and (not params[v2]):
                    err = ui.add_new_error(
                        err, "munger", munger_name, f"{k0}={k1}', but {v2} not found"
                    )

    # # extra requirements for xml
    if params["file_type"] == "xml":
        # check count_location has correct format
        if params["count_location"]:
            cl_parts = params["count_location"].split(".")
            if len(cl_parts) > 2:
                err = ui.add_new_error(
                    err,
                    "munger",
                    munger_name,
                    f"count_location parameter can have at most one period (.)",
                )
        else:
            err = ui.add_new_error(
                err,
                "munger",
                munger_name,
                f"count_locations parameter is missing, or missing information",
            )
        if ui.fatal_error(err):
            return dict(), err

    # # extra compatibility requirements for excel or flat text files
    elif params["file_type"] in ["excel", "flat_text"]:
        # # count_field_name_row is given where required
        if (params["count_field_name_row"] is None) and (
            params["count_location"] == "by_name"
        ):
            err = ui.add_new_error(
                err,
                "munger",
                munger_name,
                f"file_type={params['file_type']}' but count_field_name_row not found",
            )

    # get all munge fields
    params["munge_fields"], new_err = get_string_fields_from_munger(munger_path)
    if new_err:
        err = ui.consolidate_errors([err, new_err])

    # check formulas are well-formed and consistent with count_location for xml
    if params["file_type"] == "xml":
        xml_field_pattern = re.compile(r"^(\w+)[./]\w+$")
        tags = params["count_location"].split(".")[0].split("/")
        for field in params["munge_fields"]:
            tag = re.findall(xml_field_pattern, field)
            if not tag:
                err = ui.add_new_error(
                    err,
                    "munger",
                    munger_name,
                    f"Field in munge formula not well-formed: <{field}>",
                )
            elif tag[0] not in tags:
                err = ui.add_new_error(
                    err,
                    "munger",
                    munger_name,
                    f"Munge formula element ({tag[0]}) (in {field}) not found in count_location/lookup_id path",
                )
    elif params["file_type"] == "json-nested":
        # check json formulas are well-formed
        for mf in params["munge_fields"]:
            # at most one /
            if len(mf.split("/")) > 2:
                err = ui.add_new_error(
                    err,
                    "munger",
                    munger_name,
                    f"Munge field has more than one /: <{mf}>",
                )

    # check formulas are well-formed and consistent for excel, flat files.
    elif params["file_type"] in ["excel", "flat_text"]:
        # collect header rows in formulas into count_header_row_numbers list
        params["count_header_row_numbers"] = list()

        # classify munge fields
        mf_by_type = {
            "in_count_headers": set(),
            "by_column_name": set(),
            "by_column_number": set(),
            "constant_over_sheet_or_block": set(),
        }
        for mf in params["munge_fields"]:
            if mf[:13] == "count_header_":
                try:
                    params["count_header_row_numbers"].append(int(mf[13:]))
                    mf_by_type["in_count_headers"].update({mf})
                except ValueError:
                    mf_by_type["by_column_name"].update({mf})
            elif mf[:4] == "row_":
                try:
                    params["rows_with_constants"].append(int(mf[4:]))
                    mf_by_type["constant_over_sheet_or_block"].update({mf})
                except ValueError:
                    mf_by_type["by_column_name"].update({mf})
            elif mf[:7] == "column_":
                try:
                    params["columns_referenced_by_munge_formulas"].append(int(mf[7:]))
                    mf_by_type["by_column_number"].update({mf})
                except ValueError:
                    pass
            elif mf == "sheet_name":
                mf_by_type["constant_over_sheet_or_block"].update({mf})
            else:
                mf_by_type["by_column_name"].update({mf})

        # calculate munge_field_types parameter (and warn if overwriting)
        if params["munge_field_types"]:
            err = ui.add_new_error(
                err,
                "warn-munger",
                munger_name,
                "given munge_field_types parameter ignored -- will be derived from formulas",
            )
        params["munge_field_types"] = [
            k for k, v in mf_by_type.items() if len(mf_by_type[k]) != 0
        ]
        if params["constant_over_file"]:
            params["munge_field_types"].append("constant_over_file")

        # if all rows are data
        if params["all_rows"] == "data":
            # then we don't have multi-blocks.
            if params["multi_block"] and params["multi_block"] == "yes":
                err = ui.add_new_error(
                    err,
                    "munger",
                    munger_name,
                    f"all_rows=data and multi_block=yes are not compatible",
                )
        # if any non-count columns are referenced by field name but no header row given
        if mf_by_type["by_column_name"] and params["noncount_header_row"] is None:
            err = ui.add_new_error(
                err,
                "munger",
                munger_name,
                f"No noncount_header row specified, so can't find column names matching these "
                f"field from the formulas:\n{mf_by_type['by_column_name']}",
            )

    # check that each lookup section has a replacement formula for each element referencing the lookup field
    # and check that each auxiliary file exists
    headers, new_err = ui.get_section_headers(munger_path)
    if new_err:
        err = ui.consolidate_errors([err, new_err])
        if ui.fatal_error(new_err):
            return params, err

    pattern = re.compile(r"^(.*) lookup$")
    for h in headers:
        try:
            lookup_field = pattern.findall(h)[0]
        except IndexError:
            # if nothing found, this isn't a lookup section, so skip it.
            continue
        # check required items for lookup
        required, new_err = ui.get_parameters(
            required_keys=["source_file", "lookup_id", "file_type"],
            param_file=munger_path,
            header=h,
        )
        if new_err:
            err = ui.consolidate_errors([err, new_err])
        if results_dir:
            aux_file_path = os.path.join(results_dir, required["source_file"])
            if not os.path.isfile(aux_file_path):
                err = ui.add_new_error(
                    err,
                    "munger",
                    munger_name,
                    f"Auxiliary file not found: {required['source_file']}",
                )
        # TODO check usual format items for reading aux file

    return params, err


def get_string_fields_from_munger(
    munger_path: str,
) -> (List[str], Optional[dict]):
    """Finds the field names expected by the munger formulas"""
    err = None
    formulas, new_err = ui.get_parameters(
        required_keys=[],
        optional_keys=constants.all_munge_elements,
        header="munge formulas",
        param_file=munger_path,
    )
    if new_err:
        err = ui.consolidate_errors([err, new_err])
        if ui.fatal_error(new_err):
            return dict(), err
    munge_field_list, _ = extract_fields_from_formulas(formulas.values())
    return munge_field_list, err


def extract_fields_from_formulas(
    formulas: List[str],
    drop_lookups: bool = True,
) -> (List[str], List[str]):
    """If drop_lookups is false, return raw fields (including ... from ... for lookups).
    Otherwise return only the first foreign key field of each chain of lookups."""
    munge_field_set = set()
    foreign_key_set = set()
    angle_pattern = re.compile(
        r"<([^>]+)>"
    )  # anything within brackets (if nested, get lowest level)
    from_pattern = re.compile(r"<([^>,]* from ).*>")
    non_trivial_formulas = [f for f in formulas if f]
    for f in non_trivial_formulas:
        if drop_lookups:
            # for any lookup formulas, delete first field ("*** from ") to be lookup up
            ## and leave the remainder of the chain of lookups
            for x in from_pattern.findall(f):
                f = f.replace(x, "")
        # pull information out of angle brackets
        for found in angle_pattern.findall(f):
            munge_field_set.update(found.split(","))
            foreign_key_set.update([found])
        munge_field_set.update()
    # remove dupes, make list
    munge_field_list = list([x for x in munge_field_set if x != ""])
    foreign_key_list = list(foreign_key_set)
    return munge_field_list, foreign_key_list


def check_formula(formula: str) -> Optional[str]:
    """Runs syntax checks on a concatenation (or concatenation-with-regex) formula"""
    # brace pairs { ... , .... } contain well-formed regex as second entry with one capturing group
    err_str_list = list()
    cr_pairs = constants.brace_pattern.findall(formula)
    for _, regex in cr_pairs:
        # test regex well-formed
        try:
            # test regex has exactly one capturing group
            group_count = re.compile(regex).groups
            if group_count != 1:
                err_str_list.append(
                    f"Regular expression {regex} has {group_count} capturing groups but should have just one."
                )
        except re.error as re_err:
            err_str_list.append(f"Regular expression error in '{regex}': {re_err}")
    err_str = "\n".join(err_str_list)
    return err_str


def fill_blanks(
    df: pd.DataFrame,
    row_list: List[int],
    merged_cells: bool,
) -> pd.DataFrame:
    """Fills blank cells. If data is from an excel file with merged cells,
    fill blanks from nearest non-blank to the left. Otherwise
    fill blanks with the pandas default."""
    df_new = df.copy()
    if merged_cells:
        for i in row_list:
            prev_non_blank = None
            for j in df_new.columns:
                if df_new.loc[i, j] == "":
                    if prev_non_blank:
                        df_new.loc[i, j] = prev_non_blank
                    else:
                        df_new.loc[i, j] = f"Unnamed: {j}_level_{i}"
                else:
                    prev_non_blank = df_new.loc[i, j]

    else:
        for i in row_list:
            for j in range(df_new.shape[1]):
                if df_new.loc[i, j] == "":
                    # pandas default
                    df_new.loc[i, j] = f"Unnamed: {j}_level_{i}"
    return df_new


def get_count_cols_by_name(
    df: pd.DataFrame,
    p: Dict[str, Any],
    munger_name: str,
    use_rows: Optional[List[int]] = None,
) -> (List[str], Optional[dict]):
    err = None
    df_new = df.copy()
    # define count columns
    if p["count_location"] == "by_number":
        if use_rows:
            # use default index 0, 1, 2,...
            df_new.reset_index(inplace=True, drop=True)
            # to match how pandas handles indices, fill blanks with Unnamed
            df_new = fill_blanks(df_new, use_rows, (p["merged_cells"] == "yes"))
            # if there is more than one header row in use_rows, need multi-index of tuples
            if len(use_rows) > 1:
                count_columns = list(
                    {
                        tuple(df_new.loc[use_rows, idx])
                        for idx in p["count_column_numbers"]
                        if idx < df_new.shape[1]
                    }
                )
            else:
                count_columns = list(
                    {
                        df_new.loc[use_rows[0], idx]
                        for idx in p["count_column_numbers"]
                        if idx < df_new.shape[1]
                    }
                )

        else:
            count_columns = [
                df_new.columns[idx]
                for idx in p["count_column_numbers"]
                if idx < df_new.shape[1]
            ]
    elif p["count_location"] == "by_name":
        if isinstance(df_new.columns, pd.MultiIndex):
            err = ui.add_new_error(
                err,
                "munger",
                munger_name,
                "If there are multiple header rows, need to have count_location=by_number ",
            )
            return list(), err
        count_columns = [c for c in p["count_fields_by_name"] if c in df_new.columns]
    else:
        err = ui.add_new_error(
            err,
            "munger",
            munger_name,
            f"For file type {p['file_type']}, count_location parameter must be either by_number or by_name",
        )
        count_columns = list()
    return count_columns, err


def to_standard_count_frame(
    file_path: str,
    munger_path: str,
    p: dict,
    suffix: Optional[str] = None,
) -> (pd.DataFrame, Optional[dict]):
    """Read data from file at <f_path>; return a standard dataframe with one clean count column
    and all other columns typed as 'string'.
     If <suffix> is given, append <suffix> to all non-count columns"""

    # set up
    file_name = Path(file_path).name
    munger_name = Path(munger_path).stem
    err = None
    # initialize error, count_cols dictionaries
    error_by_df = dict()
    cc_by_name = dict()

    # get lists of string fields expected in raw file
    try:
        munge_string_fields, new_err = get_string_fields_from_munger(munger_path)
    except Exception as exc:
        err = ui.add_new_error(
            None,
            "system",
            f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
            f"Exception while getting string fields: {exc}",
        )
        return pd.DataFrame(), err
    if new_err:
        err = ui.consolidate_errors([err, new_err])

    # read count dataframe(s) and constant-over-sheet elements from rows from file
    # # NB: sheet names are the keys
    try:
        raw_dict, row_constants_by_sheet, err = ui.read_single_datafile(
            file_path, p, munger_path, err
        )

        if len(raw_dict) == 0:  # no dfs at all returned
            err = ui.add_new_error(
                err,
                "munger",
                munger_name,
                f"No data found in file {Path(file_path).name}",
            )

        if ui.fatal_error(err):
            return pd.DataFrame(), err
    except Exception as exc:
        err = ui.add_new_error(
            err,
            "system",
            f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
            f"Unexpected exception while reading data from file {file_name}:\n\t{exc}",
        )
        return pd.DataFrame(), err

    # loop over sheets
    standard = dict()
    error_by_sheet = dict()
    keys_in_order = list(raw_dict.keys())
    keys_in_order.sort()
    for sheet in keys_in_order:
        error_by_sheet[sheet] = None
        row_constants = dict()  # for syntax checker
        # set any nulls to blank in dataframe for that sheet
        raw_dict[sheet] = raw_dict[sheet].fillna("")
        if p["multi_block"] == "yes":
            try:
                # extract blocks as dataframes with generic headers and all rows treated as data
                df_list, row_constants, new_err = extract_blocks(
                    raw_dict[sheet],
                    p["rows_with_constants"],
                    munger_name,
                    file_name,
                    sheet,
                    max_blocks=p["max_blocks"],
                )
                if new_err:
                    err = ui.consolidate_errors([err, new_err])
                    if ui.fatal_error(new_err):
                        continue
            except Exception as exc:
                error_by_sheet[sheet] = ui.add_new_error(
                    None,
                    "munger",
                    munger_name,
                    f"Error extracting blocks from sheet {sheet} in {file_name}:\n {exc}",
                )
                continue  # goes to next k in loop

            try:
                # correct column headers
                # # get header list
                p_temp = p.copy()
                p_temp["multi_block"] = None
                header_int_or_list = ui.tabular_kwargs(p_temp, dict())["header"]
                if isinstance(
                    header_int_or_list, int
                ):  # TODO tech debt ugly! but tracks index vs. multiindex
                    header_list = [header_int_or_list]
                else:
                    header_list = header_int_or_list

                # loop over extracted blocks
                merged_cells = p["merged_cells"] == "yes"
                for n in range(len(df_list)):
                    # get count columns by name
                    cc_by_name[n], error_by_df[n] = get_count_cols_by_name(
                        df_list[n], p, munger_name, use_rows=header_list
                    )
                    # TODO to handle cases where noncount_header_row has info
                    #  in cells labeling columns referenced by <column_j>,
                    #  need to put 'column_j' in the header cells. Note that we can do this
                    #  safely only after collecting the <row_i> information ("row_constants")
                    # rename header cells referenced by column_j in munge formulas
                    if p["noncount_header_row"] is None:
                        # because we're multi-block, there is a header row. But if no munge
                        # fields depend on column names, noncount_header_row might not be given.
                        df_list[n] = rename_cells_by_number(
                            df_list[n],
                            0,
                            p["columns_referenced_by_munge_formulas"],
                            "column_",
                        )

                    else:
                        df_list[n] = rename_cells_by_number(
                            df_list[n],
                            p["noncount_header_row"],
                            p["columns_referenced_by_munge_formulas"],
                            "column_",
                        )

                    df_list[n] = ui.set_and_fill_headers(
                        df_list[n], header_list, merged_cells, drop_empties=True
                    )

            except Exception as exc:
                error_by_sheet[sheet] = ui.add_new_error(
                    None,
                    "system",
                    f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
                    f"Error correcting column headers or getting constant info from blocks from sheet {sheet} in {file_name}:\n {exc}",
                )
                continue  # goes to next k in loop

        # if not multi-block
        else:
            working = raw_dict[sheet].copy()
            # if there are column_j fields in the munge formulas
            if p["columns_referenced_by_munge_formulas"]:
                # if columns are multi-indices
                if isinstance(working.columns, pd.MultiIndex):
                    working = rename_column_index_by_number(
                        working,
                        p["noncount_header_row"],
                        p["columns_referenced_by_munge_formulas"],
                        "column_",
                    )
                # if columns are simple indices
                else:
                    new_cols = list(working.columns)
                    for j in p["columns_referenced_by_munge_formulas"]:
                        new_cols[j] = f"column_{j}"
                    working.columns = new_cols
            df_list = [working]
            cc_by_name[0], error_by_df[0] = get_count_cols_by_name(
                df_list[0], p, munger_name
            )

        # loop through dataframes in list
        standard[sheet] = pd.DataFrame()
        for n in range(len(df_list)):
            raw = df_list[n]
            working = raw.copy()
            # some file types are read already into "melted" form
            if p["file_type"] in ["xml", "json-nested"]:
                error_by_df[n] = None

            else:
                # transform to df with single count column 'Count' and all raw munge info in other columns
                try:
                    working, new_err = melt_to_one_count_column(
                        raw, p, cc_by_name[n], munger_name, file_name, sheet_name=sheet
                    )
                    if new_err:
                        error_by_df[n] = ui.consolidate_errors(
                            [new_err, error_by_df[n]]
                        )
                    elif working.empty:
                        error_by_df[n] = ui.add_new_error(
                            err,
                            "munger",
                            munger_name,
                            f"No data returned from pivot in file {file_name}",
                        )
                except Exception as exc:
                    error_by_df[n] = ui.add_new_error(
                        None,
                        "file",
                        file_path,
                        f"Unable to pivot dataframe from sheet {sheet}:\n {exc}",
                    )
                    continue  # goes to next dataframe in list

            # add constant-over-sheet-or-block column
            if p["multi_block"] == "yes":
                for row in p["rows_with_constants"]:
                    working = add_constant_column(
                        working, f"row_{row}", row_constants[n][row]
                    )
            else:
                for row in p["rows_with_constants"]:
                    working = add_constant_column(
                        working, f"row_{row}", row_constants_by_sheet[sheet][row]
                    )

            # add sheet_name column
            working = add_constant_column(working, "sheet_name", sheet)

            # keep only necessary columns
            try:
                necessary = munge_string_fields + ["Count"]
                working = working[necessary]
            except KeyError as ke:
                error_by_df[n] = ui.add_new_error(
                    error_by_df[n],
                    "munger",
                    munger_name,
                    f"In file {file_name} sheet {sheet}: Field in munge formulas not found in file column headers read from file: {ke}. "
                    f"Columns are {working.columns}",
                )
                continue

            # clean Count column (NB: bad rows not reported)
            working, bad_rows = clean_count_cols(
                working, ["Count"], p["thousands_separator"]
            )

            # clean Unnamed:... out of any values
            working = blank_out(working, constants.pandas_default_pattern)

            # append data from the nth dataframe to the standard-form dataframe
            ## NB: if df_list[n] fails it should not reach this statement
            standard[sheet] = pd.concat([standard[sheet], working])

        # if even one df lacks a fatal error, consider all errors non-fatal for this sheet
        non_fatal_dfs = [
            n for n in range(len(df_list)) if not ui.fatal_error(error_by_df[n])
        ]
        if non_fatal_dfs:
            fatal_dfs = [n for n in range(len(df_list)) if n not in non_fatal_dfs]
            for n in fatal_dfs:
                error_by_df[n] = ui.fatal_err_to_non(error_by_df[n])

        # consolidate errors from all dataframes into the error for the sheet
        error_by_sheet[sheet] = ui.consolidate_errors(
            [error_by_df[n] for n in range(len(df_list))]
        )

    # if even one sheet lacks a fatal error, consider all errors non-fatal for this file
    non_fatal_sheets = [
        k for k in raw_dict.keys() if not ui.fatal_error(error_by_sheet[k])
    ]
    if non_fatal_sheets:
        fatal_sheets = [k for k in raw_dict.keys() if ui.fatal_error(error_by_sheet[k])]
        for sheet in fatal_sheets:
            error_by_sheet[sheet] = ui.fatal_err_to_non(error_by_sheet[sheet])
        df = pd.concat([standard[k] for k in non_fatal_sheets])
    else:
        df = pd.DataFrame()
    err = ui.consolidate_errors([error_by_sheet[k] for k in raw_dict.keys()])

    # if even one sheet was not fatally flawed
    if suffix and non_fatal_sheets:
        # append suffix to all non-Count column names (to avoid conflicts if e.g., source has col names 'Party'
        try:
            df.columns = [c if c == "Count" else f"{c}{suffix}" for c in df.columns]
        except Exception as exc:
            err = ui.add_new_error(
                err,
                "system",
                f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
                f"Exception while appending suffix {suffix}: {exc}",
            )

    return df, err


def fill_vote_count(
    df: pd.DataFrame,
    session,
    munger_name,
    err: Optional[dict],
) -> Optional[dict]:

    working = df.copy()
    # restrict to just the VoteCount columns (so that groupby.sum will work)
    vc_cols = [
        "Count",
        "CountItemType",
        "ReportingUnit_Id",
        "Contest_Id",
        "Selection_Id",
        "Election_Id",
        "_datafile_Id",
    ]
    working = working[vc_cols]

    # TODO there are edge cases where this might include dupes
    #  that should be omitted. E.g., if data mistakenly read twice
    # Sum any rows that were disambiguated (otherwise dupes will be dropped
    #  when VoteCount is filled)
    group_cols = [c for c in working.columns if c != "Count"]
    working = working.groupby(group_cols).sum().reset_index()

    # Fill VoteCount
    try:
        new_err = db.insert_to_cdf_db(
            session.bind, working, "VoteCount", "munger", munger_name
        )
        if new_err:
            err = ui.consolidate_errors([err, new_err])
            return err
    except Exception as exc:
        err = ui.add_new_error(
            err,
            "system",
            f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
            f"Unexpected exception ({exc}) "
            f"while inserting this data into VoteCount:\n{working}",
        )

    return err


def get_aux_info(
    formula: str, munger_path: str
) -> (Dict[str, Dict[str, Any]], Dict[str, List[str]], List[str], Optional[dict]):
    """returns:
        aux_params, including lookup_id and other info for reading lookup file
            (keys are bare foreign keys)
        lookup_map, mapping foreign keys to the fields looked up from them
            (keys are foreign keys with "from")
    for all lookups arising from the formula
    NB: each foreign key is a list"""

    err = None
    # initialize dictionaries
    aux_params = dict()
    # get list of all fields that will be needed
    raw_fields, foreign_keys = extract_fields_from_formulas(
        [formula], drop_lookups=False
    )
    # get map from foreign key to all values looked up from that key, for all fields
    # NB: foreign keys include the "from"; values do not.
    lookup_map = get_lookedup_fields(foreign_keys)
    # get all bare foreign keys from lookup_map keys (which have " from ")
    bfks = {k.split(" from ")[0] for k in lookup_map.keys()}
    for fk in bfks:
        # if there is a lookup for this field, grab it
        f_p, f_err = ui.get_parameters(
            required_keys=["lookup_id"],
            optional_keys=list(constants.opt_munger_data_types.keys())
            + ["source_file", "file_type"],
            header=f"{fk} lookup",
            param_file=munger_path,
        )
        # convert parameters to appropriate types
        type_dict = {
            **constants.opt_munger_data_types,
            **{
                k: constants.req_munger_parameters[k]["data_type"]
                for k in constants.req_munger_parameters.keys()
            },
        }
        f_p, new_err = jm.recast_options(f_p, type_dict, Path(munger_path).stem)
        if new_err:
            ui.consolidate_errors([new_err, err])

        # define calculated parameters
        f_p["munge_fields"] = list(set([f for v in lookup_map.values() for f in v]))
        f_p[
            "rows_with_constants"
        ] = list()  # no lookup info is constant over rows of file

        if f_err:
            err = ui.consolidate_errors([err, f_err])
        else:
            # prepare dictionary to hold info for this lookup
            # if source_file is None, grab format parameters for the aux file reading from the [format] section of the munger
            if ("source_file" not in f_p.keys()) or (not f_p["source_file"]):
                # we read all data from the original file (e.g., for xml)
                main_format_params, new_err = ui.get_parameters(
                    required_keys=list(constants.req_munger_parameters.keys()),
                    optional_keys=list(constants.opt_munger_data_types.keys()),
                    header="format",
                    param_file=munger_path,
                )
                recast_params, new_err = jm.recast_options(
                    main_format_params, type_dict, Path(munger_path).stem
                )
                if new_err:
                    err = ui.consolidate_errors([err, new_err])
                f_p.update(recast_params)
            aux_params[fk] = f_p

    return aux_params, lookup_map, raw_fields, err


def incorporate_aux_info(
    df: pd.DataFrame,
    lookup_map: Dict[str, List[str]],
    lookup_table: Dict[str, pd.DataFrame],
    aux_params: Dict[str, Dict[str, Any]],
    munger_path: str,  # for error reporting
    suffix: str,
) -> (pd.DataFrame, Optional[Dict[str, Any]]):
    """revises the dataframe, adding necessary columns obtained from lookup tables,
    and revises the formula to pull from those columns instead of foreign key columns
    Note cols are assumed to have suffix, but aux does not include suffix"""
    w_df = df.copy()
    err = None  # TODO error handling
    ## set order for lookups
    from_count = {k: len(re.findall("(?= from )", k)) for k in lookup_map.keys()}
    max_from_count = max(from_count.values())
    ordered_fk_with_froms = list()
    for fc in range(max_from_count + 1):
        ordered_fk_with_froms += [k for k in lookup_map.keys() if from_count[k] == fc]

    # add lookup columns
    for fk_with_from in ordered_fk_with_froms:
        # join lookup for this foreign key to working dataframe
        fk = fk_with_from.split(" from ")[0]
        working_fk_cols = [f"{c}{suffix}" for c in fk_with_from.split(",")]
        lookup_fk_cols = aux_params[fk]["lookup_id"].split(",")
        w_df = w_df.merge(
            lookup_table[fk],
            how="left",
            left_on=working_fk_cols,
            right_on=lookup_fk_cols,
        )
        # rename looked-up columns to incorporate the "from" and add suffix
        rename = {
            c: f"{c} from {fk_with_from}{suffix}" for c in lookup_table[fk].columns
        }
        w_df.rename(columns=rename, inplace=True)

    return w_df, err


def get_fields_from_formula(formula: str) -> List[str]:
    texts_and_fields, final_text = text_fragments_and_fields(formula)
    fields = [x[1] for x in texts_and_fields]
    return fields


def extract_blocks(
    df: pd.DataFrame,
    rows_to_read: List[int],
    munger_name: str,
    file_name: str,
    sheet_name: str,
    max_blocks: Optional[int] = None,
) -> (List[pd.DataFrame], Dict[int, Dict[int, str]], Optional[dict]):
    """Given a dataframe, create a list of dataframes -- one for each block of
    data lines"""

    # set up
    if df.empty:
        err = ui.add_new_error(
            None,
            "warn-munger",
            munger_name,
            f"No data found in sheet {sheet_name} of file {file_name}",
        )
        return list(), err
    else:
        err = None
    df_list = list()
    working = df.copy()
    row_constants = dict()
    # NB: no info is in column headers because multi_block=yes sets header=None when data is read

    # identify count rows (have at least one integer), blank rows, and text rows (all others)
    mask_count = working.T.astype(str).apply(lambda row: row.str.isdigit().any())
    mask_blank = working.T.apply(
        lambda row: list(row.unique()) == [""]
    )  # is this the best way?
    count_rows = list(mask_count[mask_count].index)
    text_rows = list(mask_count[(~mask_count) & (~mask_blank)].index)

    # initialize check on max number of blocks
    max_blocks_attained = False
    blocks_created = 0

    # loop through blocks starting at the top (blocks defined by text lines on top)
    while text_rows and count_rows and not max_blocks_attained:
        first_text_row = min(text_rows)
        first_count_row = min(
            [n for n in count_rows if n > first_text_row]
        )  # TODO what if there is none?

        # remove this block's rows from text and count lists
        text_rows = [n for n in text_rows if n > first_count_row]
        if text_rows:
            block_end = min(text_rows)
        else:
            block_end = working.shape[0]
        count_rows = [n for n in count_rows if n > block_end]

        block = working[first_text_row:block_end]

        ## add block to list
        df_list.append(block)

        ## get row constant for block (labeled by block number)
        row_constants[len(df_list) - 1], new_err = ui.build_row_constants_from_df(
            block, rows_to_read, file_name, sheet_name
        )
        if new_err:
            err = ui.consolidate_errors([err, new_err])

        # if a maximum number of blocks was specified
        if max_blocks:
            blocks_created += 1
            if blocks_created >= max_blocks:
                max_blocks_attained = True
    return df_list, row_constants, err


def file_to_raw_df(
    munger_path: str,
    p,
    f_path: str,
    results_directory_path,
) -> (pd.DataFrame, Optional[dict]):
    err = None

    # read data into standard count format dataframe
    #  append "_SOURCE" to all non-Count column names
    #  (to avoid conflicts if e.g., source has col names 'Party')
    try:
        df, err = to_standard_count_frame(
            f_path,
            munger_path,
            p,
            suffix="_SOURCE",
        )
        if ui.fatal_error(err):
            return pd.DataFrame(), err
    except Exception as exc:
        err = ui.add_new_error(
            err,
            "system",
            f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
            f"Exception while converting data to standard form: {exc}",
        )
        return pd.DataFrame(), err
    # clean non-count columns
    non_count = [c for c in df.columns if c != "Count"]
    df = clean_strings(df, non_count)

    # transform source to completely munged (possibly with foreign keys if there is aux data)
    # # add raw-munged column for each element, removing old
    try:
        df, new_err = munge_source_to_raw(
            df,
            munger_path,
            "_SOURCE",
            results_directory_path,
            f_path,
        )
        if new_err:
            err = ui.consolidate_errors([err, new_err])
            if ui.fatal_error(new_err):
                return df, err
    except Exception as exc:
        err = ui.add_new_error(
            err,
            "system",
            f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
            f"Exception while munging source to raw: {exc}",
        )
        return df, err

    return df, err


def add_constants_to_df(
    df: pd.DataFrame, constant_dict: Dict[str, Any]
) -> pd.DataFrame:
    for element in constant_dict.keys():
        df = add_constant_column(
            df,
            element,
            constant_dict[element],
        )
    return df


def get_lookedup_fields(raw_fields) -> Dict[str, List[str]]:
    # dedupe raw fields list
    lookup_fields = [x for x in list(set(raw_fields)) if " from " in x]
    fk_map = dict()
    for f in lookup_fields:
        parts = f.split(" from ")
        for j in range(len(parts) - 1):
            val = parts[j]
            fk = " from ".join(parts[j + 1 :])
            if fk in fk_map.keys():
                fk_map[fk].append(val)
            else:
                fk_map[fk] = [val]
    return fk_map


def remove_ignored_rows(df: pd.DataFrame, munger_path: str) -> pd.DataFrame:
    working = df.copy()
    ig, new_err = ui.get_parameters(
        header="ignore",
        required_keys=[],
        optional_keys=[
            "Candidate",
            "CandidateContest",
            "BallotMeasureSelection",
            "BallotMeasureContest",
            "Party",
            "ReportingUnit",
        ],
        param_file=munger_path,
    )
    # NB: errors ignored, as header is not required

    # delete rows to be ignored
    real_ig_keys = [x for x in ig.keys() if ig[x] is not None]
    for element in real_ig_keys:
        value_list = ig[element].split(",")
        working = working[~working[f"{element}_raw"].isin(value_list)]

    return working


def blank_out(df: pd.DataFrame, regex: str) -> pd.DataFrame:
    p = re.compile(regex)
    new = df.copy()
    for c in df.columns:
        try:
            new[c] = df[c].str.replace(p, "")
        except Exception:
            pass
    return new


def rename_column_index_by_number(
    df: pd.DataFrame,
    row: Optional[int],
    cols: List[int],
    prefix: str,
) -> pd.DataFrame:
    midx_list = df.columns.to_list()
    for j in cols:
        new_list = list(midx_list[j])
        new_list[row] = f"{prefix}{j}"
        midx_list[j] = tuple(new_list)
    new_midx = pd.MultiIndex.from_tuples(midx_list, names=range(len(midx_list[0])))
    working = df.copy()
    working.columns = new_midx
    return working


def rename_cells_by_number(
    df: pd.DataFrame,
    row: Optional[int],
    cols: List[int],
    prefix: str,
) -> pd.DataFrame:
    """Renames cells in the given <row> and columns
    (identified by column numbers <cols>)
    by the column number (preceded by the prefix) .
    """
    working = df.copy()
    if (row is not None) and cols:
        for j in cols:
            try:
                working.iloc[row, j] = f"{prefix}{j}"
            except:
                pass
    else:
        pass
    return working
