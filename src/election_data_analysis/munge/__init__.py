import inspect
from pathlib import Path
import election_data_analysis as eda
from election_data_analysis import database as db
from election_data_analysis import user_interface as ui
from election_data_analysis import juris_and_munger as jm
import pandas as pd
from pandas.api.types import is_numeric_dtype
from typing import Optional, List, Dict, Any
import re
import os
from sqlalchemy.orm.session import Session

# constants
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
    "string_field_column_numbers": "list-of-integers",
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



def clean_count_cols(
    df: pd.DataFrame,
    cols: Optional[List[str]],
    thousands: Optional[str] = None
) -> (pd.DataFrame, pd.DataFrame):
    """Casts the given columns as integers, replacing any bad
    values with 0 and reporting a dataframe of any rows so changed.
    If <thousands> separator is given, check for it"""
    if cols is None:
        return df, pd.DataFrame(columns=df.columns)
    else:
        err_df = pd.DataFrame()
        working = df.copy()
        for c in cols:
            if c in working.columns:
                # remove the thousands separator if the column is not already int64
                if thousands and (df.dtypes[c] == "object"):
                    working[c] = working[c].str.replace(thousands,"")
                mask = ~working[c].astype(str).str.isdigit()
                if mask.any():
                    # return bad rows for error reporting
                    err_df = pd.concat([err_df, working[mask]]).drop_duplicates()

                    # cast as int, changing any non-integer values to 0
                    working[c] = (
                        pd.to_numeric(working[c], errors="coerce").fillna(0).astype("int64")
                    )
                else:
                    working[c] = working[c].astype("int64")
        return working, err_df


def clean_ids(
    df: pd.DataFrame,
    cols: List[str],
) -> (pd.DataFrame(), pd.DataFrame):
    """changes only the columns to of numeric type; changes them
    to integer, with any nulls changed to 0. Reports a dataframe of
    any rows so changed. Non-numeric-type columns are changed to all 0"""

    if cols == list():
        return df, pd.DataFrame()
    err_df = pd.DataFrame()
    working = df.copy()
    for c in cols:
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
            print(f"Could not convert column {c} to strings")

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
                mask = working[c].apply(lambda x: isinstance(x,str))
                working.loc[mask, c] = working[c][mask].apply(compress_whitespace)
            except (AttributeError, TypeError):
                pass
    return working


def clean_column_names(
    df: pd.DataFrame,
    count_cols: List[str],
) -> (pd.DataFrame, List[str], Optional[str]):
    working = df.copy()

    err_str = None
    # remove any columns with duplicate names
    new_working = working.loc[:, ~working.columns.duplicated()]
    # if something dropped, warn user
    if new_working.shape != working.shape:
        err_str = f"Duplicate column names found; these columns were dropped"
    # restrict count_cols to columns of working
    if count_cols:
        new_count_cols = [c for c in working.columns if c in count_cols]
    else:
        new_count_cols = None

    # strip any whitespace from column names
    if isinstance(working.columns, pd.MultiIndex):
        for j in range(len(working.columns.levels)):
            # strip whitespace at level j
            working.columns = working.columns.set_levels(
                working.columns.levels[j].str.strip(), level=j
            )
        # TODO strip whitespace from each item in count_cols as well
    else:
        working.columns = [c.strip() for c in working.columns]
        if new_count_cols:
            new_count_cols = [c.strip() for c in new_count_cols]
    return working, new_count_cols, err_str


def cast_cols_as_int(
    df: pd.DataFrame,
    col_list: list,
    mode="name",
    error_msg="",
    munger_name="unknown",
) -> (pd.DataFrame, dict):
    """recast columns as integer where possible, leaving columns with text entries as non-numeric)"""
    err = None
    if mode == "index":
        num_columns = [df.columns[idx] for idx in col_list]
    elif mode == "name":
        num_columns = [c for c in df.columns if c in col_list]
    else:
        err = ui.add_new_error(
            err,
            "system",
            "munge.cast_cols_as_int",
            f"Mode {mode} not recognized",
        )
        return df, err
    for c in num_columns:
        try:
            df[c] = df[c].astype("int64", errors="raise")
        except ValueError as e:
            err = ui.add_new_error(
                err,
                "warn-munger",
                munger_name,
                f"{error_msg}\nColumn {c} cannot be cast as integer:\n{e}",
            )
    return df, err


def munge_clean(
    raw: pd.DataFrame, munger: jm.Munger, count_columns_by_name: List[str]
) -> (pd.DataFrame, dict):
    """Drop unnecessary columns.
    Append '_SOURCE' suffix to raw column names to avoid conflicts"""
    err = None
    working = raw.copy()
    working, count_columns_by_name, e = clean_column_names(
        working, count_cols=count_columns_by_name
    )
    try:
        #  define columns named in munger formulas (both plain from 'row' sourced info and
        #  'variable_j' from column-sourced)
        munger_formula_row_sourced = [
            x for x in munger.field_list if x in working.columns
        ]
        munger_formula_column_sourced = [
            f"variable_{j}"
            for j in munger.field_list
            if f"variable_{j}" in working.columns
        ]

        # keep columns named in munger formulas; keep count columns; drop all else.
        working = working[
            munger_formula_row_sourced
            + munger_formula_column_sourced
            + count_columns_by_name
        ]

        # add suffix '_SOURCE' to certain columns to avoid any conflict with db table names
        # (since no db table name ends with _SOURCE)

        renamer = {x: f"{x}_SOURCE" for x in munger_formula_row_sourced}
        working.rename(columns=renamer, inplace=True)
    except Exception as e:
        err = ui.add_new_error(err, "system", "munge.munge_clean", f"Unexpected error: {e}")
    return working, err


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
        # # put informative error message in new_col
        old = working[old_col].copy()
        working[new_col] = working[old_col].str.cat(old, f"Does not match regex {pattern_str}: ")
        # # where regex succeeds, replace error message with good value
        mask = working[old_col].str.match(p)
        working.loc[mask, new_col] = working[mask][old_col].str.replace(p, "\\1")

    except re.error as e:
        err = ui.add_new_error(
            err,
            "munger",
            munger_name,
            f"Regex error in {pattern_str}"
        )
    except Exception as e:
        err = ui.add_new_error(
            err,
            "system",
            "munge.add_regex_column",
            f"Unexpected exception: {e}"
        )

    return working, err


def text_fragments_and_fields(formula):
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
    brace_pattern = re.compile(r"{<([^,]*)>,([^{}]*|[^{}]*{[^{}]*}[^{}]*)}")

    try:
        temp_cols = []
        for x in brace_pattern.finditer(formula):
            # create a new column with the extracted info
            old_col, pattern_str = x.groups()
            temp_col = f"extracted_from_{old_col}"
            w, new_err = add_regex_column(w, old_col, temp_col, pattern_str, munger_name)
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
            w.loc[:, new_col] = last_text[0]
        else:
            w.loc[:, new_col] = ""
        text_field_list.reverse()
        for t, f in text_field_list:
            try:
                w.loc[:, new_col] = (
                    w.loc[:, f].apply(lambda x: f"{t}{x}") + w.loc[:, new_col]
                )
            except KeyError as ke:
                err = ui.add_new_error(
                    err,
                    "munger",
                    munger_name,
                    f"Expected transformed column '{f}' not found, "
                    f"perhaps because of mismatch between munger and results file.",
                )
                return w, err

    except Exception as e:
        err = ui.add_new_error(
            err, "system", "munge.add_column_from_formula", f"Unexpected error: {e}"
        )

    # delete temporary columns
    w.drop(temp_cols, axis=1, inplace=True)
    return w, err


def add_munged_column(
    raw: pd.DataFrame,
    munger: jm.Munger,
    element: str,
    err: Optional[dict],
    mode: str = "row",
    inplace: bool = True,
) -> (pd.DataFrame, dict):
    """Alters dataframe <raw>, adding or redefining <element>_raw column
    via the <formula>. Assumes "_SOURCE" has been appended to all columns of raw
    Does not alter row count."""
    if raw.empty:
        return raw, err
    if inplace:
        working = raw
    else:
        working = raw.copy()

    try:
        formula = munger.cdf_elements.loc[element, "raw_identifier_formula"]
        if mode == "row":
            for field in munger.field_list:
                formula = formula.replace(f"<{field}>", f"<{field}_SOURCE>")
        elif mode == "column":
            for i in range(munger.options["header_row_count"]):
                formula = formula.replace(f"<{i}>", f"<variable_{i}>")

        working, new_err = add_column_from_formula(
            working, formula, f"{element}_raw", err, munger.name
        )
        if new_err:
            err = ui.consolidate_errors([err, new_err])
            if ui.fatal_error(new_err):
                return working, err

        # correct any disambiguated names back to the original
        if element in munger.alt.keys():
            working.replace({f"{element}_raw": munger.alt[element]}, inplace=True)

    except Exception as e:
        err = ui.add_new_error(
            err,
            "munger",
            munger.name,
            f"Error interpreting formula for {element} in cdf_element.txt. {e}",
        )
        return working, err

    # compress whitespace for <element>_raw
    working.loc[:, f"{element}_raw"] = working[f"{element}_raw"].apply(
        compress_whitespace
    )
    return working, err


def compress_whitespace(s: str) -> str:
    """Return a string where every instance of consecutive whitespaces internal to <s> has been replace
    by the first of those consecutive whitespace characters,
    and leading and trailing whitespace is eliminated"""
    new_s = re.sub(r"(\s)\s+", "\\1", s)
    new_s = new_s.strip()
    return new_s


def replace_raw_with_internal_ids(
    df: pd.DataFrame,
    juris: jm.Jurisdiction,
    table_df: pd.DataFrame,
    element: str,
    internal_name_column: str,
    error: dict,
    drop_unmatched: bool = False,
    drop_extraneous: bool = True,
    mode: str = "row",
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
        os.path.join(juris.path_to_juris_dir, "dictionary.txt"),
        sep="\t",
        encoding=eda.default_encoding,
    )

    # restrict to the element at hand
    raw_ids_for_element = raw_identifiers[
        raw_identifiers["cdf_element"] == element
    ].copy()

    if element == "Candidate":
        # remove any lines with nulls
        raw_ids_for_element = raw_ids_for_element[raw_ids_for_element.notnull().all(axis=1)]

        # Regularize candidate names (to match what's done during upload of candidates to Candidate
        #  table in db)
        raw_ids_for_element["cdf_internal_name"] = regularize_candidate_names(
            raw_ids_for_element["cdf_internal_name"])
        raw_ids_for_element.drop_duplicates(inplace=True)

    working = working.merge(
        raw_ids_for_element,
        how="left",
        left_on=f"{element}_raw",
        right_on="raw_identifier_value",
        suffixes=["", f"_{element}_ei"],
    )

    # identify unmatched
    unmatched = working[working["cdf_internal_name"].isnull()]
    unmatched_raw = sorted(unmatched[f"{element}_raw"].unique(), reverse=True)
    if len(unmatched_raw) > 0 and element != "BallotMeasureContest":
        unmatched_str = "\n".join(unmatched_raw)
        e = f"\n{element}s not found in dictionary.txt:\n{unmatched_str}"
        error = ui.add_new_error(error, "warn-jurisdiction", juris.short_name, e)

    if drop_unmatched:
        working = working[working["cdf_internal_name"].notnull()]

    if drop_extraneous:
        # TODO tech debt - note change of case for Candidate above which, if
        #  changed, might affect this in unexpected ways
        # drop extraneous rows identified in dictionary
        working = working[working["cdf_internal_name"] != "row should be dropped"]

    if working.empty:
        e = f"No true raw {element} in 'dictionary.txt' matched any raw {element} derived from the result file"
        if drop_unmatched and not drop_all_ok:
            error = ui.add_new_error(
                error,
                "jurisdiction",
                juris.short_name,
                e,
            )
        else:
            error = ui.add_new_error(error, "warn-jurisdiction", juris.short_name, e)
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
    else:
        if mode == "column":
            # drop rows that melted from unrecognized columns, EVEN IF drop_unmatched=False.
            #  These rows are ALWAYS extraneous. Drop cols where raw_identifier is not null
            #  but no cdf_internal_name was found (pd.merge yields nulls)
            #
            working = working[
                (working["raw_identifier_value"].isnull())
                | (working["cdf_internal_name"].notnull())
            ]
            if drop_extraneous:
                working = working[working["cdf_internal_name"] != "row should be dropped"]
            # TODO tech debt more efficient to drop these earlier, before melting

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
            f'({r[f"{element}_raw"]},{r[element]})'
            for i, r in working_unmatched[[f"{element}_raw", element]]
            .drop_duplicates()
            .iterrows()
        ]
        unmatched_str = "\n\t".join(unmatched_pairs)
        e = (
            f"Warning: Results for {working_unmatched.shape[0]} rows with unmatched {element}s "
            f"will not be loaded to database. These records (raw name, internal name) were found in dictionary.txt, but "
            f"no corresponding record was found in the {element} table in the database: \n\t{unmatched_str}"
        )
        error = ui.add_new_error(
            error,
            "warn-jurisdiction",
            juris.short_name,
            e,
        )

    if drop_unmatched:
        # if all are unmatched
        if working_unmatched.shape[0] == working.shape[0]:
            error = ui.add_new_error(
                error,
                "jurisdiction",
                juris.short_name,
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


def enum_col_from_id_othertext(
        df: pd.DataFrame,
        enum: str,
        enum_df: pd.DataFrame,
        drop_old: bool = True
) -> pd.DataFrame:
    """Returns a copy of dataframe <df>, replacing id and othertext columns
    (e.g., 'CountItemType_Id' and 'OtherCountItemType)
    with a plaintext <type> column (e.g., 'CountItemType')
        using the enumeration given in <enum_df>"""
    assert f"{enum}_Id" in df.columns, f"Dataframe lacks {enum}_Id column"
    assert f"Other{enum}" in df.columns, f"Dataframe lacks Other{enum} column"
    assert "Txt" in enum_df.columns, "Enumeration dataframe should have column 'Txt'"

    # ensure Id is in the index of enum_df (otherwise df index will be lost in merge)
    if "Id" in enum_df.columns:
        enum_df = enum_df.set_index("Id")

    df = df.merge(enum_df, left_on=f"{enum}_Id", right_index=True)

    # if Txt value is 'other', use Other{enum} value instead
    df["Txt"].mask(df["Txt"] != "other", other=df[f"Other{enum}"])
    df.rename(columns={"Txt": enum}, inplace=True)
    if drop_old:
        df.drop([f"{enum}_Id", f"Other{enum}"], axis=1, inplace=True)
    return df


def enum_col_to_id_othertext(
        df: pd.DataFrame,
        type_col: str,
        enum_df: pd.DataFrame,
        drop_type_col: bool = True,
) -> (pd.DataFrame, List[str]):
    """Returns a copy of dataframe <df>, replacing a plaintext <type_col> column (e.g., 'CountItemType') with
    the corresponding two id and othertext columns (e.g., 'CountItemType_Id' and 'OtherCountItemType
    using the enumeration given in <enum_df>. Optionally drops the original column <type_col>"""
    if df.empty:
        # add two columns
        df[f"{type_col}_Id"] = df[f"Other{type_col}"] = df.iloc[:, 0]
        non_standard = list()
    else:
        assert type_col not in ["Id", "Txt"], "type_col cannot be Id or Txt"
        # get the Id of 'other' in the enumeration
        other_id = enum_df[enum_df.Txt == 'other']['Id'].iloc[0]

        # ensure Id is a column, not the index, of enum_df (otherwise df index will be lost in merge)
        if "Id" not in enum_df.columns:
            enum_df["Id"] = enum_df.index

        for c in ["Id", "Txt"]:
            if c in df.columns:
                # avoid conflict by temporarily renaming the column in the main dataframe
                assert c * 3 not in df.colums, (
                    "Column name " + c * 3 + " conflicts with variable used in code"
                )
                df.rename(columns={c: c * 3}, inplace=True)

        # create and fill the *_Id column
        df = df.merge(
            enum_df, how="left", left_on=type_col, right_on="Txt"
        ).rename(
            columns={"Id": f"{type_col}_Id"}
        )
        df[f"{type_col}_Id"].fillna(other_id, inplace=True)

        # create and fill the Other* column (type_col if *_Id = other_id, otherwise '')
        df = add_constant_column(df, f"Other{type_col}", "")
        mask = df[f"{type_col}_Id"] == other_id
        df.loc[mask, f"Other{type_col}"] = df.loc[mask, type_col]

        # drop the Txt column from the enumeration
        df = df.drop(["Txt"], axis=1)

        # if we renamed, then undo the renaming
        for c in ["Id", "Txt"]:
            if c * 3 in df.columns:
                df.rename(columns={c * 3: c}, inplace=True)
        # create list of non-standard items found
        non_standard = list(df.loc[mask, f"Other{type_col}"].unique())
        if "" in non_standard:
            non_standard.remove("")

    if drop_type_col:
        df = df.drop([type_col], axis=1)

    return df, non_standard


def good_syntax(s):
    """Returns true if formula string <s> passes certain syntax check(s)"""
    good = True
    # check that angle brackets match
    #  split the string by opening angle bracket:
    split = s.split("<")
    lead = split[0]  # must be free of close angle brackets
    if ">" in lead:
        good = False
        return good
    else:
        p1 = re.compile(r"^\S")  # must start with non-whitespace
        p2 = re.compile(
            r"^[^>]*\S>[^>]*$"
        )  # must contain exactly one >, preceded by non-whitespace
        for x in split[1:]:
            if not (p1.search(x) and p2.search(x)):
                good = False
                return good
    return good


def regularize_candidate_names(
        candidate_column: pd.Series,
) -> pd.Series:
    ws = candidate_column.copy()

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


def melt_to_one_count_column(df: pd.DataFrame, p: dict, mu_name: str) -> (pd.DataFrame, Optional[dict]):
    """transform to df with single count column and all raw munge info in other columns"""
    err = None
    if isinstance(df.columns,pd.MultiIndex):
        multi = True
    else:
        multi = False

    if multi:
        # transform multi-index to plain index
        df.columns = [";:;".join([f"{x}" for x in tup]) for tup in df.columns]

    # define count columns
    if p["count_locations"] == "by_column_number":
        count_columns = [
            df.columns[idx] for idx in p["count_column_numbers"] if idx < df.shape[1]
        ]
    elif p["count_locations"] == "by_field_name":
        assert not multi, "If there are multiple header rows, need to have count_locations=by_column_number, "
        count_columns = p["count_fields_by_name"]
    else:
        err = ui.add_new_error(
            err,
            "munger",
            mu_name,
            f"count_locations parameter must be either by_column_number or by_field_name"
        )
        return pd.DataFrame(), err

    # melt so that there is one single count column
    id_columns = [c for c in df.columns if c not in count_columns]
    melted = df.melt(id_vars=id_columns, value_vars=count_columns, var_name="header_0", value_name="Count")
    if multi:
        # remove extraneous text from columns headers, leaving only field name row
        for idx in len(melted.columns):
            if melted.columns[idx] in id_columns:
                melted.columns[idx] = melted.columns[idx].split(";:;")[p["string_field_name_row"]]
        if "in_count_headers" in p["string_locations"]:

            # split header_0 column into separate columns
            melted[
                [f"header_{idx}" for idx in p["count_header_row_numbers"]]
            ] = pd.DataFrame(
                melted["header_0"].str.split(";:;", expand=True).values
            )
    
    return melted, err


def munge_and_melt(
    mu: jm.Munger, raw: pd.DataFrame, count_cols: List[str], err: Optional[dict]
) -> (pd.DataFrame, Optional[dict]):
    """Does not alter raw; returns transformation of raw:
     all row- and column-sourced mungeable info into columns (but doesn't translate via dictionary)
    new column names are, e.g., ReportingUnit_raw, Candidate_raw, etc.
    """
    working = raw.copy()

    # melt all column (multi-) index info into columns
    non_count_cols = [x for x in working.columns if x not in count_cols]
    working = working.melt(id_vars=non_count_cols)

    # ensure all columns have string names
    # (i.e., get rid of any tuples from column multi-index)
    new_col_index = [
        c[mu.options["field_name_row"]] if isinstance(c, tuple) else c
        for c in working.columns
    ]
    working.columns = new_col_index

    #  if only one header row, rename variable to variable_0 for consistency
    working.rename(columns={"variable": "variable_0"}, inplace=True)

    # clean and append "_SOURCE" to each original non-count column name
    working, new_err = munge_clean(working, mu, ["value"])
    if new_err:
        err = ui.consolidate_errors([err, new_err])
        if ui.fatal_error(new_err):
            return working, err

    # NB: if there is just one numerical column, melt still creates dummy variable col
    #  in which each value is 'value'

    # rename value to Count
    #  NB: any unnecessary numerical cols (e.g., Contest Group ID) will not matter
    #  as they will be be missing from dictionary.txt and hence will be ignored.
    working.rename(columns={"value": "Count"}, inplace=True)

    # apply munging formula from row sources (after renaming fields in raw formula as necessary)
    for t in mu.cdf_elements[mu.cdf_elements.source == "row"].index:
        working, new_err = add_munged_column(working, mu, t, None, mode="row")
        if new_err:
            err = ui.consolidate_errors([err, new_err])
            if ui.fatal_error(new_err):
                return working, err

    # remove original row-munge columns
    munged = [x for x in working.columns if x[-7:] == "_SOURCE"]
    working.drop(munged, axis=1, inplace=True)

    # apply munge formulas for column sources
    for t in mu.cdf_elements[mu.cdf_elements.source == "column"].index:
        working, new_err = add_munged_column(working, mu, t, None, mode="column")
        if new_err:
            err = ui.consolidate_errors([err, new_err])
            if ui.fatal_error(new_err):
                return working, err

    # remove unnecessary columns
    not_needed = [c for c in working.columns if c[:9] == "variable_"]
    working.drop(not_needed, axis=1, inplace=True)

    return working, err


def add_constant_column(df, col_name, col_value):
    new_df = df.assign(**dict.fromkeys([col_name], col_value))
    return new_df


def add_contest_id(
    df: pd.DataFrame, juris: jm.Jurisdiction, err: Optional[dict], session: Session
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
                juris,
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
            err, "jurisdiction", juris.short_name, f"No contests recognized."
        )
    else:
        working = working_temp
    if ui.fatal_error(err):
        return working, err

    return working, err


def add_selection_id(  # TODO tech debt: why does this add columns 'I' and 'd'?
    df: pd.DataFrame, engine, jurisdiction: jm.Jurisdiction, err: dict
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
            jurisdiction,
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
            db.insert_to_cdf_db(engine, c_df_unmatched, "CandidateSelection")

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
                "munge.add_selection_id",
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
        juris: jm.Jurisdiction,
        element_list: list,
        session,
) -> (pd.DataFrame, Optional[dict]):
    """ Append ids to <df> for any row- or column- sourced elements given in <element_list>"""

    err = None
    working = df.copy()
    for t in element_list:
        try:
            # capture id from db in new column and erase any now-redundant cols
            element_df = pd.read_sql_table(t, session.bind)
            name_field = db.get_name_field(t)
            # set drop_unmatched = True for fields necessary to BallotMeasure rows,
            #  drop_unmatched = False otherwise to prevent losing BallotMeasureContests for BM-inessential fields
            if t == "ReportingUnit" or t == "CountItemType":
                drop = True
            else:
                drop = False
            if t == "CountItemType":
                # munge raw to internal CountItemType
                r_i = pd.read_csv(
                    os.path.join(juris.path_to_juris_dir, "dictionary.txt"),
                    sep="\t",
                    encoding=eda.default_encoding,
                )
                r_i = r_i[r_i.cdf_element == "CountItemType"]
                recognized = r_i.raw_identifier_value.unique()
                matched = (working.CountItemType_raw.isin(recognized))
                if not matched.all():
                    unmatched = "\n".join((working[~matched]["CountItemType_raw"]).unique())
                    err = ui.add_new_error(
                        err,
                        "warn-jurisdiction",
                        juris.short_name,
                        f"Some unmatched CountItemTypes:\n{unmatched}",
                    )
                # get CountItemType for all matched lines
                working = working[matched].merge(
                    r_i,
                    how="left",
                    left_on="CountItemType_raw",
                    right_on="raw_identifier_value",
                ).rename(columns={"cdf_internal_name": "CountItemType"})

                # join CountItemType_Id and OtherCountItemType
                cit = pd.read_sql_table("CountItemType", session.bind)
                working, non_standard = enum_col_to_id_othertext(working, "CountItemType", cit)
                if non_standard:
                    ns = "\n\t".join(non_standard)
                    err = ui.add_new_error(
                        err,
                        "warn-jurisdiction",
                        juris.short_name,
                        f"Some recognized CountItemTypes are not standard:\n\t{ns}"
                    )
                working, err_df = clean_ids(working, ["CountItemType_Id"])
                working = clean_strings(working, ["OtherCountItemType"])
                working = working.drop(
                    ["raw_identifier_value", "cdf_element", "CountItemType_raw"], axis=1
                )
            else:
                none_or_unknown_id = db.name_to_id(session, t, "none or unknown")
                working, new_err = replace_raw_with_internal_ids(
                    working,
                    juris,
                    element_df,
                    t,
                    name_field,
                    err,
                    drop_unmatched=drop,
                    unmatched_id=none_or_unknown_id,
                )
                err = ui.consolidate_errors([err, new_err])
                if ui.fatal_error(new_err):
                    return working, err
                working.drop(t, axis=1, inplace=True)
        except KeyError as exc:
            err = ui.add_new_error(
                err,
                "system",
                "munge.raw_elements_to_cdf",
                f"KeyError ({exc}) while adding internal ids for {t}.",
            )
        except Exception as exc:
            err = ui.add_new_error(
                err,
                "system",
                "munge.raw_elements_to_cdf",
                f"Exception ({exc}) while adding internal ids for {t}.",
            )

    return working, err


def ensure_total_counts(
        df: pd.DataFrame,
        session,
) -> pd.DataFrame:
    """Assumes cols of df include Count and CountItemType_Id, OtherCountItemType
    If there are no records with CountItemType 'total', add records"""

    # find CountItemType_Id for 'total'
    total_idx = db.name_to_id(session, "CountItemType", "total")

    # find groups that have no total CountItemType

    # add total for each of those groups, but not for others.
    group_cols = [x for x in df.columns if x not in ["Count","CountItemType_Id","OtherCountItemType"]]

    # get all totals
    sums = df.groupby(group_cols)["Count"].sum()

    # keep only sums that already involve a total
    no_total = df.groupby(group_cols)["CountItemType_Id"].apply(list).apply(lambda x: total_idx not in x)
    sums = sums[no_total]
    sums_df = sums.reset_index()

    sums_df = add_constant_column(sums_df, "CountItemType_Id", total_idx)
    sums_df = add_constant_column(sums_df, "OtherCountItemType", "")

    working = pd.concat([df, sums_df])

    return working


def raw_elements_to_cdf(
    session,
    juris: jm.Jurisdiction,
    mu: jm.Munger,
    raw: pd.DataFrame,
    count_cols: List[str],
    err: dict,
    constants: dict,
) -> dict:
    """load data from <raw> into the database."""
    working = raw.copy()

    try:
        working, new_err = munge_and_melt(mu, working, count_cols, err)
        if new_err:
            err = ui.consolidate_errors([err, new_err])
            if ui.fatal_error(new_err):
                return err
    except Exception as exc:
        err = ui.add_new_error(
            err,
            "system",
            "munge.raw_elements_to_cdf",
            f"Unexpected exception during munge_and_melt: {exc}",
        )
        return err

    # enter elements from sources outside raw data, including creating id column(s)
    for k in constants.keys():
        working = add_constant_column(working, k, constants[k])

    # add Contest_Id (unless it was passed in ids)
    if "Contest_Id" not in working.columns:
        try:
            working, err = add_contest_id(working, juris, err, session)
        except Exception as exc:
            err = ui.add_new_error(
                err,
                "system",
                "munge.raw_elements_to_cdf",
                f"Unexpected exception while adding Contest_Id: {exc}",
            )
            return err
        if ui.fatal_error(err):
            return err

    # get ids for remaining info sourced from rows and columns (except Selection_Id)
    element_list = [
        t
        for t in mu.cdf_elements.index
        if (
            t[-7:] != "Contest"
            and (t[-9:] != "Selection")
            and f"{t}_Id" not in constants.keys()
        )
    ]
    working, new_err = raw_to_id_simple(working, juris, element_list, session)
    if new_err:
        err = ui.consolidate_errors([err, new_err])
        if ui.fatal_error(new_err):
            return err

    # add Selection_Id (combines info from BallotMeasureSelection and CandidateContestSelection)
    try:
        working, err = add_selection_id(working, session.bind, juris, err)
        working, err_df = clean_ids(working, ["Selection_Id"])
    except Exception as exc:
        err = ui.add_new_error(
            err,
            "system",
            "munge.raw_elements_to_cdf",
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
    working, e = clean_count_cols(working, ["Count"])

    working = ensure_total_counts(working, session)
    # TODO there are edge cases where this might include dupes
    #  that should be omitted. E.g., if data mistakenly read twice
    # Sum any rows that were disambiguated (otherwise dupes will be dropped
    #  when VoteCount is filled)
    group_cols = [c for c in working.columns if c != "Count"]
    working = working.groupby(group_cols).sum().reset_index()
    # TODO clean before inserting? All should be already clean, no?

    # Fill VoteCount
    try:
        e = db.insert_to_cdf_db(session.bind, working, "VoteCount")
        if e:
            err = ui.add_new_error(
                err,
                "system",
                "munge.raw_elements_to_cdf",
                f"database insertion error {e}",
            )
            return err
    except Exception as exc:
        err = ui.add_new_error(
            err,
            "system",
            "munge.raw_elements_to_cdf",
            f"Error filling VoteCount:\n{exc}",
        )
    return err


if __name__ == "__main__":
    pass
    exit()


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
        working = add_constant_column(
            working,
            "Contest_Id",
            db.name_to_id(session, "Contest", constants["CandidateContest"])
        )
        working = add_constant_column(
            working,
            "contest_type",
            "Candidate"
        )
        working.drop("CandidateContest", axis=1, inplace=True)
    elif "BallotMeasureContest" in constants.keys():
        working = add_constant_column(
            working,
            "Contest_Id",
            db.name_to_id(session, "Contest", constants["BallotMeasureContest"])
        )
        working.drop("BallotMeasureContest", axis=1, inplace=True)
        working = add_constant_column(
            working,
            "contest_type",
            "BallotMeasure"
        )
    else:
        try:
            working, err = add_contest_id(working, juris, err, session)
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
            id_txt_one_line, non_standard = enum_col_to_id_othertext(
                one_line, element, enum_df, drop_type_col=False
            )
            for c in [f"{element}_Id", f"Other{element}"]:
                working = add_constant_column(
                    working,
                    c,
                    id_txt_one_line.loc[0, c]
                )
            working.drop(element, axis=1, inplace=True)
        else:
            working = add_constant_column(
                working,
                f"{element}_Id",
                db.name_to_id(session, element, constants[element])
            )
            working.drop(element, axis=1, inplace=True)
            working, err_df = clean_ids(working, ["CountItemType_Id"])
            if not err_df.empty:
                err = ui.add_new_error(
                    err,
                    "warn-munger",
                    munger_name,
                    f"Problem cleaning these Ids:\n{err_df}"
                )

    other_elements = [
        t for t in all_munge_elements
        if (t[-7:] != "Contest") and (t[-9:] != "Selection") and (t not in constants.keys())
    ]
    working, new_err = raw_to_id_simple(working, juris, other_elements, session)
    if new_err:
        err = ui.consolidate_errors([err, new_err])
        if ui.fatal_error(new_err):
            return working, err

    # add Selection_Id (combines info from BallotMeasureSelection and CandidateContestSelection)
    try:
        working, err = add_selection_id(working, session.bind, juris, err)
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
            juris.short_name,
            "No contests found, or no selections found for contests.",
        )
        return working, err

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
        elements = [k for k in formulas.keys() if (formulas[k] is not None) and (formulas[k] != "None")]
        for element in elements:
            try:
                formula = formulas[element]
                # append suffix to formula fields
                for c in orig_string_cols:
                    formula = formula.replace(f"<{c}>", f"<{c}{suffix}>")
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
                    f"Error interpreting formula for {element} in cdf_element.txt. {exc}",
                )
                return working, err
            # compress whitespace for <element>_raw
            working.loc[:, f"{element}_raw"] = working[f"{element}_raw"].apply(
                compress_whitespace
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
    # TODO check formats (e.g., formulas for constant_over_sheet use only <sheet_name> and <row_{i}>)
    # TODO check that required headers are present (see User_Guide) per string_locations list
    # TODO check that required headers are present (see User_Guide) per lookups list

    # # add parameter listing all munge fields
    # get lists of string fields expected in raw file
    # TODO why can't munge_fields and string_fields be the same?
    params["munge_fields"], new_err = get_string_fields(
        [x for x in params["string_locations"] if x != "constant_over_file"],
        munger_path,
    )
    if new_err:
        ui.consolidate_errors([err, new_err])
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
    raw_dict, err = ui.read_single_datafile(f_path, p, munger_name, err)
    if ui.fatal_error(err):
        return pd.DataFrame(), err

    # get lists of string fields expected in raw file
    try:
        munge_field_lists, new_err = get_string_fields(
            [x for x in p["string_locations"] if x != "constant_over_file"],
            munger_path,
        )
    except Exception as exc:
        new_err = ui.add_new_error(
            None,
            "system",
            f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
            f"Exception while getting string fields: {exc}"
        )
    if new_err:
        err = ui.consolidate_errors([err, new_err])
        if ui.fatal_error(new_err):
            return pd.DataFrame(), err
    # remove any sheets designated to be removed
    if p["sheets_to_skip"]:
        for k in p["sheets_to_skip"]:
            if k in raw_dict.keys():
                raw_dict.pop(k)
    # for each df:
    standard = dict()
    for k, raw in raw_dict.items():
        # transform to df with single count column 'Count' and all raw munge info in other columns
        try:
            standard[k], k_err = melt_to_one_count_column(raw, p, munger_name)
        except Exception as exc:
            k_err = ui.add_new_error(None, "file", f_path, f"Unable to pivot dataframe {k}")
        if k_err:
            err = ui.consolidate_errors([err, k_err])
            if ui.fatal_error(k_err):
                return err

        # add columns for any constant-over-sheet elements
        if "constant_over_sheet" in p["string_locations"]:
            # see if <sheet_name> is needed
            if "sheet_name" in munge_field_lists["constant_over_sheet"]:
                standard[k] = add_constant_column(standard[k], "sheet_name", k)
            # find max row needed
            try:
                rows_needed = [int(var[4:]) for var in munge_field_lists["constant_over_sheet"] if var != "sheet_name"]
                if rows_needed:
                    max_row = max(rows_needed)
                    data = pd.read_excel(f_path, nrows=max_row+1)
                    for row in rows_needed:
                        standard[k] = add_constant_column(
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
        try:
            necessary = [item for sublist in munge_field_lists.values() for item in sublist] + ["Count"]
            standard[k] = standard[k][necessary]
        except KeyError as ke:
            err = ui.add_new_error(
                err,
                "munger",
                munger_name,
                f"Field in munge formulas not found in file column headers read from file: {ke}",
            )
            return pd.DataFrame(), err

        # clean Count column
        standard[k], bad_rows = clean_count_cols(standard[k], ["Count"], p["thousands_separator"])
        if not bad_rows.empty:
            err = ui.add_err_df(err, bad_rows, munger_name, f_path)

    df = pd.concat(standard.values())

    # clean non-count columns
    non_count = [c for c in df.columns if c != "Count"]
    df = clean_strings(df, non_count)

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
    working = ensure_total_counts(working, session)
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