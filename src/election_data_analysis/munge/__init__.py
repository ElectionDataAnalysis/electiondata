from election_data_analysis import database as db
from election_data_analysis import user_interface as ui
from election_data_analysis import juris_and_munger as jm
import pandas as pd
from pandas.api.types import is_numeric_dtype
import re
import os
import numpy as np
from sqlalchemy.orm.session import Session
import time


class MungeError(Exception):
    pass


def generic_clean(df: pd.DataFrame) -> pd.DataFrame:
    """Replaces nulls, strips whitespace."""
    # TODO put all info about data cleaning into README.md (e.g., whitespace strip)
    # TODO return error if cleaning fails, including dtypes of columns
    working = df.copy()
    for c in working.columns:
        if is_numeric_dtype(working[c]):
            # change nulls to 0
            working[c] = working[c].fillna(0).astype("int64")
        elif working.dtypes[c] == np.object:
            # change nulls to the empty string
            working[c] = working[c].fillna("")
            # replace any double quotes with single quotes
            try:
                mask = working[c].str.contains('"')
                working.loc[mask, c] = working[c].str.replace('"', "'")
            except AttributeError or TypeError:
                pass
            try:
                # strip extraneous whitespace
                working[c] = working[c].apply(lambda x: x.strip())
            except AttributeError:
                pass
    return working


def cast_cols_as_int(
    df: pd.DataFrame, col_list: list, mode="name", error_msg=""
) -> pd.DataFrame:
    """recast columns as integer where possible, leaving columns with text entries as non-numeric)"""
    if mode == "index":
        num_columns = [df.columns[idx] for idx in col_list]
    elif mode == "name":
        num_columns = [c for c in df.columns if c in col_list]
    else:
        raise ValueError(f"Mode {mode} not recognized")
    for c in num_columns:
        try:
            df[c] = df[c].astype("int64", errors="raise")
        except ValueError as e:
            print(f"{error_msg}\nColumn {c} cannot be cast as integer:\n{e}")
    return df


def munge_clean(raw: pd.DataFrame, munger: jm.Munger):
    """Drop unnecessary columns.
    Append '_SOURCE' suffix to raw column names to avoid conflicts"""
    working = raw.copy()
    # drop columns that are neither count columns nor used in munger formulas
    #  define columns named in munger formulas
    if munger.options["header_row_count"] > 1:
        munger_formula_columns = [
            x for x in working.columns if x[munger.options["field_name_row"]] in munger.field_list
        ]
    else:
        munger_formula_columns = [x for x in working.columns if x in munger.field_list]

    if munger.options["field_name_row"] is None:
        count_columns_by_name = [
            munger.options["field_names_if_no_field_name_row"][idx] for idx in munger.options["count_columns"]
        ]
    else:
        count_columns_by_name = [working.columns[idx] for idx in munger.options["count_columns"]]
    # TODO error check- what if cols_to_munge is missing something from munger.field_list?

    # keep columns named in munger formulas; keep count columns; drop all else.
    working = working[munger_formula_columns + count_columns_by_name]

    # add suffix '_SOURCE' to certain columns to avoid any conflict with db table names
    # (since no db table name ends with _SOURCE)
    renamer = {x: f"{x}_SOURCE" for x in munger_formula_columns}
    working.rename(columns=renamer, inplace=True)
    return working


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
    working: pd.DataFrame, formula: str, new_col: str, err: dict, suffix=None
) -> (pd.DataFrame, dict):
    """If <suffix> is given, add it to each field in the formula
    If formula is enclosed in braces, parse first entry as formula, second as a
    regex (with one parenthesized group) as a recipe for pulling the value via regex analysis
    """

    # set regex_flag (True if regex analysis is needed beyond concatenation formula)
    if formula[0] == "{" and formula[-1] == "}":
        regex_flag = True
        concat_formula, pattern = formula[1:-1].split(",")
    else:
        regex_flag = False
        pattern = final = None
        concat_formula = formula

    text_field_list, last_text = text_fragments_and_fields(concat_formula)

    # add suffix, if required
    if suffix:
        text_field_list = [(t, f"{f}{suffix}") for (t, f) in text_field_list]

    # add column to <working> dataframe via the concatenation formula
    if last_text:
        working.loc[:, new_col] = last_text[0]
    else:
        working.loc[:, new_col] = ""
    text_field_list.reverse()
    for t, f in text_field_list:
        try:
            working.loc[:, new_col] = (
                working.loc[:, f].apply(lambda x: f"{t}{x}") + working.loc[:, new_col]
            )
        except KeyError:
            ui.add_error(err, "munge-error", f"missing column {f}")

    # use regex to pull info out of the concatenation formula (e.g., 'DEM' from 'DEM - US Senate')
    if regex_flag:
        # TODO figure out how to allow more general manipulations. This can only pull out one part of the pattern
        working[new_col] = working[new_col].str.replace(pattern,'\\1')

    return working, err


def add_munged_column(
    raw: pd.DataFrame,
    munger: jm.Munger,
    element: str,
    err: dict,
    mode: str = "row",
    inplace: bool = True,
) -> (pd.DataFrame, dict):
    """Alters dataframe <raw>, adding or redefining <element>_raw column
    via the <formula>. Assumes "_SOURCE" has been appended to all columns of raw
    Does not alter row count."""
    if not err:
        err = {}
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
            for i in range(munger.header_row_count):
                formula = formula.replace(f"<{i}>", f"<variable_{i}>")

        working, err = add_column_from_formula(working, formula, f"{element}_raw", err)

    except:
        e = f"Error munging {element}. Check raw_identifier_formula for {element} in cdf_elements.txt"
        if "cdf_elements.txt" in err.keys():
            err["cdf_elements.txt"].append(e)
        else:
            err["cdf_elements.txt"] = [e]

    # compress whitespace for <element>_raw
    working.loc[:, f"{element}_raw"] = working[f"{element}_raw"].apply(
        compress_whitespace
    )
    return working, err


def compress_whitespace(s: str) -> str:
    """Return a string where every instance of consecutive whitespaces in <s> has been replace by a single space,
    and leading and trailing whitespace is eliminated"""
    new_s = re.sub(r"\s+", " ", s)
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
    mode: str = "row",
    unmatched_id: int = 0,
    drop_all_ok: bool = False,
) -> (pd.DataFrame, dict):
    """replace columns in <working> with raw_identifier values by columns with internal names and Ids
    from <table_df>, which has structure of a db table for <element>.
    <unmatched_id> is the id to assign to unmatched records.
    """
    working = df.copy()
    # join the 'cdf_internal_name' from the raw_identifier table -- this is the internal name field value,
    # no matter what the name field name is in the internal element table (e.g. 'Name', 'BallotName' or 'Selection')
    # use dictionary.txt from jurisdiction

    raw_identifiers = pd.read_csv(
        os.path.join(juris.path_to_juris_dir, "dictionary.txt"), sep="\t"
    )
    raw_ids_for_element = raw_identifiers[raw_identifiers["cdf_element"] == element]

    working = working.merge(
        raw_ids_for_element,
        how="left",
        left_on=f"{element}_raw",
        right_on="raw_identifier_value",
        suffixes=["", f"_{element}_ei"],
    )

    unmatched = working[working["cdf_internal_name"].isnull()]
    unmatched_raw = list(unmatched[f"{element}_raw"].unique())
    if len(unmatched_raw) > 0 and element != "BallotMeasureContest":
        unmatched_str = "\n".join(unmatched_raw)
        e = f"{element}s not found in dictionary.txt:\n{unmatched_str}"
        ui.add_error(error, "munge_warning", e)

    if drop_unmatched:
        working = working[working["cdf_internal_name"].notnull()]

    if working.empty:
        e = f"No raw {element} in 'dictionary.txt' matched any raw {element} derived from the result file"
        if drop_unmatched and not drop_all_ok:
            ui.add_error(error, "munge_error", e)
        else:
            ui.add_error(error, "munge_warning", e)
        return working, error

    if mode == "column":
        # drop rows that melted from unrecognized columns, EVEN IF drop_unmatched=False.
        #  These rows are ALWAYS extraneous. Drop cols where raw_identifier is not null
        #  but no cdf_internal_name was found (pd.merge yields nulls)
        #
        working = working[
            (working["raw_identifier_value"].isnull())
            | (working["cdf_internal_name"].notnull())
        ]
        # TODO more efficient to drop these earlier, before melting

    # unmatched elements get nan in fields from dictionary table. Change these to "none or unknown"
    if not drop_unmatched:
        working["cdf_internal_name"] = working["cdf_internal_name"].fillna(
            "none or unknown"
        )
        #

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
            f"no corresponding record was found in {element}.txt: \n{unmatched_str}"
        )
        ui.add_error(error, "munge_warning", e)

    if drop_unmatched:
        if working_unmatched.shape[0] == working.shape[0]:
            e = (
                f"No {element} was matched. Either raw values are not in dictionary.txt, or "
                f"the corresponding cdf_internal_names are missing from {element}.txt"
            )
            ui.add_error(error, "munge_error", e)
            return working.drop(working.index), error

    else:
        # change name of unmatched to 'none or unknown' and assign <unmatched_id> as Id
        working.loc[working.Id.isnull(), internal_name_column] = "none or unknown"
        working["Id"].fillna(unmatched_id, inplace=True)

    working = working.drop([internal_name_column, f"{element}_raw"], axis=1)
    working.rename(columns={"Id": f"{element}_Id"}, inplace=True)
    return working, error


def enum_col_from_id_othertext(df, enum, enum_df, drop_old=True):
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


def enum_col_to_id_othertext(df, type_col, enum_df, drop_old=True):
    """Returns a copy of dataframe <df>, replacing a plaintext <type_col> column (e.g., 'CountItemType') with
    the corresponding two id and othertext columns (e.g., 'CountItemType_Id' and 'OtherCountItemType
    using the enumeration given in <enum_df>"""
    if df.empty:
        # add two columns
        df[f"{type_col}_Id"] = df[f"Other{type_col}"] = df.iloc[:, 0]
    else:
        # ensure Id is a column, not the index, of enum_df (otherwise df index will be lost in merge)
        assert type_col not in ["Id", "Txt"], "type_col cannot be Id or Txt"
        if "Id" not in enum_df.columns:
            enum_df["Id"] = enum_df.index

        for c in ["Id", "Txt"]:
            if c in df.columns:
                # avoid conflict by temporarily renaming the column in the main dataframe
                assert c * 3 not in df.colums, (
                    "Column name " + c * 3 + " conflicts with variable used in code"
                )
                df.rename(columns={c: c * 3}, inplace=True)
        df = df.merge(enum_df, how="left", left_on=type_col, right_on="Txt")
        df.rename(columns={"Id": f"{type_col}_Id"}, inplace=True)
        add_constant_column(df, f"Other{type_col}", "")

        other_id_df = enum_df[enum_df["Txt"] == "other"]
        if not other_id_df.empty:
            other_id = other_id_df.iloc[0]["Id"]
            df[f"{type_col}_Id"] = df[f"{type_col}_Id"].fillna(other_id)
            df.loc[df[f"{type_col}_Id"] == other_id, "Other" + type_col] = df.loc[
                df[f"{type_col}_Id"] == other_id, type_col
            ]
        df = df.drop(["Txt"], axis=1)
        for c in ["Id", "Txt"]:
            if c * 3 in df.columns:
                # avoid restore name renaming the column in the main dataframe
                df.rename(columns={c * 3: c}, inplace=True)
    if drop_old:
        df = df.drop([type_col], axis=1)
    return df


def good_syntax(s):
    """Returns true if formula string <s> passes certain syntax main_routines(s)"""
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


def munge_and_melt(mu: jm.Munger, raw: pd.DataFrame, count_cols: list, err: dict) -> (pd.DataFrame, dict):
    """Does not alter raw; returns Transformation of raw:
     all row- and column-sourced mungeable info into columns (but doesn't translate via dictionary)
    new column names are, e.g., ReportingUnit_raw, Candidate_raw, etc.
    """
    working = raw.copy()

    # apply munging formula from row sources (after renaming fields in raw formula as necessary)
    for t in mu.cdf_elements[mu.cdf_elements.source == "row"].index:
        working, err = add_munged_column(working, mu, t, err, mode="row")

    # remove original row-munge columns
    munged = [x for x in working.columns if x[-7:] == "_SOURCE"]
    working.drop(munged, axis=1, inplace=True)

    # if there is just one numerical column, melt still creates dummy variable col
    #  in which each value is 'value'
    # TODO how to ensure such files munge correctly?

    # reshape
    non_count_cols = [x for x in working.columns if x not in count_cols]
    working = working.melt(id_vars=non_count_cols)
    # rename count to Count
    #  if only one header row, rename variable to variable_0 for consistency
    #  NB: any unnecessary numerical cols (e.g., Contest Group ID) will not matter
    #  as they will be be missing from raw_identifiers.txt and hence will be ignored.
    # TODO check and correct: no num col names conflict with raw identifiers of column-source
    #  elements!
    working.rename(columns={"variable": "variable_0", "value": "Count"}, inplace=True)

    # apply munge formulas for column sources
    for t in mu.cdf_elements[mu.cdf_elements.source == "column"].index:
        working, err = add_munged_column(working, mu, t, err, mode="column")

    # remove unnecessary columns
    not_needed = [f"variable_{i}" for i in range(mu.options["header_row_count"])]
    working.drop(not_needed, axis=1, inplace=True)

    return working, err


def add_constant_column(df, col_name, col_value):
    new_df = df.assign(**dict.fromkeys([col_name], col_value))
    return new_df


def add_contest_id(
    df: pd.DataFrame, juris: jm.Jurisdiction, err: dict, session: Session
) -> (pd.DataFrame, dict):
    working = df.copy()
    """Append Contest_Id and contest_type. Add contest_type column and fill it correctly.
    Drop rows which match neither BM nor C contest"""

    # add Contest_Id and contest_type
    df_for_type = dict()
    w_for_type = dict()
    df_contest = pd.read_sql_table(f"Contest", session.bind)
    for c_type in ["BallotMeasure", "Candidate"]:
        # restrict df_contest to the contest_type <c_type> and get the <c_type>Contest_Id
        df_for_type[c_type] = df_contest[df_contest.contest_type == c_type]
        none_or_unknown_id = db.name_to_id(
            session, f"{c_type}Contest", "none or unknown"
        )
        working, err = replace_raw_with_internal_ids(
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
        # restrict working to the contest_type <c_type>, add contest_type column
        w_for_type[c_type] = working[working[f"{c_type}Contest"] != "none or unknown"]
        w_for_type[c_type] = add_constant_column(
            w_for_type[c_type], "contest_type", c_type
        ).rename(columns={f"{c_type}Contest_Id": "Contest_Id"})

        # drop text column
        w_for_type[c_type] = w_for_type[c_type].drop(f"{c_type}Contest", axis=1)

    # FIXME: check somewhere that no name (other than 'none or unknown') is shared by BMContests and CandidateContests
    # TODO check this also when juris files loaded, to save time for user

    working = pd.concat([w_for_type[ct] for ct in ["BallotMeasure", "Candidate"]])
    missing_idx = [idx for idx in df.index if idx not in working.index]
    missing = df.loc[missing_idx]
    # fail if no contests recognized
    if working.empty:

        e = "No contests in database matched. No results will be loaded to database."
        ui.add_error(err, "munge_error", e)
        return working, err

    # drop obsolete columns
    common_cols = [
        c
        for c in w_for_type["BallotMeasure"].columns
        if c in w_for_type["Candidate"].columns
    ]
    for c_type in ["BallotMeasure", "Candidate"]:
        w_for_type[c_type] = w_for_type[c_type][common_cols]

    return working, err


def add_selection_id(
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
        c_df = c_df[(c_df.Candidate_Id.notnull()) & (c_df.Candidate_Id != 0)]

        # pull any existing Ids into a new CandidateSelection_Id column
        col_map = {c: c for c in ["Party_Id", "Candidate_Id"]}
        c_df = db.append_id_to_dframe(
            engine, c_df, "CandidateSelection", col_map=col_map
        )

        # find unmatched records
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
        # recast Candidate_Id and Party_Id to int in w['Candidate']; Note that neither should have nulls, but rather the 'none or unknown' Id
        #  NB: c_df had this recasting done in the append_id_to_dframe routine
        w["Candidate"] = generic_clean(w["Candidate"])

        # append CandidateSelection_Id to w['Candidate']
        w["Candidate"] = w["Candidate"].merge(
            c_df, how="left", on=["Candidate_Id", "Party_Id"]
        )

        # rename to Selection_Id and drop extraneous
        w["Candidate"] = (
            w["Candidate"]
            .rename(columns={"CandidateSelection_Id": "Selection_Id"})
            .drop(["Candidate_Id", "BallotMeasureSelection_raw"], axis=1)
        )

    working = pd.concat([w["BallotMeasure"], w["Candidate"]])

    return working, err


def raw_elements_to_cdf(
    session,
    project_root: str,
    juris: jm.Jurisdiction,
    mu: jm.Munger,
    raw: pd.DataFrame,
    count_cols: list,
    err: dict,
    ids=None,
) -> dict:
    """load data from <raw> into the database."""
    working = raw.copy()

    # enter elements from sources outside raw data, including creating id column(s)
    working = add_constant_column(working, "Election_Id", ids[1])
    working = add_constant_column(working, "_datafile_Id", ids[0])

    try:
        working, err = munge_and_melt(mu, working, count_cols, err)
    except Exception as exc:
        e = f"Error during munge-and-melt: {exc}"
        ui.add_error(err, "munge_error", e)
        return err
    try:
        working, err = add_contest_id(working, juris, err, session)
    except Exception as exc:
        e = f"Error while adding Contest_Id: {exc}"
        ui.add_error(err, "munge_error", e)
        return err
    if working.empty:
        e = f"No contest ids could be found. "
        ui.add_error(err, "munge_error", e)
        return err

    # get ids for remaining info sourced from rows and columns
    element_list = [
        t
        for t in mu.cdf_elements.index
        if (t[-7:] != "Contest" and t[-9:] != "Selection")
    ]
    for t in element_list:
        try:
            # capture id from db in new column and erase any now-redundant cols
            df = pd.read_sql_table(t, session.bind)
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
                    os.path.join(juris.path_to_juris_dir, "dictionary.txt"), sep="\t"
                )
                r_i = r_i[r_i.cdf_element == "CountItemType"]
                working = working.merge(
                    r_i,
                    how="left",
                    left_on="CountItemType_raw",
                    right_on="raw_identifier_value",
                ).rename(columns={"cdf_internal_name": "CountItemType"})

                # join CountItemType_Id and OtherCountItemType
                cit = pd.read_sql_table("CountItemType", session.bind)
                working = enum_col_to_id_othertext(working, "CountItemType", cit)
                working = working.drop(
                    ["raw_identifier_value", "cdf_element", "CountItemType_raw"], axis=1
                )
            else:
                none_or_unknown_id = db.name_to_id(session, t, "none or unknown")
                working, err = replace_raw_with_internal_ids(
                    working,
                    juris,
                    df,
                    t,
                    name_field,
                    err,
                    drop_unmatched=drop,
                    unmatched_id=none_or_unknown_id,
                )
                working.drop(t, axis=1, inplace=True)
        except Exception as exc:
            ui.add_error(err, "munge-error", f"Error adding internal ids for {t}.")
            return err

    # add Selection_Id (combines info from BallotMeasureSelection and CandidateContestSelection)
    try:
        working, err = add_selection_id(working, session.bind, juris, err)
    except Exception as exc:
        e = f"Error adding Selection_Id:\n{exc}"
        err = ui.add_error(err, "munge_error", e)
        return err
    if working.empty:
        e = "No contests found, or no selections found for contests."
        err = ui.add_error(err, "datafile_error", e)
        return err

    # Fill VoteCount
    vc_start = time.perf_counter()
    try:
        e = db.insert_to_cdf_db(session.bind, working, "VoteCount")
        if e:
            ui.add_error(err, "database", e)
        session.commit()
    except Exception as exc:
        e = f"Error filling VoteCount:\n{exc}"
        err = ui.add_error(err, "munge_error", e)
    vc_time = time.perf_counter() - vc_start
    print(f"VoteCount load time in seconds: {vc_time}")

    return err


if __name__ == "__main__":
    pass
    exit()
