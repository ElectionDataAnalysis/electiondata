# Routines to aid in preparing Jurisdiction and Munger files
import pandas as pd
import os
from election_data_analysis import user_interface as ui
import election_data_analysis as eda


def primary(row: pd.Series, party: str, contest_field: str) -> str:
    try:
        pr = f"{row[contest_field]} ({party})"
    except KeyError:
        pr = None
    return pr


def get_element(juris_path: str, element: str) -> pd.DataFrame:
    """<juris> is path to jurisdiction directory. Info taken
    from <element>.txt file in that directory. If file doesn't exist,
    empty dataframe returned"""
    f_path = os.path.join(juris_path, f"{element}.txt")
    if os.path.isfile(f_path):
        element_df = pd.read_csv(
            f_path,
            sep="\t",
            dtype="object",
            encoding=eda.default_encoding,
        )
    else:
        element_df = pd.DataFrame()
    return element_df


def remove_empty_lines(df: pd.DataFrame, element: str) -> pd.DataFrame:
    """return copy of <df> with any contentless lines removed.
    For dictionary element, such lines may have a first entry (e.g., CandidateContest)"""
    working = df.copy()
    # remove all rows with nothing
    working = working[((working != "") & (working != '""')).any(axis=1)]

    if element == "dictionary":
        working = working[(working.iloc[:, 1:] != "").any(axis=1)]
    return working


def write_element(
    juris_path: str, element: str, df: pd.DataFrame, file_name=None
) -> dict:
    """<juris> is path to jurisdiction directory. Info taken
    from <element>.txt file in that directory.
    <element>.txt is overwritten with info in <df>"""
    err = None
    # set name of target file
    if not file_name:
        file_name = f"{element}.txt"
    # dedupe the input df
    dupes_df, deduped = ui.find_dupes(df)

    if element == "dictionary":
        # remove empty lines
        deduped = remove_empty_lines(deduped, element)
    try:
        # write info to file (note: this overwrites existing info in file!)
        deduped.drop_duplicates().fillna("").to_csv(
            os.path.join(juris_path, file_name),
            index=False,
            sep="\t",
            encoding=eda.default_encoding,
        )
    except Exception as e:
        err = ui.add_new_error(
            err,
            "system",
            "preparation.write_element",
            f"Unexpected exception writing to file: {e}",
        )
    return err


def add_defaults(juris_path: str, juris_template_dir: str, element: str) -> dict:
    old = get_element(juris_path, element)
    new = get_element(juris_template_dir, element)
    err = write_element(juris_path, element, pd.concat([old, new]).drop_duplicates())
    return err
