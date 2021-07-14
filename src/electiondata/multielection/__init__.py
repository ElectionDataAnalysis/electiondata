from electiondata import (
    juris,
    munge,
    userinterface as ui,
    database as db,
    constants,
)
import pandas as pd
import os
import re
from typing import Optional, List, Dict, Any


def add_elections_to_db(session, election_file: str) -> Optional[Dict[str, Any]]:
    err = None
    try:
        e_df = pd.read_csv(election_file, sep="\t")
        err = db.insert_to_cdf_db(
            session.bind, e_df, "Election", "database", session.bind.url.database
        )

    except Exception as exc:
        err = ui.add_new_error(
            err, "file", election_file, f"Error adding elections to database {session.bind.url.database}: {exc}"
        )
    return err


def add_candidates(
    juris_sys_name: str,
    repo_content_root: str,
    candidate_list: List[str],
    normal: Dict[str, str],
) -> Optional[Dict[str, Any]]:
    err = None
    juris_path = os.path.join(repo_content_root, "jurisdictions", juris_sys_name)
    try:
        old_df = juris.get_element(juris_path, "Candidate")
        old_dictionary = juris.get_element(juris_path, "dictionary")
        new_df = pd.DataFrame(
            [[normal[x], x] for x in candidate_list], columns=["BallotName", "raw"]
        )
        new_dict = new_df.rename(
            columns={"BallotName": "cdf_internal_name", "raw": "raw_identifier_value"}
        )
        new_dict["cdf_element"] = "Candidate"
        juris.write_element(
            juris_path, "Candidate", pd.concat([old_df, new_df[["BallotName"]]])
        )
        juris.write_element(
            juris_path, "dictionary", pd.concat([old_dictionary, new_dict])
        )
    except Exception as exc:
        err = ui.add_new_error(
            err, "jurisdiction", juris_sys_name, f"Error adding candidates: {exc}"
        )
    return err


def add_dictionary_entries(juris_sys_name, repo_content_root, element, p_map):
    err = None
    juris_path = os.path.join(repo_content_root, "jurisdictions", juris_sys_name)
    try:
        old_dictionary = juris.get_element(juris_path, "dictionary")
        new_dict = pd.DataFrame(
            [[element, raw, internal] for internal, raw in p_map.items()],
            columns=["cdf_element", "cdf_internal_name", "raw_identifier_value"],
        )
        juris.write_element(
            juris_path,
            "dictionary",
            pd.concat([old_dictionary, new_dict]).sort_values(
                by=["cdf_element", "cdf_internal_name"]
            ),
        )
    except Exception as exc:
        err = ui.add_new_error(
            err, "jurisdiction", juris_sys_name, f"Error adding {element}: {exc}"
        )
    return err


def replacement(match):
    return f"Mc{match.group(1).upper()}"


def correct(proper_name: str) -> str:
    p = r"Mc([a-z])"
    new = re.sub(p, replacement, proper_name)
    if new in constants.mit_correction.keys():
        new = constants.mit_correction[new]
    return new
