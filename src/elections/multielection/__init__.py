
from elections import \
    externaldata as exd, \
    juris, \
    munge, \
    userinterface as ui, \
    database as db
import pandas as pd
import os, re
from pathlib import Path
from typing import Optional, List, Dict, Any

# constants
mit_datafile_info = {
    "download_date": "2021-06-20",
    "note":"Presidential only, total only, results by county",
    "source": 'MIT Election Data and Science Lab, 2018, "County Presidential Election Returns 2000-2020",' 
    'https://doi.org/10.7910/DVN/VOQCHQ, Harvard Dataverse, V8, UNF:6:20+0NUTez42tTN5eqIKd5g== [fileUNF]',

}


mit_cols = {
    "Jurisdiction": "state",
    "Election": "year",
    "Count": "candidatevotes",
    "ReportingUnit_raw": "county_fips",
    "CandidateContest_raw": "office",
    "Party_raw": "party",
    "Candidate_raw": "candidate",
    "CountItemType_raw": "mode"
}

mit_party = {
    "DEMOCRAT": "Democratic Party",
    "OTHER": "none or unknown",
    "REPUBLICAN": "Republican Party",
    "LIBERTARIAN": "Libertarian Party",
    "GREEN": "Green Party",
}

# map file names to internal db names
mit_elections = {"2000": "2000 General",
                 "2004": "2004 General",
                 "2008": "2008 General",
                "2012": "2012 General",
                "2016": "2016 General",
}  # 2020 is there but we won't load it.

mit_election_types = {
    "2000": "general",
    "2004": "general",
    "2008": "general",
                "2012": "general",
                "2016": "general",
}

mit_cit = {"TOTAL": "total"}


mit_candidates = ['MITT ROMNEY', 'OTHER', 'DONALD J TRUMP', 'DONALD TRUMP',
       'HILLARY CLINTON', 'PRINCESS KHADIJAH M JACOB-FAMBRO',
       'JOHN KERRY', 'JOHN MCCAIN', 'BARACK OBAMA', 'JOSEPH R BIDEN JR',
       'RALPH NADER', 'JO JORGENSEN', 'WRITEIN', 'AL GORE',
       'ROQUE "ROCKY" DE LA FUENTE', 'GEORGE W. BUSH',
       'BRIAN CARROLL', 'JEROME M SEGAL',
       'BROCK PIERCE', 'DON BLANKENSHIP']


correct_d ={
    "District Of Columbia": "District of Columbia",
    "Donald Trump": "Donald J. Trump",
    "Joseph R Biden Jr": "Joseph R. Biden",
    "John Mccain": "John McCain",
    """Roque "Rocky" De La Fuenta""": "Roque 'Rocky' de la Fuenta"
}


abbr = {
    "Alabama": "AL",
    "Alaska": "AK",
    "Arizona": "AZ",
    "Arkansas": "AR",
    "California": "CA",
    "Colorado": "CO",
    "Connecticut": "CT",
    "Delaware": "DE",
    "Florida": "FL",
    "Georgia": "GA",
    "Hawaii": "HI",
    "Idaho": "ID",
    "Illinois": "IL",
    "Indiana": "IN",
    "Iowa": "IA",
    "Kansas": "KS",
    "Kentucky": "KY",
    "Louisiana": "LA",
    "Maine": "ME",
    "Maryland": "MD",
    "Massachusetts": "MA",
    "Michigan": "MI",
    "Minnesota": "MN",
    "Mississippi": "MS",
    "Missouri": "MO",
    "Montana": "MT",
    "Nebraska": "NE",
    "Nevada": "NV",
    "New Hampshire": "NH",
    "New Jersey": "NJ",
    "New Mexico": "NM",
    "New York": "NY",
    "North Carolina": "NC",
    "North Dakota": "ND",
    "Ohio": "OH",
    "Oklahoma": "OK",
    "Oregon": "OR",
    "Pennsylvania": "PA",
    "Rhode Island": "RI",
    "South Carolina": "SC",
    "South Dakota": "SD",
    "Tennessee": "TN",
    "Texas": "TX",
    "Utah": "UT",
    "Vermont": "VT",
    "Virginia": "VA",
    "Washington": "WA",
    "West Virginia": "WV",
    "Wisconsin": "WI",
    "Wyoming": "WY",
    "American Samoa": "AS",
    "District of Columbia": "DC",
    "District Of Columbia": "DC",
    "Guam": "GU",
    "Marshall Islands": "MH",
    "Northern Mariana Island": "MP",
    "Puerto Rico": "PR",
    "Virgin Islands": "VI",
}


def add_elections_to_db(session) -> Optional[Dict[str, Any]]:
    err = None
    try:
        et = pd.read_sql_table("ElectionType", session.bind, index_col=None)
        e_df, _ = munge.enum_col_to_id_othertext(
            pd.DataFrame(
                [[mit_elections[y],mit_election_types[y]] for y in mit_elections.keys()],
                columns=["Name", "ElectionType"],
            ), "ElectionType", et)
        err = db.insert_to_cdf_db(session.bind, e_df, "Election","database", session.bind.url.database)
    except Exception as exc:
        err = ui.add_new_error(err, "database",session.bind.url.database,f"Error adding elections: {exc}")
    return err


def add_candidates(
        juris_sys_name: str,
        repo_content_root: str,
        candidate_list: List[str],
        normal: Dict[str, str],
) -> Optional[Dict[str, Any]]:
    err = None
    juris_path = os.path.join(repo_content_root, "jurisdictions",juris_sys_name)
    try:
        old_df = juris.get_element(juris_path, "Candidate")
        old_dictionary = juris.get_element(juris_path, "dictionary")
        new_df = pd.DataFrame([[normal[x],x] for x in candidate_list], columns= ["BallotName", "raw"])
        new_dict = new_df.rename(columns={"BallotName": "cdf_internal_name", "raw": "raw_identifier_value"})
        new_dict["cdf_element"] = "Candidate"
        juris.write_element(
            juris_path, "Candidate", pd.concat([old_df, new_df[["BallotName"]]])
        )
        juris.write_element(
            juris_path, "dictionary", pd.concat([old_dictionary, new_dict])
        )
    except Exception as exc:
        err = ui.add_new_error(err, "jurisdiction",juris_sys_name,f"Error adding candidates: {exc}")
    return err


def add_dictionary_entries(juris_sys_name,repo_content_root,element,p_map):
    err = None
    juris_path = os.path.join(repo_content_root, "jurisdictions",juris_sys_name)
    try:
        old_dictionary = juris.get_element(juris_path, "dictionary")
        new_dict = pd.DataFrame(
            [[element, raw, internal] for internal, raw in p_map.items()],
            columns=["cdf_element", "cdf_internal_name", "raw_identifier_value"]
        )
        juris.write_element(
            juris_path, "dictionary", pd.concat([old_dictionary, new_dict]).sort_values(
                by=["cdf_element", "cdf_internal_name"]
            )
        )
    except Exception as exc:
        err = ui.add_new_error(err, "jurisdiction",juris_sys_name,f"Error adding {element}: {exc}")
    return err


def replacement(match):
    return f"Mc{match.group(1).upper()}"


def correct(proper_name: str) -> str:
    p = r"Mc([a-z])"
    new = re.sub(p, replacement, proper_name)
    if new in correct_d.keys():
        new = correct_d[new]
    return new






