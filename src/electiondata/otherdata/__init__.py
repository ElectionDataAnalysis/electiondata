import pandas as pd
import requests
from typing import List, Dict
from electiondata import munge as m, constants


def get_raw_acs5_data(
    columns_to_get: List[str], census_year: int, reporting_unit_type: str = "county"
) -> pd.DataFrame:
    """Download census data for all geographies of <reporting_unit_type>
    and a single year <census_year> to a dataframe.
    Columns of dataframe are named as on census.gov"""

    params = {"get": columns_to_get, "for": f"{reporting_unit_type}:*"}
    url = f"https://api.census.gov/data/{census_year}/acs/acs5"

    response = requests.get(url, params=params).json()
    census_df = pd.DataFrame(response)

    # make first row into header
    headers = census_df.iloc[0]
    census_df = census_df[1:]
    census_df.columns = headers

    # make count columns numeric
    census_df, bad_rows = m.clean_count_cols(
        census_df,
        [c for c in census_df.columns if c not in constants.census_noncount_columns],
    )
    if not bad_rows.empty:
        print(f"Not all rows processed. Bad rows are\n{bad_rows}")
    return census_df


def combine_and_rename_columns(
    df: pd.DataFrame, label_summands: Dict[str, List[str]]
) -> pd.DataFrame:
    """Returns new dataframe based on <df>, with columns named for keys of
    <label_summands> and values obtained by summing over input <df> columns"""
    new = pd.DataFrame()
    for label in label_summands.keys():
        raw_list = label_summands[label]
        # if all summands are in df
        if all([c in df.columns for c in raw_list]):
            new[label] = df[raw_list].sum(axis=1)

    # TODO
    return new


def normalize(df: pd.DataFrame, label_columns: List[str]) -> pd.DataFrame:
    working = df.copy()
    working["ReportingUnit"] = df["NAME"].str.apply(normalize_geo_name)
    lc = [c for c in label_columns if c in working.columns]
    working = working.melt(
        working, id_vars="ReportingUnit", value_vars=lc, value_name="Label"
    )
    return working


def normalize_geo_name(txt):
    """make geo name match ReportingUnit convention"""
    tmp = txt.split(",")
    return f"{tmp[1].strip()};{tmp[0].strip()}"
