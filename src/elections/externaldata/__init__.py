import pandas as pd
import requests
from typing import List, Dict
from elections import munge as m

# constants
census_noncount_columns = ["Name", "state", "county"]
fips = {
    "Alabama": "01",
    "Alaska": "02",
    "Arizona": "04",
    "Arkansas": "05",
    "California": "06",
    "Colorado": "08",
    "Connecticut": "09",
    "Delaware": "10",
    "District of Columbia": "11",
    "Florida": "12",
    "Georgia": "13",
    "Hawaii": "15",
    "Idaho": "16",
    "Illinois": "17",
    "Indiana": "18",
    "Iowa": "19",
    "Kansas": "20",
    "Kentucky": "21",
    "Louisiana": "22",
    "Maine": "23",
    "Maryland": "24",
    "Massachusetts": "25",
    "Michigan": "26",
    "Minnesota": "27",
    "Mississippi": "28",
    "Missouri": "29",
    "Montana": "30",
    "Nebraska": "31",
    "Nevada": "32",
    "New Hampshire": "33",
    "New Jersey": "34",
    "New Mexico": "35",
    "New York": "36",
    "North Carolina": "37",
    "North Dakota": "38",
    "Ohio": "39",
    "Oklahoma": "40",
    "Oregon": "41",
    "Pennsylvania": "42",
    "Rhode Island": "44",
    "South Carolina": "45",
    "South Dakota": "46",
    "Tennessee": "47",
    "Texas": "48",
    "Utah": "49",
    "Vermont": "50",
    "Virginia": "51",
    "Washington": "53",
    "West Virginia": "54",
    "Wisconsin": "55",
    "Wyoming": "56",
    "American Samoa": "60",
    "Guam": "66",
    "Northern Mariana Islands": "69",
    "Puerto Rico": "72",
    "U.S. Minor Outlying Islands": "74",
    "U.S. Virgin Islands": "78",
}

acs5_columns = {
    # {display category: {census.gov column name: internal column name}}
    "Population": {"B01001_001E": "Population"},
    "Pop. by Income (Avg Household)": {
        "B19001_002E": "Less than $10,000",
        "B19001_003E": "$10,000 to $14,999",
        "B19001_004E": "$15,000 to $19,999",
        "B19001_005E": "$20,000 to $24,999",
        "B19001_006E": "$25,000 to $29,999",
        "B19001_007E": "$30,000 to $34,999",
        "B19001_008E": "$35,000 to $39,999",
        "B19001_009E": "$40,000 to $44,999",
        "B19001_010E": "$45,000 to $49,999",
        "B19001_011E": "$50,000 to $59,999",
        "B19001_012E": "$60,000 to $74,999",
        "B19001_013E": "$75,000 to $99,999",
        "B19001_014E": "$100,000 to $124,999",
        "B19001_015E": "$125,000 to $149,999",
        "B19001_016E": "$150,000 to $199,999",
        "B19001_017E": "$200,000 or more",
    },
    "Pop. by Race": {
        "B01001A_001E": "White",
        "B01001B_001E": "Black",
        "B01001C_001E": "American Indian and Alaska Native",
        "B01001D_001E": "Asian",
        "B01001E_001E": "Hawaiian and Pacific Islander",
        "B01001F_001E": "Some other race (one race)",
        "B01001G_001E": "Some other race (two or more races)",
        "B01001I_001E": "Hispanic",
    },
    "Pop.by Age": {
        "B01001_007E": "Male 18 and 19 years",
        "B01001_008E": "Male 20 years",
        "B01001_009E": "Male 21 years",
        "B01001_010E": "Male 22 to 24 years",
        "B01001_011E": "Male 25 to 29 years",
        "B01001_012E": "Male 30 to 34 years",
        "B01001_013E": "Male 35 to 39 years",
        "B01001_014E": "Male 40 to 44 years",
        "B01001_015E": "Male 45 to 49 years",
        "B01001_016E": "Male 50 to 54 years",
        "B01001_017E": "Male 55 to 59 years",
        "B01001_018E": "Male 60 and 61 years",
        "B01001_019E": "Male 62 to 64 years",
        "B01001_020E": "Male 65 and 66 years",
        "B01001_021E": "Male 67 to 69 years",
        "B01001_022E": "Male 70 to 74 years",
        "B01001_023E": "Male 75 to 79 years",
        "B01001_024E": "Male 80 to 84 years",
        "B01001_025E": "Male 85 years and over",
        "B01001_031E": "Female 18 and 19 years",
        "B01001_032E": "Female 20 years",
        "B01001_033E": "Female 21 years",
        "B01001_034E": "Female 22 to 24 years",
        "B01001_035E": "Female 25 to 29 years",
        "B01001_036E": "Female 30 to 34 years",
        "B01001_037E": "Female 35 to 39 years",
        "B01001_038E": "Female 40 to 44 years",
        "B01001_039E": "Female 45 to 49 years",
        "B01001_040E": "Female 50 to 54 years",
        "B01001_041E": "Female 55 to 59 years",
        "B01001_042E": "Female 60 and 61 years",
        "B01001_043E": "Female 62 to 64 years",
        "B01001_044E": "Female 65 and 66 years",
        "B01001_045E": "Female 67 to 69 years",
        "B01001_046E": "Female 70 to 74 years",
        "B01001_047E": "Female 75 to 79 years",
        "B01001_048E": "Female 80 to 84 years",
        "B01001_049E": "Female 85 years and over",
    },
}

acs_5_label_summands = {
    "18 to 29": [
        "Male 18 and 19 years",
        "Male 20 years",
        "Male 21 years",
        "Male 22 to 24 years",
        "Male 25 to 29 years",
        "Female 18 and 19 years",
        "Female 20 years",
        "Female 21 years",
        "Female 22 to 24 years",
        "Female 25 to 29 years",
    ],
    "30 to 49": [
        "Male 30 to 34 years",
        "Male 35 to 39 years",
        "Male 40 to 44 years",
        "Male 45 to 49 years",
        "Female 30 to 34 years",
        "Female 35 to 39 years",
        "Female 40 to 44 years",
        "Female 45 to 49 years",
    ],
    "50 to 64": [
        "Male 50 to 54 years",
        "Male 55 to 59 years",
        "Male 60 and 61 years",
        "Male 62 to 64 years",
        "Female 50 to 54 years",
        "Female 55 to 59 years",
        "Female 60 and 61 years",
        "Female 62 to 64 years",
    ],
    "64+": [
        "Male 65 and 66 years",
        "Male 67 to 69 years",
        "Male 70 to 74 years",
        "Male 75 to 79 years",
        "Male 80 to 84 years",
        "Male 85 years and over",
        "Female 65 and 66 years",
        "Female 67 to 69 years",
        "Female 70 to 74 years",
        "Female 75 to 79 years",
        "Female 80 to 84 years",
        "Female 85 years and over",
    ],
    "<$30,000": [
        "Less than $10,000",
        "$10,000 to $14,999",
        "$15,000 to $19,999",
        "$20,000 to $24,999",
        "$25,000 to $29,999",
    ],
    "$30,000 to $49,999": [
        "$30,000 to $34,999",
        "$35,000 to $39,999",
        "$40,000 to $44,999",
        "$45,000 to $49,999",
    ],
    "$50,000 to $99,999": [
        "$50,000 to $59,999",
        "$60,000 to $74,999",
        "$75,000 to $99,999",
    ],
    "$100,000 to $109,999": [
        "$100,000 to $124,999",
        "$125,000 to $149,999",
        "$150,000 to $199,999",
    ],
}


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
        census_df, [c for c in census_df.columns if c not in census_noncount_columns]
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
