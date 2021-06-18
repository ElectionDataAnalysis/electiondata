import pandas as pd
import requests
from typing import List

# constants
census_number = {
	"Alabama": "01",
	"Alaska": "02",
	"American Samoa": "03",
	"Arizona": "04",
	"Arkansas": "05",
	"California": "06",
	"Colorado": "07",
	"Connecticut": "08",
	"Delaware": "09",
	"District of Columbia": "10",
	"Florida": "11",
	"Georgia": "12",
	"Guam": "13",
	"Hawaii": "14",
	"Idaho": "15",
	"Illinois": "16",
	"Indiana": "17",
	"Iowa": "18",
	"Kansas": "19",
	"Kentucky": "20",
	"Louisiana": "21",
	"Maine": "22",
	"Maryland": "23",
	"Massachusetts": "24",
	"Michigan": "25",
	"Minnesota": "26",
	"Mississippi": "27",
	"Missouri": "28",
	"Montana": "29",
	"Nebraska": "30",
	"Nevada": "31",
	"New Hampshire": "32",
	"New Jersey": "33",
	"New Mexico": "34",
	"New York": "35",
	"North Carolina": "36",
	"North Dakota": "37",
	"Northern Mariana Islands": "38",
	"Ohio": "39",
	"Oklahoma": "40",
	"Oregon": "41",
	"Pennsylvania": "42",
	"Puerto Rico": "43",
	"Rhode Island": "44",
	"South Carolina": "45",
	"South Dakota": "46",
	"Tennessee": "47",
	"Texas": "48",
	"Utah": "49",
	"US Virgin Islands": "50",
	"Vermont": "51",
	"Virginia": "52",
	"Washington": "53",
	"West Virginia": "54",
	"Wisconsin": "55",
	"Wyoming": "56",
}


def get_census_data(columns_to_get: List[str], state_num: int,census_year: int) -> pd.DataFrame:
    """Download census data for a single state (with census number <state_num>)
    and a single year <census_year> to a dataframe. """
    # TODO revise routines calling this to remove: columns_to_get = ",".join(columns.keys())

    params = {
        "get": columns_to_get,
        "for": "county:*",
        "in": f"state:{state_num}"
    }
    url = f"https://api.census.gov/data/{census_year}/acs/acs5"

    response = requests.get(url,params=params).json()
    census_df = pd.DataFrame(response)

    # make first row into header
    headers = census_df.iloc[0]
    census_df = census_df[1:]
    census_df.columns = headers
    return census_df


def normalize_census_county_name(txt):
    """ make census county names match ReportingUnit """
    tmp = txt.split(",")
    return f"{tmp[1].strip()};{tmp[0].strip()}"