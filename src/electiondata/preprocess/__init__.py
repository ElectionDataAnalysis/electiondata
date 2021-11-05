import pandas as pd
from typing import Optional
from electiondata import userinterface as ui

nj_raw_county_list = [
    "ATLANTIC", 
    "BERGEN", 
    "BURLINGTON", 
    "CAMDEN", 
    "CAPE MAY", 
    "CUMBERLAND", 
    "ESSEX", 
    "GLOUCESTER", 
    "HUDSON", 
    "HUNTERDON", 
    "MERCER", 
    "MIDDLESEX", 
    "MONMOUTH", 
    "MORRIS", 
    "OCEAN", 
    "PASSAIC", 
    "SALEM", 
    "SOMERSET", 
    "SUSSEX", 
    "UNION", 
    "WARREN",
    ]

def preprocess(
        input_file_path: str,
        mode: str,
        output_file_path: str,
        sep: str = "\t",
) -> Optional[dict]:
    """
    Inputs:
        input_file_path: str, path to file to be processed; file needs to be tabular
        mode: str, identifies expected structure of file to be processed ("NJ")
        output_file_path: str, path to location to write processed file
        sep: str, field separation character (defaults to \t)

    Creates new, munge-ready file at <output_file_path>

    Returns:
        Optional[dict], error dictionary
    """
    err = None
    if mode not in ["NJ"]:
        err = ui.add_new_error(err, "munge", mode, f"Not recognized.")
        return err
    # TODO read file into dataframe
    df = pd.read_csv(input_file_path, sep=sep)

    # TODO fill in missing values

    # TODO convert from multi-block to single block

    # TODO parse single-county contests

    # TODO parse multi-county contests


    return err


if __name__ == "__main__":

    in_path = "/Users/Steph-Airbook/Documents/Temp/NJ/tabula-2021-official-primary-results-state-senate.tsv"
    out_path = f"{in_path}.processed"
    err = preprocess(in_path,"NJ",out_path)

    exit()