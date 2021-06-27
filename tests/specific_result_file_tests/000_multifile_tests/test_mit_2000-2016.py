import election_data_analysis as eda
from pathlib import Path
import os
import pandas as pd
from election_data_analysis import database as db

# # # constants - CHANGE THESE!! - use internal db names

def test_presidential(dbname, param_file):
    compare_file = os.path.join(Path(__file__).parent, "mit_by_state_yr2000-2016.txt")
    bad_list, _ = eda.bad_multi_presidentials(dbname, param_file, compare_file)
    assert bad_list == list()

