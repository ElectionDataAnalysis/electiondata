import pytest
import pandas as pd
import os
from pathlib import Path
from electiondata import juris as ju
import datetime



def test_dataloader_exists(dataloader):
    assert dataloader is not None, \
        "Specify viable dataloader parameter file path with --param_file option to pytest" \
        "or correct default param_file (run_time.ini in same folder as test_dataloading.py)"

def test_loading(dataloader, test_data_url, param_file):
    dataloader.get_testing_data(url=test_data_url)
    ts = datetime.datetime.now().strftime("%m%d_%H%M")
    dbname = f"test_{ts}"
    dataloader.change_db(dbname=dbname, db_param_file=param_file)
    successfully_loaded, failed_to_load, all_tests_passed, err = dataloader.load_all(
        move_files=False,
        rollup=True,
    )
    # for all election-jurisdiction pairs attempted, can't have any files failing to load, and must have
    #  all tests passed.
    # set of ej-pairs attempted cannot be null
    attempted_pairs = successfully_loaded.keys()
    assert attempted_pairs

    # all files loaded successfully
    assert set(failed_to_load.values()) == set(list())

    # all tests passed
    assert set(all_tests_passed.values()) == set(True)




