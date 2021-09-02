import results
from electiondata import DataLoader
from typing import Dict, Any, List, Optional
import os
import datetime
from pathlib import Path
import pytest


# some lists of dictionaries need to be sorted by a particular key
def dict_sort(
    list_of_dicts: List[Dict[str, Any]],
    sort_key: Optional[str] = None,
    drop_key: bool = True,
) -> List[Dict[str, Any]]:
    """If <key> is given, sort items by value of that key in each dictionary.
    If <key> is not given, return <list_of_dicts>"""

    if list_of_dicts and sort_key:
        new = sorted(list_of_dicts, key=lambda k: k[sort_key])
        if drop_key:
            for d in new:
                d.pop(sort_key)
    else:
        new = list_of_dicts

    return new


@pytest.fixture
def dataloader(param_file):
    # NB: don't create temp db yet -- it will be created in test_load_test_data
    dl = DataLoader(param_file=param_file)

    # name new db
    ts = datetime.datetime.now().strftime("%m%d_%H%M")
    new_dbname = f"pytest_{ts}"

    # Now load test data
    tests_path = os.path.join(Path(dl.d["repository_content_root"]).parent, "tests")
    db_dump = os.path.join(
        tests_path, "000_data_for_pytest", "postgres_test_db_dump.tar"
    )
    err_str = dl.load_data_from_db_dump(
        dbname=new_dbname, dump_file=db_dump, delete_existing=True
    )

    # point dl to new db
    dl.change_db(new_db_name=new_dbname, db_param_file=param_file, db_params=None)
    yield dl
    dl.close_and_erase()


def test_election_data_exists(dataloader):
    ej_pairs = [
        ("2016 General", "Georgia"),
        ("2018 General", "Georgia"),
        ("2020 Primary", "Georgia"),
        ("2020 General", "Georgia"),
        ("2018 General", "North Carolina"),
        # ("2020 Primary", "Alaska"),
    ]
    has_data = [(e, j) for (e, j) in ej_pairs if dataloader.analyzer.data_exists(e, j)]
    assert set(has_data) == set(ej_pairs)


"""def test_census_data_exists(param_file):
    assert external_data_exists("2018 General", "Georgia", p_path=param_file, dbname=test_db)
"""
