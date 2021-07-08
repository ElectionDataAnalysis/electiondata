import os
from electiondata import Analyzer, DataLoader
from electiondata import database as db
from pathlib import Path
import datetime


def test_nist_v2_and_v1(runtime):
    """Tests whether length of nist v2 export string matches the standard.
    (Would be better to test that xml is equivalent, but that's harder.)"""
    # load test data to the test db
    dl = DataLoader(param_file=runtime)  # test db will be created later
    tests_path = os.path.join(Path(dl.d["repository_content_root"]).parent, "tests")
    db_dump = os.path.join(
        tests_path, "000_data_for_pytest", "postgres_test_db_dump.tar"
    )
    nist_v2_reference_file = os.path.join(
        tests_path, "000_data_for_pytest", "nist_v2_wy20g.xml"
    )
    nist_v1_reference_file = os.path.join(
        tests_path, "000_data_for_pytest", "nist_v1_wy20g.json"
    )

    ts = datetime.datetime.now().strftime("%m%d_%H%M")
    test_db_name = f"pytest_{ts}"
    err_str = dl.load_data_from_db_dump(dbname=test_db_name, dump_file=db_dump)

    # create Analyzer object
    an = Analyzer(dbname=test_db_name, param_file=runtime)

    # test nist v2 export against sample file
    new_str = an.export_nist_v2("2020 General", "Wyoming")
    correct_str = open(nist_v2_reference_file, "rb").read()

    # test nist v1 export against sample file
    new_str_v1 = f"{an.export_nist_v1_json('2020 General', 'Wyoming')}"
    correct_str_v1 = open(nist_v1_reference_file, "r").read()

    # remove db
    db_params = {
        "dbname": test_db_name,
        "host": dl.session.bind.url.host,
        "user": dl.session.bind.url.username,
        "port": dl.session.bind.url.port,
    }
    err = db.remove_database(db_params)

    assert len(correct_str) == len(new_str) and len(correct_str_v1) == len(new_str_v1)
