import os
from election_data_analysis import Analyzer, DataLoader
from election_data_analysis import database as db
from pathlib import Path
import datetime


def test_nist_v2():
    """Tests whether length of nist v2 export string matches the standard.
    (Would be better to test that xml is equivalent, but that's harder.)"""
    data_path = Path(__file__).parents[1].absolute()
    db_dump = os.path.join(data_path,"postgres_test_db_dump.tar")
    nist_v2_reference_file = os.path.join(data_path,"wy20g.xml")

    ts = datetime.datetime.now().strftime("%m%d_%H%M")
    test_db_name = f"pytest_{ts}"

    # load test data to the test db
    dl = DataLoader()
    dl.load_data_from_db_dump(test_db_name, db_dump)

    # create Analyzer object
    an = Analyzer(dbname=test_db_name)

    # test nist v2 export against sample file
    new_str = an.export_nist("2020 General", "Wyoming")
    correct_str = open(nist_v2_reference_file, "rb").read()

    # remove db
    db_params = {
        "dbname": test_db_name,
        "host": dl.session.bind.url.host,
        "user": dl.session.bind.url.username,
        "port": dl.session.bind.url.port
    }
    err = db.remove_database(db_params)
    assert len(correct_str) == len(new_str)

