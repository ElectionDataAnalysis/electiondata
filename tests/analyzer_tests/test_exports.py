import os
from electiondata import Analyzer, DataLoader
from electiondata import database as db
from pathlib import Path
import datetime


def test_nist_v2(analyzer, tests_path):
    """Tests whether length of nist v2 export string matches the standard.
    (Would be better to test that xml is equivalent, but that's harder.)"""
    # TODO restore test of nist v1 export

    nist_v2_reference_file = os.path.join(
        tests_path, "000_data_for_pytest", "nist_v2_wy20g.xml"
    )

    # test nist v2 export against sample file
    new_str_v2 = analyzer.export_nist_v2("2020 General", "Wyoming")
    correct_str_v2 = open(nist_v2_reference_file, "r").read()

    # test nist v1 export against sample file
    # new_str_v1 = f"{analyzer.export_nist_v1_json('2020 General', 'Wyoming')}"
    # correct_str_v1 = open(nist_v1_reference_file, "r").read()

    assert len(correct_str_v2) == len(
        new_str_v2
    )  # and len(correct_str_v1) == len(new_str_v1)


def test_nist_v1(analyzer, tests_path):
    """Tests whether length of nist v2 export string matches the standard.
    (Would be better to test that xml is equivalent, but that's harder.)"""

    nist_v1_reference_file = os.path.join(
        tests_path, "000_data_for_pytest", "nist_v1_wy20g.json"
    )

    # test nist v1 export against sample file
    new_str_v1 = f"{analyzer.export_nist_v1_json('2020 General', 'Wyoming')}"
    correct_str_v1 = open(nist_v1_reference_file, "r").read()

    assert  len(correct_str_v1) == len(new_str_v1)
