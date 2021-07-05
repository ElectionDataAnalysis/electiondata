import elections as eda
import os
from pathlib import Path


def test_new_files():
    tests_dir = Path(__file__).parents[1]
    prep_file = os.path.join(
        tests_dir, "jurisdiction_prepper_tests", "jurisdiction_prep.ini_for_test"
    )
    run_time_file = os.path.join(tests_dir, "run_time.ini")
    temp_dir = os.path.join(tests_dir, "000_data_for_pytest", "Temp")
    ref_dir = os.path.join(
        tests_dir, "000_data_for_pytest", "JurisdictionPrepper_reference_files"
    )
    templates = os.path.join(tests_dir, "000_data_for_pytest", "jurisdiction_templates")

    file_list = [f for f in os.listdir(ref_dir) if f[-4:] == ".txt"]

    # remove all temp files
    for f in file_list:
        f_path = os.path.join(temp_dir, f)
        if os.path.isfile(f_path):
            os.remove(f_path)

    jp = eda.JurisdictionPrepper(
        prep_param_file=prep_file,
        run_time_param_file=run_time_file,
        target_dir=temp_dir,
    )
    jp.new_juris_files(target_dir=temp_dir, templates=templates)

    bad = [
        f
        for f in file_list
        if open(os.path.join(temp_dir, f), "r").read()
        != open(os.path.join(ref_dir, f), "r").read()
    ]

    # remove all temp files
    for f in file_list:
        os.remove(os.path.join(temp_dir, f))

    assert not bad
