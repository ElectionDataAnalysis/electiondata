import pytest
import os
from pathlib import Path


def test_dataloader_exists(dataloader):
    assert dataloader is not None, (
        "Specify viable dataloader parameter file path with --param_file option to pytest"
        "or correct default param_file (run_time.ini in same folder as test_dataloading.py)"
    )


def test_multielection_loading(dataloader):
    tests_dir = Path(__file__).parents[1]
    reference_results = os.path.join(
        tests_dir, "dataloading_tests", "multielection_loading", "reference_results.tsv"
    )
    dictionary_path = os.path.join(
        tests_dir, "dataloading_tests", "multielection_loading", "dictionary.txt"
    )
    multi_ini = os.path.join(
        tests_dir,
        "dataloading_tests",
        "multielection_loading",
        "county_2018_AL_AZ_only.ini",
    )

    # ensure results_dir points to testing data
    old_results_dir = dataloader.d["results_dir"]
    dataloader.d["results_dir"] = os.path.join(tests_dir, "dataloading_tests")

    # load results
    success, err = dataloader.load_multielection_from_ini(
        multi_ini,
        dictionary_path=dictionary_path,
        overwrite_existing=True,
        load_jurisdictions=True,
        report_err_to_file=False,
    )
    # restore dataloader to previous
    dataloader.d["results_dir"] = old_results_dir

    # test successful load
    assert success == {("2018 General", "Alabama"): [], ("2018 General", "Arizona"): []}
    assert err is None

    # test results
    for jurisdiction in ["Alabama", "Arizona"]:
        test_err = dataloader.analyzer.test_loaded_results(
            "2018 General",
            jurisdiction,
            jurisdiction.replace(" ", "-"),
            reference_results=reference_results,
        )
        assert test_err["test"] == dict()
