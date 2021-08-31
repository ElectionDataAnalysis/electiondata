import pytest
import os
from pathlib import Path


def test_dataloader_exists(dataloader):
    assert dataloader is not None, (
        "Specify viable dataloader parameter file path with --param_file option to pytest"
        "or correct default param_file (run_time.ini in same folder as test_dataloading.py)"
    )


def test_loading(dataloader, test_data_url, param_file):
    dataloader.get_testing_data_from_git_repo(test_data_url)
    successfully_loaded, failed_to_load, all_tests_passed, err = dataloader.load_all(
        move_files=False,
        rollup=True,
    )
    print(
        f"successfully loaded:\n{successfully_loaded}\n\n"
        f"failed to load:\n{failed_to_load}\n\n"
        f"passed all tests?:\n{all_tests_passed}"
    )
    # for all election-jurisdiction pairs attempted, can't have any files failing to load, and must have
    #  all tests passed.
    # set of ej-pairs attempted cannot be null
    attempted_pairs = successfully_loaded.keys()
    assert (
        attempted_pairs
    ), "No loadable results files found; check run_time.ini or specified parameter file"

    # all files loaded successfully
    assert all(
        [v == list() for v in failed_to_load.values()]
    ), f"Not all files loaded successfully."

    # all tests passed
    assert all([v for v in all_tests_passed.values()]), (
        "Some tests failed.  For more information "
        f"see reports in {dataloader.d['reports_and_plots_dir']}"
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
