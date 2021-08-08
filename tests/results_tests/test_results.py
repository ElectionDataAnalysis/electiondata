import pytest
import pandas as pd
import os
from pathlib import Path
from electiondata import juris as ju


def test_analyzer_exists(analyzer):
    assert analyzer is not None, "Specify analyzer parameter file path with --param_file option to pytest"


def test_data_exists(analyzer, election, jurisdiction):
    assert election is not None, "Specify election with --election option to pytest"
    assert jurisdiction is not None, "Specify jurisdiction with --jurisdiction option to pytest"
    assert analyzer.data_exists(election, jurisdiction)


def test_standard_vote_types(analyzer, election, jurisdiction):
    assert analyzer.check_count_types_standard(election,jurisdiction)


def test_vote_type_counts_consistent(analyzer, election, jurisdiction, major_subdiv_type):
    assert analyzer.check_totals_match_vote_types(election,jurisdiction, sub_unit_type=major_subdiv_type)


def test_all_candidates_known(analyzer, election, jurisdiction,  major_subdiv_type):
    contest_with_unknowns = analyzer.get_contest_with_unknown_candidates(
        election, jurisdiction)
    bad = "\n".join(contest_with_unknowns)
    if bad:
        print(f"\nContests with unknown candidates:\n{bad}\n")
    assert contest_with_unknowns == []


def test_contests(request,analyzer,election,jurisdiction):
    significance = request.config.getoption("--significance")
    ref = os.path.join(
        Path(__file__).absolute().parents[0],
        "../../src/reference_results",
        f"{ju.system_name_from_true_name(jurisdiction)}.tsv"
    )
    assert os.path.isfile(ref)
    not_found,ok,wrong,significantly_wrong,sub_dir, err = analyzer.compare_to_results_file(
        reference=ref,
        single_election=election,
        single_jurisdiction=jurisdiction,
        report_dir=analyzer.reports_and_plots_dir,
        significance=significance,
    )
    assert err is None, f"Errors during comparison: {err}"
    assert wrong.empty, f"See {sub_dir}, for failed comparisons"


