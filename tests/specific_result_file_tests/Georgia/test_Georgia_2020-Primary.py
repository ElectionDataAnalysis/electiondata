import election_data_analysis as e

election = "2020 Primary"
jurisdiction = "Georgia"
abbreviation = "GA"


def test_data_exists(dbname):
    assert e.data_exists(election, jurisdiction, dbname=dbname)


def test_presidential(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"US President ({abbreviation}) (Republican Party)",
            dbname=dbname,
        )
        == 947352
    )


def test_ga_statewide_totals_20(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            "US Senate GA (Republican Party)",
            dbname=dbname,
        )
        == 992555
    )


def test_ga_senate_totals_20(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            "GA Senate District 8 (Democratic Party)",
            dbname=dbname,
        )
        == 9103
    )


def test_ga_house_totals_20(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            "GA House District 7 (Democratic Party)",
            dbname=dbname,
        )
        == 2193
    )


def test_contest_by_vote_type(dbname):
    assert (
        e.count_type_total(
            election,
            jurisdiction,
            "GA House District 7 (Democratic Party)",
            "absentee-mail",
            dbname=dbname,
        )
        == 1655
    )


def test_ga_totals_match_vote_type_20(dbname):
    assert (
        e.check_totals_match_vote_types(election, jurisdiction, dbname=dbname) == True
    )


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )
