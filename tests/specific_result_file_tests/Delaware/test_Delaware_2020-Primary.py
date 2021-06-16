import election_data_analysis as e

election = "2020 Primary"
jurisdiction = "Delaware"


def test_data_exists(dbname):
    assert e.data_exists("2020 Primary", "Delaware", dbname=dbname)


def test_presidential(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Delaware",
            "US President (DE) (Democratic Party)",
            dbname=dbname,
        )
        == 91682
    )


def test_statewide_totals(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Delaware",
            "DE Governor (Republican Party)",
            dbname=dbname,
        )
        == 55447
    )


def test_state_senate_totals(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Delaware",
            "DE Senate District 14 (Republican Party)",
            dbname=dbname,
        )
        == 2649
    )


def test_state_rep_totals(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Delaware",
            "DE House District 26 (Democratic Party)",
            dbname=dbname,
        )
        == 2990
    )


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )
