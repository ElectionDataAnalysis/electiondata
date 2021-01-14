import election_data_analysis as e

election = "2018 General"
jurisdiction = "Texas"
# IA18 tests


def test_data_exists(dbname):
    assert e.data_exists("2018 General", "Texas", dbname=dbname)


def test_senate_totals(dbname):
    assert (
        e.contest_total(
            "2018 General",
            "Texas",
            "US Senate TX",
            dbname=dbname,
        )
        == 8371655
    )


def test_governor_totals(dbname):
    assert (
        e.contest_total(
            "2018 General",
            "Texas",
            "TX Governor",
            dbname=dbname,
        )
        == 8343443
    )


def test_state_senate_totals(dbname):
    assert (
        e.contest_total(
            "2018 General",
            "Texas",
            "TX Senate District 15",
            dbname=dbname,
        )
        == 234763
    )


def test_state_rep_totals(dbname):
    assert (
        e.contest_total(
            "2018 General",
            "Texas",
            "TX House District 25",
            dbname=dbname,
        )
        == 40902
    )


# # no tests by vote type


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )
