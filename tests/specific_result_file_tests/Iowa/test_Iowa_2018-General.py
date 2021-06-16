import election_data_analysis as e

election = "2018 General"
jurisdiction = "Iowa"
# IA18 tests


def test_data_exists(dbname):
    assert e.data_exists("2018 General", "Iowa", dbname=dbname)


def test_statewide_totals(dbname):
    assert (
        e.contest_total(
            "2018 General",
            "Iowa",
            "IA Governor",
            dbname=dbname,
        )
        == 1327638
    )


def test_state_senate_totals(dbname):
    assert (
        e.contest_total(
            "2018 General",
            "Iowa",
            "IA Senate District 13",
            dbname=dbname,
        )
        == 30787
    )


def test_state_rep_totals(dbname):
    assert (
        e.contest_total(
            "2018 General",
            "Iowa",
            "IA House District 14",
            dbname=dbname,
        )
        == 8551
    )


# no tests by vote type


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )
