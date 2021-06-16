import election_data_analysis as e

election = "2018 General"
jurisdiction = "California"


def test_data_exists(dbname):
    assert e.data_exists("2018 General", "California", dbname=dbname)


def test_ca_statewide_totals_18(dbname):
    assert (
        e.contest_total(
            "2018 General",
            "California",
            "US Senate CA",
            dbname=dbname,
        )
        == 11113364
    )


def test_ca_senate_totals_18(dbname):
    assert (
        e.contest_total(
            "2018 General",
            "California",
            "CA Senate District 12",
            dbname=dbname,
        )
        == 203077
    )


def test_ca_rep_18(dbname):
    assert (
        e.contest_total(
            "2018 General",
            "California",
            "CA House District 60",
            dbname=dbname,
        )
        == 125660
    )


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )
