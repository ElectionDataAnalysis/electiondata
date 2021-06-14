import election_data_analysis as e

election = "2020 Primary"
jurisdiction = "California"


def test_data_exists(dbname):
    assert e.data_exists("2020 Primary", "California", dbname=dbname)


def test_ca_presidential_20(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "California",
            "US President (CA) (Democratic Party)",
            dbname=dbname,
        )
        == 5784364
    )


def test_ca_senate_totals_20(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "California",
            "CA Senate District 13 (Libertarian Party)",
            dbname=dbname,
        )
        == 5910
    )


def test_ca_rep_20(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "California",
            "CA House District 60 (Republican Party)",
            dbname=dbname,
        )
        == 38968
    )


def test_ca_congress_20(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "California",
            "US House CA District 6 (Democratic Party)",
            dbname=dbname,
        )
        == 132661
    )


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )
