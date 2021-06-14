import election_data_analysis as e

election = "2018 General"
jurisdiction = "Pennsylvania"


def test_data_exists(dbname):
    assert e.data_exists("2018 General", "Pennsylvania", dbname=dbname)


def test_pa_statewide_totals_18(dbname):
    assert (
        e.contest_total(
            "2018 General",
            "Pennsylvania",
            "PA Governor",
            dbname=dbname,
        )
        == 5012555
    )


def test_pa_senate_totals_18(dbname):
    assert (
        e.contest_total(
            "2018 General",
            "Pennsylvania",
            "PA Senate District 20",
            dbname=dbname,
        )
        == 81817
    )


def test_pa_house_totals_18(dbname):
    assert (
        e.contest_total(
            "2018 General",
            "Pennsylvania",
            "PA House District 103",
            dbname=dbname,
        )
        == 18363
    )


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )
