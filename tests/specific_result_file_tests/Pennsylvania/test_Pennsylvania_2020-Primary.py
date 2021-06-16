import election_data_analysis as e

election = "2020 Primary"
jurisdiction = "Pennsylvania"


def test_data_exists(dbname):
    assert e.data_exists("2020 Primary", "Pennsylvania", dbname=dbname)


def test_pa_presidential_20(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Pennsylvania",
            "US President (PA) (Democratic Party)",
            dbname=dbname,
        )
        == 1595508
    )


def test_pa_statewide_totals_20(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Pennsylvania",
            "PA Auditor General (Republican Party)",
            dbname=dbname,
        )
        == 1042092
    )


def test_pa_senate_totals_20(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Pennsylvania",
            "PA Senate District 21 (Democratic Party)",
            dbname=dbname,
        )
        == 18435
    )


def test_pa_house_totals_20(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Pennsylvania",
            "PA House District 100 (Republican Party)",
            dbname=dbname,
        )
        == 6327
    )


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )
