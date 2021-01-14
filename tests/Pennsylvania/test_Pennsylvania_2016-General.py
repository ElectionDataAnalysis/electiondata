import election_data_analysis as e

election = "2016 General"
jurisdiction = "Pennsylvania"
# PA16 tests
def test_data_exists(dbname):
    assert e.data_exists("2016 General", "Pennsylvania", dbname=dbname)


def test_pa_presidential_16(dbname):
    assert (
        e.contest_total(
            "2016 General",
            "Pennsylvania",
            "US President (PA)",
            dbname=dbname,
        )
        == 6115402
    )


def test_pa_statewide_totals_16(dbname):
    assert (
        e.contest_total(
            "2016 General",
            "Pennsylvania",
            "PA Auditor General",
            dbname=dbname,
        )
        == 5916931
    )


def test_pa_senate_totals_16(dbname):
    assert (
        e.contest_total(
            "2016 General",
            "Pennsylvania",
            "PA Senate District 41",
            dbname=dbname,
        )
        == 112283
    )


def test_pa_house_totals_16(dbname):
    assert (
        e.contest_total(
            "2016 General",
            "Pennsylvania",
            "PA House District 21",
            dbname=dbname,
        )
        == 26453
    )


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )
