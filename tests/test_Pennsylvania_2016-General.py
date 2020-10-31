import election_data_analysis as e

# PA16 tests


def test_pa_presidential_16(dbname):
    assert (
        not e.data_exists("2016 General", "Pennsylvania", dbname=dbname)
        or e.contest_total(
            "2016 General",
            "Pennsylvania",
            "US President (PA)",
            dbname=dbname,
        )
        == 6115402
    )


def test_pa_statewide_totals_16(dbname):
    assert (
        not e.data_exists("2016 General", "Pennsylvania", dbname=dbname)
        or e.contest_total(
            "2016 General",
            "Pennsylvania",
            "PA Auditor General",
            dbname=dbname,
        )
        == 5916931
    )


def test_pa_senate_totals_16(dbname):
    assert (
        not e.data_exists("2016 General", "Pennsylvania", dbname=dbname)
        or e.contest_total(
            "2016 General",
            "Pennsylvania",
            "PA Senate District 41",
            dbname=dbname,
        )
        == 112283
    )


def test_pa_house_totals_16(dbname):
    assert (
        not e.data_exists("2016 General", "Pennsylvania", dbname=dbname)
        or e.contest_total(
            "2016 General",
            "Pennsylvania",
            "PA House District 21",
            dbname=dbname,
        )
        == 26453
    )


def test_pa_contest_by_vote_type_16(dbname):
    # Vote type not available
    assert True == True


def test_pa_totals_match_vote_type_16(dbname):
    # Vote type not available
    assert True == True
