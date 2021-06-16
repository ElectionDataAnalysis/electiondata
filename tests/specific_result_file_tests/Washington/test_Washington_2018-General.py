import election_data_analysis as e

election = "2018 General"
jurisdiction = "Washington"


def test_data_exists(dbname):
    assert e.data_exists("2018 General", "Washington", dbname=dbname)


def test_wa_statewide_totals_18(dbname):
    assert (
        e.contest_total(
            "2018 General",
            "Washington",
            "US Senate WA",
            dbname=dbname,
        )
        == 3086168
    )


def test_wa_senate_totals_18(dbname):
    assert (
        e.contest_total(
            "2018 General",
            "Washington",
            "WA Senate District 13",
            dbname=dbname,
        )
        == 38038
    )


def test_wa_house_totals_18(dbname):
    assert (
        e.contest_total(
            "2018 General",
            "Washington",
            "WA House District 9 Position 1",
            dbname=dbname,
        )
        == 52909
    )


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )
