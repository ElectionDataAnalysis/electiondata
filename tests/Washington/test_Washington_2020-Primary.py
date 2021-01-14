import election_data_analysis as e

election = "2020 Primary"
jurisdiction = "Washington"


def test_data_exists(dbname):
    assert e.data_exists("2020 Primary", "Washington", dbname=dbname)


def test_wa_statewide_totals_20(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Washington",
            "WA Attorney General",
            dbname=dbname,
        )
        == 2430736
    )


def test_wa_senate_totals_20(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Washington",
            "WA Senate District 11",
            dbname=dbname,
        )
        == 31652
    )


def test_wa_house_totals_20(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Washington",
            "WA House District 7 Position 1",
            dbname=dbname,
        )
        == 55618
    )


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )
