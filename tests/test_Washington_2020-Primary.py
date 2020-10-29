import election_data_analysis as e

def data_exists(dbname):
    assert e.data_exists("2020 Primary", "Washington", dbname=dbname)

def test_wa_presidential_20(dbname):
    assert (e.contest_total(
            "2020 Primary",
            "Washington",
            "US President (WA)",
            dbname=dbname,
        )
            == 2229730
    )


def test_wa_statewide_totals_20(dbname):
    assert (e.contest_total(
            "2020 Primary",
            "Washington",
            "WA Attorney General",
            dbname=dbname,
        )
            == 2430736
    )


def test_wa_senate_totals_20(dbname):
    assert (e.contest_total(
            "2020 Primary",
            "Washington",
            "WA Senate District 11",
            dbname=dbname,
        )
            == 31652
    )


def test_wa_house_totals_20(dbname):
    assert (e.contest_total(
            "2020 Primary",
            "Washington",
            "WA House District 7",
            dbname=dbname,
        )
            == 53860
    )