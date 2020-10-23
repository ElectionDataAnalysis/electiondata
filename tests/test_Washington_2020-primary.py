import election_data_analysis as e

def test_wa_presidential_20(dbname):
    assert (not e.data_exists("2020 Primary","Washington",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Washington",
            "United States President Democratic Party",
            dbname=dbname,
        )
            == 2229730
    )


def test_wa_statewide_totals_20(dbname):
    assert (not e.data_exists("2020 Primary","Washington",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Washington",
            "Washington State Attorney General",
            dbname=dbname,
        )
            == 2430736
    )


def test_wa_senate_totals_20(dbname):
    assert (not e.data_exists("2020 Primary","Washington",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Washington",
            "Legislative District 11 State Senator",
            dbname=dbname,
        )
            == 31652
    )


def test_wa_house_totals_20(dbname):
    assert (not e.data_exists("2020 Primary","Washington",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Washington",
            "Legislative District 7 State Representative Pos. 2",
            dbname=dbname,
        )
            == 53860
    )