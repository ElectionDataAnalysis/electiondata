import election_data_analysis as e

def test_wa_statewide_totals_18(dbname):
    assert (not e.data_exists("2018 General","Washington",dbname=dbname) or e.contest_total(
            "2018 General",
            "Washington",
            "Supreme Court Justice Position 8",
            dbname=dbname,
        )
            == 2496249
    )


def test_wa_senate_totals_18(dbname):
    assert (not e.data_exists("2018 General","Washington",dbname=dbname) or e.contest_total(
            "2018 General",
            "Washington",
            "Legislative District 13 State Senator",
            dbname=dbname,
        )
            == 38038
    )


def test_wa_house_totals_18(dbname):
    assert (not e.data_exists("2018 General","Washington",dbname=dbname) or e.contest_total(
            "2018 General",
            "Washington",
            "Legislative District 9 State Representative Pos. 1",
            dbname=dbname,
        )
            == 52909
    )