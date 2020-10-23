import election_data_analysis as e
#WA16 tests

def test_wa_presidential_16(dbname):
    assert (not e.data_exists("2016 General","Washington",dbname=dbname) or e.contest_total(
            "2016 General",
            "Washington",
            "US President (WA)",
            dbname=dbname,
        )
            == 3209214
    )


def test_wa_statewide_totals_16(dbname):
    assert (not e.data_exists("2016 General","Washington",dbname=dbname) or e.contest_total(
            "2016 General",
            "Washington",
            "Washington State Attorney General",
            dbname=dbname,
        )
            == 2979909
    )


def test_wa_senate_totals_16(dbname):
    assert (not e.data_exists("2016 General","Washington",dbname=dbname) or e.contest_total(
            "2016 General",
            "Washington",
            "Legislative District 22 State Senator",
            dbname=dbname,
        )
            == 68868
    )


def test_wa_house_totals_16(dbname):
    assert (not e.data_exists("2016 General","Washington",dbname=dbname) or e.contest_total(
            "2016 General",
            "Washington",
            "Legislative District 37 State Representative Pos. 1",
            dbname=dbname,
        )
            == 62801
    )


def test_wa_contest_by_vote_type_16(dbname):
    # Vote type not available
    assert True == True


def test_wa_totals_match_vote_type_16(dbname):
    # Vote type not available
    assert True == True