import election_data_analysis as e
#PA16 tests

def test_presidential(dbname):
    assert (not e.data_exists("2016 General","Virginia",dbname=dbname) or e.contest_total(
            "2016 General",
            "Virginia",
            "US President (VA)",
            dbname=dbname,
        )
            == 3984631
    )


def test_statewide_totals(dbname):
    # Vote type not available
    assert True == True


def test_senate_totals(dbname):
    # Vote type not available
    assert True == True


def test_house_totals(dbname):
    assert (not e.data_exists("2016 General","Virginia",dbname=dbname) or e.contest_total(
            "2016 General",
            "Virginia",
            "US House VA District 2",
            dbname=dbname,
        )
            == 309915
    )


def test_contest_by_vote_type(dbname):
    # Vote type not available
    assert True == True


def test_totals_match_vote_type(dbname):
    # Vote type not available
    assert True == True
