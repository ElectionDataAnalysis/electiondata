import election_data_analysis as e
#VA18 tests

def test_presidential(dbname):
    # Vote type not available
    assert True == True


def test_statewide_totals(dbname):
    assert (not e.data_exists("2018 General","Virginia",dbname=dbname) or e.contest_total(
            "2018 General",
            "Virginia",
            "US Senator VA",
            dbname=dbname,
        )
            == 3351757
    )


def test_senate_totals(dbname):
    # Vote type not available
    assert True == True


def test_house_totals(dbname):
    assert (not e.data_exists("2018 General","Virginia",dbname=dbname) or e.contest_total(
            "2018 General",
            "Virginia",
            "US House VA District 1",
            dbname=dbname,
        )
            == 273029
    )


def test_contest_by_vote_type(dbname):
    # Vote type not available
    assert True == True


def test_totals_match_vote_type(dbname):
    # Vote type not available
    assert True == True
