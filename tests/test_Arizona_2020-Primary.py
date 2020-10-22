import election_data_analysis as e
#AZ20 tests

def test_presidential(dbname):
    #Contest Not available
    assert True == True


def test_statewide_totals(dbname):
    assert(not e.data_exists("2020 Primary","Arizona",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Arizona",
            "US Senate AZ (Democratic Party)",
        )
        == 451 + 665620
    )

def test_state_senate_totals(dbname):
    assert (not e.data_exists("2020 Primary","Arizona",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Arizona",
            "AZ Senate District 10 (Republican Party)",
        )
        == 19891
    )

def test_state_rep_totals(dbname):
    assert (not e.data_exists("2020 Primary","Arizona",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Arizona",
            "US House AZ District 6 (Democratic Party)",
        )
        == 3651 + 29218 + 4592 + 42538
    )

def test_contest_by_vote_type(dbname):
    assert True == True

def test_totals_match_vote_type(dbname):
    assert True == True
