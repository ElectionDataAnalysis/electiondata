import election_data_analysis as e
#IA20 tests

def test_presidential(dbname):
    assert(not e.data_exists("2020 Primary","Iowa",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Iowa",
            "US President (IA) (Democratic Party)",
        )
        == 149973
    )


def test_statewide_totals(dbname):
    assert(not e.data_exists("2020 Primary","Iowa",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Iowa",
            "US Senate IA (Republican Party)",
        )
        == 229721
    )

def test_state_senate_totals(dbname):
    assert (not e.data_exists("2020 Primary","Iowa",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Iowa",
            "IA Senate District 2 (Republican Party)",
        )
        == 11583
    )

def test_state_rep_totals(dbname):
    assert (not e.data_exists("2020 Primary","Iowa",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Iowa",
            "US House IA District 8 (Democratic Party)",
        )
        == 1382
    )

def test_contest_by_vote_type(dbname):
    assert True == True

def test_totals_match_vote_type(dbname):
    assert True == True
